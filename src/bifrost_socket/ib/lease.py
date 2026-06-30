"""K8s Lease leader election — W4 of trade-k8s-native.

The IB socket layer is a *singleton accessor* to an external stateful resource
(the Win11 TWS / IB Gateway). IB caps each TWS instance at 32 API clients and
rejects a duplicate ``clientId`` with **Error 326**; the account-level market
data line budget is shared by every connected client. Therefore the K8s HA model
for IB edge services is **Active-Standby via ``coordination.k8s.io/Lease``** — NOT
Deployment ``replicas`` with simultaneous ``eConnect``.

This module is a self-contained, framework-agnostic leader-election library:

  - ``LeaseRecord``      — the subset of a coordination.k8s.io/v1 Lease we use.
  - ``LeaseBackend``     — storage protocol (get / create / update with CAS).
  - ``InMemoryLeaseBackend`` — deterministic backend for unit tests.
  - ``KubernetesLeaseBackend`` — real backend (lazy ``kubernetes`` client import).
  - ``LeaderElector``    — the acquire / renew / fail-over algorithm (client-go
                           style), with a pure ``try_acquire_or_renew(now)`` core
                           so the state machine is fully unit-testable.
  - ``run_with_leadership`` / ``run_with_leadership_sync`` — entrypoint helpers
                           that only run the IB service while this pod holds the
                           Lease, and exit (so K8s restarts the pod as standby)
                           when leadership is lost.

Authority: console/src/lib/architecture/tradeK8sNativeCatalog.ts (wave W4).
"""

from __future__ import annotations

import asyncio
import enum
import logging
import signal
import threading
import time
from dataclasses import dataclass, field, replace
from typing import Any, Awaitable, Callable, Dict, Optional, Protocol

logger = logging.getLogger(__name__)

# ── Defaults (client-go leaderelection style; tuned for IB fail-over <20s) ──────

DEFAULT_LEASE_DURATION_SEC = 15.0
DEFAULT_RENEW_DEADLINE_SEC = 10.0
DEFAULT_RETRY_PERIOD_SEC = 2.0


# ── Lease record ────────────────────────────────────────────────────────────────


@dataclass
class LeaseRecord:
    """Subset of a ``coordination.k8s.io/v1`` Lease ``spec`` we rely on.

    Times are epoch seconds (wall clock) for cross-pod comparison; the Kubernetes
    backend converts to/from RFC3339 ``datetime``. ``resource_version`` is an opaque
    optimistic-concurrency token (K8s ``metadata.resourceVersion``; an int counter
    for the in-memory backend).
    """

    holder_identity: str
    lease_duration_sec: float = DEFAULT_LEASE_DURATION_SEC
    acquire_time: float = 0.0
    renew_time: float = 0.0
    lease_transitions: int = 0
    resource_version: Optional[str] = None

    def is_expired(self, now: float) -> bool:
        """True when the lease can be taken over (renew older than its duration)."""
        return (now - self.renew_time) > self.lease_duration_sec


class LeaseConflict(Exception):
    """Optimistic-concurrency conflict (another writer won the race)."""


# ── Backend protocol + in-memory implementation ──────────────────────────────────


class LeaseBackend(Protocol):
    """Storage for a single named Lease object."""

    def get(self) -> Optional[LeaseRecord]:
        """Return the current record, or ``None`` if the Lease does not exist."""
        ...

    def create(self, record: LeaseRecord) -> LeaseRecord:
        """Create the Lease. Raise :class:`LeaseConflict` if it already exists."""
        ...

    def update(self, record: LeaseRecord) -> LeaseRecord:
        """Compare-and-swap update keyed on ``record.resource_version``.

        Raise :class:`LeaseConflict` if the stored version differs (someone else
        wrote in between).
        """
        ...


class InMemoryLeaseBackend:
    """Deterministic in-memory backend for unit tests and single-process dev.

    Models Kubernetes optimistic concurrency with a monotonically increasing
    integer ``resource_version``.
    """

    def __init__(self) -> None:
        self._record: Optional[LeaseRecord] = None
        self._version = 0

    def get(self) -> Optional[LeaseRecord]:
        return replace(self._record) if self._record is not None else None

    def create(self, record: LeaseRecord) -> LeaseRecord:
        if self._record is not None:
            raise LeaseConflict("lease already exists")
        self._version += 1
        stored = replace(record, resource_version=str(self._version))
        self._record = stored
        return replace(stored)

    def update(self, record: LeaseRecord) -> LeaseRecord:
        if self._record is None:
            raise LeaseConflict("lease does not exist")
        if record.resource_version != self._record.resource_version:
            raise LeaseConflict(
                f"resource_version mismatch: have {self._record.resource_version!r}, "
                f"got {record.resource_version!r}"
            )
        self._version += 1
        stored = replace(record, resource_version=str(self._version))
        self._record = stored
        return replace(stored)


class KubernetesLeaseBackend:
    """Real backend backed by ``coordination.k8s.io/v1`` Leases.

    The ``kubernetes`` client is imported lazily so unit tests and the mock dev
    path never require it. Resolves in-cluster config first, then local kubeconfig.
    """

    def __init__(self, namespace: str, name: str) -> None:
        self._namespace = namespace
        self._name = name
        self._api = self._build_api()

    @staticmethod
    def _build_api() -> Any:
        from kubernetes import client, config as k8s_config  # lazy

        try:
            k8s_config.load_incluster_config()
        except Exception:  # noqa: BLE001 — fall back to local kubeconfig (dev)
            k8s_config.load_kube_config()
        return client.CoordinationV1Api()

    @staticmethod
    def _to_epoch(dt: Any) -> float:
        if dt is None:
            return 0.0
        try:
            return float(dt.timestamp())
        except Exception:  # noqa: BLE001
            return 0.0

    @staticmethod
    def _to_dt(epoch: float) -> Any:
        from datetime import datetime, timezone

        return datetime.fromtimestamp(float(epoch), tz=timezone.utc)

    def _from_k8s(self, lease: Any) -> LeaseRecord:
        spec = lease.spec
        return LeaseRecord(
            holder_identity=str(spec.holder_identity or ""),
            lease_duration_sec=float(spec.lease_duration_seconds or DEFAULT_LEASE_DURATION_SEC),
            acquire_time=self._to_epoch(spec.acquire_time),
            renew_time=self._to_epoch(spec.renew_time),
            lease_transitions=int(spec.lease_transitions or 0),
            resource_version=str(lease.metadata.resource_version),
        )

    def _spec(self, record: LeaseRecord) -> Any:
        from kubernetes import client

        return client.V1LeaseSpec(
            holder_identity=record.holder_identity,
            lease_duration_seconds=int(round(record.lease_duration_sec)),
            acquire_time=self._to_dt(record.acquire_time),
            renew_time=self._to_dt(record.renew_time),
            lease_transitions=int(record.lease_transitions),
        )

    def get(self) -> Optional[LeaseRecord]:
        from kubernetes.client.rest import ApiException

        try:
            lease = self._api.read_namespaced_lease(self._name, self._namespace)
        except ApiException as e:
            if e.status == 404:
                return None
            raise
        return self._from_k8s(lease)

    def create(self, record: LeaseRecord) -> LeaseRecord:
        from kubernetes import client
        from kubernetes.client.rest import ApiException

        body = client.V1Lease(
            metadata=client.V1ObjectMeta(name=self._name, namespace=self._namespace),
            spec=self._spec(record),
        )
        try:
            created = self._api.create_namespaced_lease(self._namespace, body)
        except ApiException as e:
            if e.status == 409:
                raise LeaseConflict("lease already exists") from e
            raise
        return self._from_k8s(created)

    def update(self, record: LeaseRecord) -> LeaseRecord:
        from kubernetes import client
        from kubernetes.client.rest import ApiException

        body = client.V1Lease(
            metadata=client.V1ObjectMeta(
                name=self._name,
                namespace=self._namespace,
                resource_version=record.resource_version,
            ),
            spec=self._spec(record),
        )
        try:
            updated = self._api.replace_namespaced_lease(self._name, self._namespace, body)
        except ApiException as e:
            if e.status == 409:
                raise LeaseConflict("resource_version conflict") from e
            raise
        return self._from_k8s(updated)


# ── Leader election ──────────────────────────────────────────────────────────────


class LeaseAttempt(enum.Enum):
    """Outcome of a single ``try_acquire_or_renew`` tick."""

    ACQUIRED = "acquired"   # took the lease (was not leader before)
    RENEWED = "renewed"     # extended a lease we already hold
    HELD_BY_OTHER = "held_by_other"  # a different valid holder owns it — definitive loss
    FAILED = "failed"       # transient (conflict / backend error) — let deadline decide


@dataclass
class LeaderElector:
    """Acquire / renew a Lease and report leadership transitions.

    The state machine core, :meth:`try_acquire_or_renew`, is pure given an
    injected ``now`` and therefore fully unit-testable. :meth:`run` is a thin
    async driver around it.
    """

    backend: LeaseBackend
    identity: str
    lease_duration_sec: float = DEFAULT_LEASE_DURATION_SEC
    renew_deadline_sec: float = DEFAULT_RENEW_DEADLINE_SEC
    retry_period_sec: float = DEFAULT_RETRY_PERIOD_SEC
    on_started_leading: Optional[Callable[[], None]] = None
    on_stopped_leading: Optional[Callable[[], None]] = None
    clock: Callable[[], float] = time.time

    is_leader: bool = field(default=False, init=False)
    _last_renew_success: float = field(default=0.0, init=False)

    # ── pure core ───────────────────────────────────────────────────────────────

    def try_acquire_or_renew(self, now: float) -> LeaseAttempt:
        """Single tick: try to create / take over / renew the Lease.

        Side effects: flips :attr:`is_leader` and fires callbacks on transition,
        and records the last successful renew time.
        """
        try:
            observed = self.backend.get()
        except Exception as e:  # noqa: BLE001 — transient backend error
            logger.debug("lease get failed: %s", e)
            return LeaseAttempt.FAILED

        if observed is None:
            fresh = LeaseRecord(
                holder_identity=self.identity,
                lease_duration_sec=self.lease_duration_sec,
                acquire_time=now,
                renew_time=now,
                lease_transitions=0,
            )
            try:
                self.backend.create(fresh)
            except LeaseConflict:
                return LeaseAttempt.FAILED
            except Exception as e:  # noqa: BLE001
                logger.debug("lease create failed: %s", e)
                return LeaseAttempt.FAILED
            return self._mark_leader(now, acquired=True)

        held_by_us = observed.holder_identity == self.identity
        if not held_by_us and not observed.is_expired(now):
            # A different pod holds a still-valid lease: we are (or just became) standby.
            return self._mark_not_leader(definitive=True)

        candidate = replace(
            observed,
            holder_identity=self.identity,
            lease_duration_sec=self.lease_duration_sec,
            renew_time=now,
        )
        if held_by_us:
            acquired = False  # renewal — no leadership transition
        else:
            candidate.acquire_time = now
            candidate.lease_transitions = observed.lease_transitions + 1
            acquired = True  # taking over an expired lease

        try:
            self.backend.update(candidate)
        except LeaseConflict:
            return LeaseAttempt.FAILED
        except Exception as e:  # noqa: BLE001
            logger.debug("lease update failed: %s", e)
            return LeaseAttempt.FAILED
        return self._mark_leader(now, acquired=acquired)

    def _mark_leader(self, now: float, *, acquired: bool) -> LeaseAttempt:
        self._last_renew_success = now
        was_leader = self.is_leader
        self.is_leader = True
        if not was_leader:
            logger.info("lease %s ACQUIRED by %s", _lease_label(self.backend), self.identity)
            _safe_call(self.on_started_leading)
        return LeaseAttempt.ACQUIRED if acquired or not was_leader else LeaseAttempt.RENEWED

    def _mark_not_leader(self, *, definitive: bool) -> LeaseAttempt:
        if self.is_leader:
            logger.warning(
                "lease %s LOST by %s (another holder)", _lease_label(self.backend), self.identity
            )
            self.is_leader = False
            _safe_call(self.on_stopped_leading)
        return LeaseAttempt.HELD_BY_OTHER if definitive else LeaseAttempt.FAILED

    def _expire_self(self) -> None:
        """Demote after the renew deadline elapsed without a successful renew."""
        if self.is_leader:
            logger.warning(
                "lease %s renew deadline exceeded — %s stepping down",
                _lease_label(self.backend),
                self.identity,
            )
            self.is_leader = False
            _safe_call(self.on_stopped_leading)

    # ── async driver ──────────────────────────────────────────────────────────────

    async def acquire(self, stop: asyncio.Event) -> bool:
        """Block (as standby) until this pod becomes leader or ``stop`` is set."""
        while not stop.is_set():
            attempt = self.try_acquire_or_renew(self.clock())
            if attempt in (LeaseAttempt.ACQUIRED, LeaseAttempt.RENEWED):
                return True
            await _wait_or_stop(stop, self.retry_period_sec)
        return False

    async def renew_until_lost(self, stop: asyncio.Event) -> None:
        """Renew on a timer while leading; return once leadership is lost/stopped."""
        while not stop.is_set():
            await _wait_or_stop(stop, self.retry_period_sec)
            if stop.is_set():
                return
            attempt = self.try_acquire_or_renew(self.clock())
            if attempt in (LeaseAttempt.ACQUIRED, LeaseAttempt.RENEWED):
                continue
            if attempt is LeaseAttempt.HELD_BY_OTHER:
                return  # definitive loss — already demoted
            # transient FAILED: tolerate until renew deadline, then step down.
            if (self.clock() - self._last_renew_success) > self.renew_deadline_sec:
                self._expire_self()
                return


# ── helpers ──────────────────────────────────────────────────────────────────────


def _safe_call(cb: Optional[Callable[[], None]]) -> None:
    if cb is None:
        return
    try:
        cb()
    except Exception as e:  # noqa: BLE001 — callbacks must never break the elector
        logger.warning("leader-election callback error: %s", e)


def _lease_label(backend: LeaseBackend) -> str:
    ns = getattr(backend, "_namespace", None)
    name = getattr(backend, "_name", None)
    if ns and name:
        return f"{ns}/{name}"
    return type(backend).__name__


async def _wait_or_stop(stop: asyncio.Event, timeout: float) -> None:
    """Sleep ``timeout`` seconds unless ``stop`` is set first."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=timeout)
    except asyncio.TimeoutError:
        pass


# ── entrypoint orchestration ─────────────────────────────────────────────────────


async def run_with_leadership(
    elector: LeaderElector,
    make_service_coro: Callable[[], Awaitable[None]],
    stop: asyncio.Event,
) -> None:
    """Run ``make_service_coro`` only while this pod holds the Lease.

    Standby pods wait in :meth:`LeaderElector.acquire` without opening any IB
    socket. Once leader, the service runs alongside a renew loop. If leadership is
    lost (renew deadline / takeover) the service task is cancelled and control
    returns — the entrypoint then exits so K8s restarts the pod as a fresh standby
    (the client-go ``OnStoppedLeading`` = exit pattern). This guarantees at most
    one live IB ``eConnect`` per Lease, so Error 326 cannot occur.
    """
    if not await elector.acquire(stop):
        return  # stop requested before we ever led

    service_task = asyncio.create_task(make_service_coro())
    renew_task = asyncio.create_task(elector.renew_until_lost(stop))
    try:
        done, _pending = await asyncio.wait(
            {service_task, renew_task}, return_when=asyncio.FIRST_COMPLETED
        )
    finally:
        if not service_task.done():
            service_task.cancel()
        if not renew_task.done():
            renew_task.cancel()
        for task in (service_task, renew_task):
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
    if service_task in done and service_task.exception() is not None:
        raise service_task.exception()  # surface real service crashes


def run_with_leadership_sync(
    elector: LeaderElector,
    run_service: Callable[[threading.Event], None],
    stop: threading.Event,
) -> None:
    """Synchronous variant for the IB Operator loop (thread + threading.Event).

    Bridges a :class:`threading.Event` stop into the async leadership orchestration.
    ``run_service`` receives an inner stop event that is set when leadership is
    lost or the outer stop fires, so the operator loop unwinds and disconnects IB.
    """

    async def _main() -> None:
        async_stop = asyncio.Event()

        async def _watch_outer_stop() -> None:
            while not async_stop.is_set():
                if stop.is_set():
                    async_stop.set()
                    return
                await _wait_or_stop(async_stop, 0.5)

        watcher = asyncio.create_task(_watch_outer_stop())
        inner_stop = threading.Event()

        async def _service() -> None:
            try:
                await asyncio.to_thread(run_service, inner_stop)
            finally:
                inner_stop.set()

        def _signal_inner_stop() -> None:
            inner_stop.set()

        # Step down callback must release the operator loop.
        prev = elector.on_stopped_leading
        elector.on_stopped_leading = lambda: (_signal_inner_stop(), _safe_call(prev))

        try:
            await run_with_leadership(elector, _service, async_stop)
        finally:
            inner_stop.set()
            async_stop.set()
            watcher.cancel()
            try:
                await watcher
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass

    asyncio.run(_main())


# ── config ───────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class IbLeaseSettings:
    enabled: bool
    namespace: str
    name: str
    identity: str
    lease_duration_sec: float
    renew_deadline_sec: float
    retry_period_sec: float


def ib_lease_settings(cfg: Dict[str, Any], role: str) -> IbLeaseSettings:
    """Resolve :class:`IbLeaseSettings` from loaded config + env for an IB role."""
    from bifrost_socket.config import get_ib_lease_settings  # avoid import cycle

    raw = get_ib_lease_settings(cfg, role)
    return IbLeaseSettings(
        enabled=bool(raw["enabled"]),
        namespace=str(raw["namespace"]),
        name=str(raw["name"]),
        identity=str(raw["identity"]),
        lease_duration_sec=float(raw["lease_duration_sec"]),
        renew_deadline_sec=float(raw["renew_deadline_sec"]),
        retry_period_sec=float(raw["retry_period_sec"]),
    )


def build_lease_backend(settings: IbLeaseSettings) -> LeaseBackend:
    """Construct the real Kubernetes backend from resolved settings."""
    return KubernetesLeaseBackend(namespace=settings.namespace, name=settings.name)


def build_elector(
    settings: IbLeaseSettings,
    backend: LeaseBackend,
    *,
    on_started_leading: Optional[Callable[[], None]] = None,
    on_stopped_leading: Optional[Callable[[], None]] = None,
) -> LeaderElector:
    return LeaderElector(
        backend=backend,
        identity=settings.identity,
        lease_duration_sec=settings.lease_duration_sec,
        renew_deadline_sec=settings.renew_deadline_sec,
        retry_period_sec=settings.retry_period_sec,
        on_started_leading=on_started_leading,
        on_stopped_leading=on_stopped_leading,
    )


# ── entrypoint convenience (used by run_ib_* scripts) ─────────────────────────────


def run_async_ib_service(
    cfg: Dict[str, Any],
    role: str,
    make_service_coro: Callable[[], Awaitable[None]],
) -> None:
    """Run an asyncio IB service (ingestor / account agent), gated by the Lease.

    When ``ib.lease.enabled`` is false this is a transparent ``asyncio.run`` of the
    service, so single-process / Compose runs are unchanged. When enabled, the pod
    waits as standby until it holds the Lease, runs the service while leader, and
    returns (process exits) on leadership loss.
    """
    settings = ib_lease_settings(cfg, role)
    if not settings.enabled:
        asyncio.run(make_service_coro())
        return

    async def _main() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
            except NotImplementedError:
                pass
        backend = build_lease_backend(settings)
        elector = build_elector(
            settings,
            backend,
            on_started_leading=lambda: logger.warning(
                "IB %s became LEADER (%s) — opening IB connection", role, settings.identity
            ),
            on_stopped_leading=lambda: logger.warning(
                "IB %s lost leadership (%s) — releasing IB connection, exiting for restart",
                role,
                settings.identity,
            ),
        )
        logger.warning(
            "IB %s leader election ENABLED — lease %s/%s id=%s; standby until acquired",
            role,
            settings.namespace,
            settings.name,
            settings.identity,
        )
        await run_with_leadership(elector, make_service_coro, stop)

    asyncio.run(_main())


def run_sync_ib_service(
    cfg: Dict[str, Any],
    role: str,
    run_service: Callable[[threading.Event], None],
    stop: threading.Event,
) -> None:
    """Run a synchronous IB service (operator loop), gated by the Lease.

    ``run_service`` takes a :class:`threading.Event` it must honour to stop. When
    leadership is lost the inner stop event is set so the operator loop unwinds and
    disconnects IB. Transparent passthrough when the Lease is disabled.
    """
    settings = ib_lease_settings(cfg, role)
    if not settings.enabled:
        run_service(stop)
        return

    backend = build_lease_backend(settings)
    elector = build_elector(
        settings,
        backend,
        on_started_leading=lambda: logger.warning(
            "IB %s became LEADER (%s) — opening IB connection", role, settings.identity
        ),
    )
    logger.warning(
        "IB %s leader election ENABLED — lease %s/%s id=%s; standby until acquired",
        role,
        settings.namespace,
        settings.name,
        settings.identity,
    )
    run_with_leadership_sync(elector, run_service, stop)
