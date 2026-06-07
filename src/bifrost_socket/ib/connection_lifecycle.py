"""Unified IB Broker connection lifecycle: policy, heartbeat, retry, Message Center.

Shared by ib_ingestor, ib_account_agent, and ib_operator. Process models differ
(asyncio vs sync operator loop) but semantics are identical:
  - IB liveness probe every ``probe_interval_sec`` (~5s)
  - Service heartbeat every ``service_heartbeat_interval_sec`` (~30s)
  - At most one ``ensure_connected(1)`` per slot per heartbeat tick
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Literal, Optional, Sequence

from bifrost_core.core.message_center import (
    IB_DISCONNECT_REASON_SERVICE_STOPPED,
    IB_DISCONNECT_REASON_SESSION_ENDED,
    IbConnectionStatusTracker,
    publish_ib_connection_transition,
)

from bifrost_socket.config import get_effective_ib_config

logger = logging.getLogger(__name__)

# ── Constants (single source of truth) ───────────────────────────────────────

SERVICE_HEARTBEAT_CONNECT_TIMEOUT_SEC = 5.0
DEFAULT_SERVICE_HEARTBEAT_REFRESH_SEC = 30.0
MAX_CONNECT_ATTEMPTS_PER_HEARTBEAT = 1

REASON_SESSION_ENDED = IB_DISCONNECT_REASON_SESSION_ENDED
REASON_SERVICE_STOPPED = IB_DISCONNECT_REASON_SERVICE_STOPPED

IbBrokerService = Literal["ib_ingestor", "ib_account_agent", "ib_operator"]

_SETTINGS_KEYS: Dict[IbBrokerService, tuple[str, ...]] = {
    "ib_ingestor": ("ib_ingestor", "ib_market_ingest"),
    "ib_account_agent": ("ib_account_agent",),
    "ib_operator": ("ib_operator",),
}


# ── Policy ───────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class IbBrokerLifecycleConfig:
    probe_interval_sec: float
    service_heartbeat_interval_sec: float
    connect_timeout_sec: float


def _service_settings_block(cfg: Dict[str, Any], service: IbBrokerService) -> Dict[str, Any]:
    for key in _SETTINGS_KEYS[service]:
        raw = cfg.get(key)
        if isinstance(raw, dict):
            return raw
    return {}


def effective_service_heartbeat_interval_sec(
    service_cfg: Dict[str, Any],
    probe_interval_sec: float,
    *,
    default_refresh_sec: float = DEFAULT_SERVICE_HEARTBEAT_REFRESH_SEC,
) -> float:
    """Main-process heartbeat interval: at least YAML health_refresh_sec and probe interval."""
    hr = float((service_cfg or {}).get("health_refresh_sec") or default_refresh_sec)
    return max(hr, float(probe_interval_sec))


def resolve_ib_broker_lifecycle(
    cfg: Dict[str, Any],
    service: IbBrokerService,
    *,
    connect_timeout_sec: Optional[float] = None,
) -> IbBrokerLifecycleConfig:
    ib_eff = get_effective_ib_config(cfg)
    probe = float(ib_eff["ib_probe_interval_sec"])
    st = _service_settings_block(cfg, service)
    heartbeat_iv = effective_service_heartbeat_interval_sec(st, probe)
    # IB API sync (positions, farms) often exceeds 5s — use YAML ib.connect_timeout (default 60s).
    ct = float(
        connect_timeout_sec
        if connect_timeout_sec is not None
        else ib_eff.get("connect_timeout") or 60.0
    )
    return IbBrokerLifecycleConfig(
        probe_interval_sec=probe,
        service_heartbeat_interval_sec=heartbeat_iv,
        connect_timeout_sec=ct,
    )


# ── Service heartbeat clock + Redis fields ───────────────────────────────────


class ServiceHeartbeatClock:
    """Tracks last service heartbeat time and builds Redis countdown fields."""

    def __init__(
        self,
        interval_sec: float,
        *,
        last_at: Optional[float] = None,
    ) -> None:
        self._interval_sec = float(interval_sec)
        self._last_at = float(last_at if last_at is not None else time.time())

    @property
    def interval_sec(self) -> float:
        return self._interval_sec

    @property
    def last_at(self) -> float:
        return self._last_at

    def tick(self, now: float) -> bool:
        """Return True when a service heartbeat tick fires (and advance last_at)."""
        if self._last_at <= 0:
            self._last_at = now
            return False
        if now - self._last_at >= self._interval_sec:
            self._last_at = now
            return True
        return False

    def redis_fields(self, now: float, *, reconnect_in_progress: str = "") -> Dict[str, Any]:
        iv = self._interval_sec
        lh = self._last_at
        nx = max(0.0, lh + iv - now) if lh > 0 else iv
        out: Dict[str, Any] = {
            "service_heartbeat_interval_sec": iv,
            "last_service_heartbeat_at": lh,
            "next_service_heartbeat_in_s": nx,
        }
        if reconnect_in_progress:
            out["service_heartbeat_reconnect_in_progress"] = reconnect_in_progress
        return out

    @staticmethod
    def reconnect_hint_part(slot_label: str, client_id: int) -> str:
        return f"{slot_label} (client {client_id})"

    @staticmethod
    def reconnect_hint_join(*parts: str) -> str:
        return ", ".join(p for p in parts if p)


# ── Heartbeat reconnect (one attempt per slot per tick) ────────────────────────


@dataclass(frozen=True)
class HeartbeatReconnectTarget:
    slot_label: str
    client_id: int
    reconnect: Callable[[], Awaitable[None]]


async def heartbeat_reconnect_slots_parallel(
    targets: Sequence[HeartbeatReconnectTarget],
    *,
    connect_timeout_sec: float = SERVICE_HEARTBEAT_CONNECT_TIMEOUT_SEC,
    log_prefix: str = "IB",
) -> None:
    """Run one reconnect attempt per target in parallel; failures stay local."""

    async def _one(target: HeartbeatReconnectTarget) -> None:
        try:
            await asyncio.wait_for(target.reconnect(), timeout=connect_timeout_sec)
        except asyncio.TimeoutError:
            logger.debug(
                "%s %s heartbeat connect timed out after %.0fs",
                log_prefix,
                target.slot_label,
                connect_timeout_sec,
            )
        except Exception as e:
            logger.debug("%s %s heartbeat connect: %s", log_prefix, target.slot_label, e)

    if targets:
        await asyncio.gather(*[_one(t) for t in targets])


async def heartbeat_reconnect_slot(
    client: Any,
    *,
    slot_label: str,
    client_id: int,
    connect_timeout_sec: float = SERVICE_HEARTBEAT_CONNECT_TIMEOUT_SEC,
    log_prefix: str = "IB",
) -> None:
    """Single-slot ``ensure_connected(1)`` capped by connect timeout."""

    async def _reconnect() -> None:
        await client.ensure_connected(max_connect_attempts=MAX_CONNECT_ATTEMPTS_PER_HEARTBEAT)

    await heartbeat_reconnect_slots_parallel(
        [
            HeartbeatReconnectTarget(
                slot_label=slot_label,
                client_id=client_id,
                reconnect=_reconnect,
            )
        ],
        connect_timeout_sec=connect_timeout_sec,
        log_prefix=log_prefix,
    )


def run_heartbeat_reconnect_sync(coro: Awaitable[None]) -> None:
    """Run async heartbeat reconnect from sync contexts (IB Operator main loop)."""
    try:
        asyncio.run(coro)
    except RuntimeError:
        # Nested event loop (tests): fall back to existing loop policy.
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(coro)
        finally:
            loop.close()


# ── Message Center publishing ─────────────────────────────────────────────────


@dataclass(frozen=True)
class IbSlotSnapshot:
    slot: str
    connected: bool
    client_id: Optional[int]
    reason: Optional[str] = None


def _coerce_client_id(raw: Any) -> Optional[int]:
    try:
        n = int(raw or 0)
        return n if n > 0 else None
    except (TypeError, ValueError):
        return None


def publish_ib_slots(
    tracker: IbConnectionStatusTracker,
    *,
    slots: Sequence[IbSlotSnapshot],
    occurred_at: float,
    default_reason: Optional[str] = None,
) -> None:
    for snap in slots:
        tracker.update(
            slot=snap.slot,
            status="connected" if snap.connected else "disconnected",
            client_id=snap.client_id,
            occurred_at=occurred_at,
            reason=snap.reason if snap.reason is not None else default_reason,
        )


def slots_from_operator_health(
    health_dict: Dict[str, Any],
    *,
    default_reason: Optional[str] = None,
) -> list[IbSlotSnapshot]:
    host = health_dict.get("host") if isinstance(health_dict.get("host"), dict) else {}
    slots: list[IbSlotSnapshot] = []
    if host:
        host_reason = default_reason
        if host_reason is None and host.get("last_error") is not None:
            host_reason = (str(host.get("last_error")).strip() or None)
        slots.append(
            IbSlotSnapshot(
                slot="host",
                connected=bool(host.get("connected")),
                client_id=_coerce_client_id(host.get("client_id")),
                reason=host_reason,
            )
        )
    secondary = health_dict.get("secondary")
    if isinstance(secondary, dict):
        sec_reason = default_reason
        if sec_reason is None and secondary.get("last_error") is not None:
            sec_reason = (str(secondary.get("last_error")).strip() or None)
        slots.append(
            IbSlotSnapshot(
                slot="secondary",
                connected=bool(secondary.get("connected")),
                client_id=_coerce_client_id(secondary.get("client_id")),
                reason=sec_reason,
            )
        )
    return slots


def publish_ingestor_slot(
    tracker: IbConnectionStatusTracker,
    *,
    connected: bool,
    client_id: Optional[int],
    occurred_at: float,
    reason: Optional[str] = None,
) -> None:
    publish_ib_slots(
        tracker,
        slots=[IbSlotSnapshot(slot="host", connected=connected, client_id=client_id, reason=reason)],
        occurred_at=occurred_at,
        default_reason=reason,
    )


def publish_account_agent_slots(
    tracker: IbConnectionStatusTracker,
    *,
    host_connected: bool,
    host_client_id: Optional[int],
    occurred_at: float,
    secondary_connected: Optional[bool] = None,
    secondary_client_id: Optional[int] = None,
    reason: Optional[str] = None,
) -> None:
    slots: list[IbSlotSnapshot] = [
        IbSlotSnapshot(slot="host", connected=host_connected, client_id=host_client_id, reason=reason),
    ]
    if secondary_connected is not None:
        slots.append(
            IbSlotSnapshot(
                slot="secondary",
                connected=secondary_connected,
                client_id=secondary_client_id,
                reason=reason,
            )
        )
    publish_ib_slots(tracker, slots=slots, occurred_at=occurred_at, default_reason=reason)


def publish_operator_slots(
    tracker: IbConnectionStatusTracker,
    health_dict: Dict[str, Any],
    *,
    reason: Optional[str] = None,
) -> None:
    occurred_at = float(health_dict.get("updated_at") or time.time())
    publish_ib_slots(
        tracker,
        slots=slots_from_operator_health(health_dict, default_reason=reason),
        occurred_at=occurred_at,
        default_reason=reason,
    )


def publish_service_stopped_disconnects(
    r: Any,
    *,
    mc_service: str,
    slot_client_ids: Sequence[tuple[str, int]],
    occurred_at: Optional[float] = None,
) -> None:
    """Force disconnect toasts on graceful service stop (bypasses in-process tracker state).

    Dual-slot services (Account Agent / Operator) may already show IB disconnected in Redis
    while ``host_alive`` is still true; Ops stop and process exit must still emit one toast
    per configured slot with a known client_id.
    """
    ts = float(occurred_at or time.time())
    seen: set[tuple[str, int]] = set()
    for slot, client_id in slot_client_ids:
        cid = int(client_id or 0)
        if cid <= 0:
            continue
        key = (_sanitize_mc_slot(slot), cid)
        if key in seen:
            continue
        seen.add(key)
        publish_ib_connection_transition(
            r,
            service=mc_service,
            slot=slot,
            client_id=cid,
            status_from="connected",
            status_to="disconnected",
            reason=REASON_SERVICE_STOPPED,
            occurred_at=ts,
        )


def _sanitize_mc_slot(slot: str) -> str:
    s = str(slot or "host").strip().lower()
    return s or "host"
