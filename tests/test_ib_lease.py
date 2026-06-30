"""W4 trade-k8s-native: K8s Lease leader election (active-standby) for IB edge.

Verifies the verify-bar from tradeK8sNativeCatalog.ts wave W4:
"scale socket to 2 pods — only one holds IB connection".
"""

from __future__ import annotations

import asyncio
from typing import List, Optional

import pytest

from bifrost_socket.config import get_ib_lease_settings
from bifrost_socket.ib.lease import (
    DEFAULT_LEASE_DURATION_SEC,
    InMemoryLeaseBackend,
    LeaderElector,
    LeaseAttempt,
    LeaseConflict,
    LeaseRecord,
    run_with_leadership,
)


class _Clock:
    """Deterministic injectable clock."""

    def __init__(self, t: float = 0.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


# ── LeaseRecord ──────────────────────────────────────────────────────────────────


def test_lease_record_expiry() -> None:
    rec = LeaseRecord(holder_identity="A", lease_duration_sec=15.0, renew_time=100.0)
    assert not rec.is_expired(110.0)
    assert not rec.is_expired(115.0)
    assert rec.is_expired(115.01)


# ── InMemoryLeaseBackend (optimistic concurrency) ────────────────────────────────


def test_inmemory_create_then_conflict() -> None:
    be = InMemoryLeaseBackend()
    assert be.get() is None
    created = be.create(LeaseRecord(holder_identity="A"))
    assert created.resource_version == "1"
    with pytest.raises(LeaseConflict):
        be.create(LeaseRecord(holder_identity="B"))


def test_inmemory_update_cas() -> None:
    be = InMemoryLeaseBackend()
    created = be.create(LeaseRecord(holder_identity="A", renew_time=1.0))
    # Stale version (None) must conflict.
    with pytest.raises(LeaseConflict):
        be.update(LeaseRecord(holder_identity="A", renew_time=2.0, resource_version=None))
    bumped = be.update(LeaseRecord(holder_identity="A", renew_time=2.0,
                                   resource_version=created.resource_version))
    assert bumped.resource_version == "2"
    # The previously-read version is now stale.
    with pytest.raises(LeaseConflict):
        be.update(LeaseRecord(holder_identity="A", renew_time=3.0,
                              resource_version=created.resource_version))


# ── LeaderElector pure core ──────────────────────────────────────────────────────


def _elector(backend: InMemoryLeaseBackend, identity: str, clock: _Clock,
             started: Optional[List[str]] = None,
             stopped: Optional[List[str]] = None) -> LeaderElector:
    return LeaderElector(
        backend=backend,
        identity=identity,
        lease_duration_sec=15.0,
        renew_deadline_sec=10.0,
        retry_period_sec=2.0,
        clock=clock,
        on_started_leading=(lambda: started.append(identity)) if started is not None else None,
        on_stopped_leading=(lambda: stopped.append(identity)) if stopped is not None else None,
    )


def test_acquire_on_empty_backend() -> None:
    be = InMemoryLeaseBackend()
    clock = _Clock(0.0)
    started: List[str] = []
    el = _elector(be, "A", clock, started=started)
    assert el.try_acquire_or_renew(0.0) is LeaseAttempt.ACQUIRED
    assert el.is_leader is True
    assert started == ["A"]
    rec = be.get()
    assert rec is not None and rec.holder_identity == "A"


def test_renew_keeps_leadership_without_extra_callback() -> None:
    be = InMemoryLeaseBackend()
    clock = _Clock(0.0)
    started: List[str] = []
    el = _elector(be, "A", clock, started=started)
    el.try_acquire_or_renew(0.0)
    clock.t = 3.0
    assert el.try_acquire_or_renew(3.0) is LeaseAttempt.RENEWED
    assert el.is_leader is True
    assert started == ["A"]  # started fires once only
    rec = be.get()
    assert rec is not None and rec.renew_time == 3.0
    assert rec.lease_transitions == 0  # renewal, no transition


def test_standby_sees_valid_other_holder() -> None:
    be = InMemoryLeaseBackend()
    clock = _Clock(0.0)
    a = _elector(be, "A", clock)
    b = _elector(be, "B", clock)
    assert a.try_acquire_or_renew(0.0) is LeaseAttempt.ACQUIRED
    # B at t=1 sees A's still-valid lease → standby, never leader.
    assert b.try_acquire_or_renew(1.0) is LeaseAttempt.HELD_BY_OTHER
    assert b.is_leader is False


def test_takeover_after_expiry_increments_transitions() -> None:
    be = InMemoryLeaseBackend()
    clock = _Clock(0.0)
    started_b: List[str] = []
    a = _elector(be, "A", clock)
    b = _elector(be, "B", clock, started=started_b)
    a.try_acquire_or_renew(0.0)
    # A stops renewing; lease expires after 15s. B takes over at t=20.
    assert b.try_acquire_or_renew(20.0) is LeaseAttempt.ACQUIRED
    assert b.is_leader is True
    assert started_b == ["B"]
    rec = be.get()
    assert rec is not None and rec.holder_identity == "B"
    assert rec.lease_transitions == 1


def test_old_leader_steps_down_when_other_takes_over() -> None:
    be = InMemoryLeaseBackend()
    clock = _Clock(0.0)
    stopped_a: List[str] = []
    a = _elector(be, "A", clock, stopped=stopped_a)
    b = _elector(be, "B", clock)
    a.try_acquire_or_renew(0.0)
    assert a.is_leader is True
    b.try_acquire_or_renew(20.0)  # B takes over expired lease
    # A now observes a different valid holder → definitive loss + callback.
    assert a.try_acquire_or_renew(21.0) is LeaseAttempt.HELD_BY_OTHER
    assert a.is_leader is False
    assert stopped_a == ["A"]


def test_single_leader_invariant_two_electors() -> None:
    """Core W4 invariant: at most one elector reports leadership at any time."""
    be = InMemoryLeaseBackend()
    clock = _Clock(0.0)
    a = _elector(be, "A", clock)
    b = _elector(be, "B", clock)
    for t in range(0, 60):
        clock.t = float(t)
        # Both pods attempt every tick; A keeps renewing, B keeps probing.
        a.try_acquire_or_renew(float(t))
        b.try_acquire_or_renew(float(t))
        assert not (a.is_leader and b.is_leader), f"two leaders at t={t}"
        assert a.is_leader or b.is_leader, f"no leader at t={t}"
    # A renewed continuously, so A remains the sole leader.
    assert a.is_leader and not b.is_leader


# ── transient failure handling ───────────────────────────────────────────────────


class _ConflictOnUpdateBackend:
    """Holds a record but always rejects update() with a conflict (transient churn)."""

    def __init__(self, record: LeaseRecord) -> None:
        self._record = record

    def get(self) -> Optional[LeaseRecord]:
        return self._record

    def create(self, record: LeaseRecord) -> LeaseRecord:  # pragma: no cover - unused
        raise LeaseConflict("exists")

    def update(self, record: LeaseRecord) -> LeaseRecord:
        raise LeaseConflict("always conflict")


def test_transient_conflict_returns_failed_without_immediate_demote() -> None:
    rec = LeaseRecord(holder_identity="A", lease_duration_sec=15.0, renew_time=0.0,
                      resource_version="1")
    be = _ConflictOnUpdateBackend(rec)
    el = LeaderElector(backend=be, identity="A", clock=_Clock(1.0))
    el.is_leader = True
    el._last_renew_success = 1.0
    # We still appear as the holder; a single conflict is transient, not a demote.
    assert el.try_acquire_or_renew(1.0) is LeaseAttempt.FAILED
    assert el.is_leader is True


async def test_renew_until_lost_steps_down_after_deadline() -> None:
    rec = LeaseRecord(holder_identity="A", lease_duration_sec=10.0, renew_time=0.0,
                      resource_version="1")
    be = _ConflictOnUpdateBackend(rec)
    stopped: List[str] = []
    el = LeaderElector(
        backend=be,
        identity="A",
        lease_duration_sec=10.0,
        renew_deadline_sec=0.05,
        retry_period_sec=0.01,
        on_stopped_leading=lambda: stopped.append("A"),
    )
    el.is_leader = True
    el._last_renew_success = el.clock()
    stop = asyncio.Event()
    await asyncio.wait_for(el.renew_until_lost(stop), timeout=2.0)
    assert el.is_leader is False
    assert stopped == ["A"]


# ── async orchestration ──────────────────────────────────────────────────────────


async def test_run_with_leadership_runs_service_while_leader() -> None:
    be = InMemoryLeaseBackend()
    el = LeaderElector(backend=be, identity="A", lease_duration_sec=10.0,
                       renew_deadline_sec=5.0, retry_period_sec=0.01)
    ran = asyncio.Event()
    stop = asyncio.Event()

    async def service() -> None:
        ran.set()
        await asyncio.sleep(100)  # runs until cancelled

    task = asyncio.create_task(run_with_leadership(el, service, stop))
    await asyncio.wait_for(ran.wait(), timeout=2.0)
    assert el.is_leader is True
    stop.set()
    await asyncio.wait_for(task, timeout=2.0)


async def test_standby_never_starts_service_then_exits_on_stop() -> None:
    be = InMemoryLeaseBackend()
    # Pre-seed a valid lease held by someone else so our elector stays standby.
    be.create(LeaseRecord(holder_identity="other", lease_duration_sec=300.0,
                          acquire_time=0.0, renew_time=1e12))
    el = LeaderElector(backend=be, identity="me", lease_duration_sec=10.0,
                       renew_deadline_sec=5.0, retry_period_sec=0.01,
                       clock=lambda: 1e12)
    started: List[str] = []
    stop = asyncio.Event()

    async def service() -> None:  # pragma: no cover - must never run
        started.append("service")

    task = asyncio.create_task(run_with_leadership(el, service, stop))
    await asyncio.sleep(0.1)
    assert el.is_leader is False
    assert started == []  # standby never opened the IB service
    stop.set()
    await asyncio.wait_for(task, timeout=2.0)
    assert started == []


# ── config resolution ────────────────────────────────────────────────────────────


def test_lease_settings_disabled_by_default() -> None:
    s = get_ib_lease_settings({}, "ib_market_gateway")
    assert s["enabled"] is False
    assert s["name"] == "bifrost-ib-market"
    assert s["lease_duration_sec"] == DEFAULT_LEASE_DURATION_SEC


def test_lease_settings_role_default_names() -> None:
    assert get_ib_lease_settings({}, "ib_account_agent")["name"] == "bifrost-ib-account"
    assert get_ib_lease_settings({}, "ib_operator")["name"] == "bifrost-ib-operator"
    assert get_ib_lease_settings({}, "ib_ingestor")["name"] == "bifrost-ib-market"


def test_lease_settings_yaml_enable_and_tuning() -> None:
    cfg = {
        "ib": {
            "lease": {
                "enabled": True,
                "namespace": "bifrost-stg",
                "name": "custom-lease",
                "lease_duration_sec": 20,
                "renew_deadline_sec": 12,
                "retry_period_sec": 3,
            }
        }
    }
    s = get_ib_lease_settings(cfg, "ib_ingestor")
    assert s["enabled"] is True
    assert s["namespace"] == "bifrost-stg"
    assert s["name"] == "custom-lease"
    assert s["lease_duration_sec"] == 20.0
    assert s["renew_deadline_sec"] == 12.0
    assert s["retry_period_sec"] == 3.0


def test_lease_settings_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BIFROST_IB_LEASE_ENABLED", "1")
    monkeypatch.setenv("POD_NAMESPACE", "bifrost-prod")
    monkeypatch.setenv("POD_NAME", "ib-ingestor-0")
    monkeypatch.setenv("BIFROST_IB_LEASE_NAME", "env-lease")
    s = get_ib_lease_settings({"ib": {"lease": {"enabled": False}}}, "ib_ingestor")
    assert s["enabled"] is True  # env wins over YAML
    assert s["namespace"] == "bifrost-prod"
    assert s["identity"] == "ib-ingestor-0"
    assert s["name"] == "env-lease"
