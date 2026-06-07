"""Unit tests for unified IB broker connection lifecycle."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from bifrost_socket.ib.connection_lifecycle import (
    HeartbeatReconnectTarget,
    ServiceHeartbeatClock,
    effective_service_heartbeat_interval_sec,
    heartbeat_reconnect_slot,
    heartbeat_reconnect_slots_parallel,
    publish_service_stopped_disconnects,
    resolve_ib_broker_lifecycle,
)


def test_effective_service_heartbeat_interval_sec() -> None:
    st = {"health_refresh_sec": 10}
    assert effective_service_heartbeat_interval_sec(st, 5.0) == 10.0
    assert effective_service_heartbeat_interval_sec(st, 15.0) == 15.0
    assert effective_service_heartbeat_interval_sec({}, 12.0) == 30.0


def _minimal_ib_cfg(**extra: object) -> dict:
    base = {
        "ib": {
            "probe_interval_sec": 5,
            "host": {"ip": "127.0.0.1", "port_type": "tws_paper", "client_id": {}},
        },
    }
    base.update(extra)
    return base


def test_resolve_ib_broker_lifecycle_settings_keys() -> None:
    cfg = _minimal_ib_cfg(
        ib_ingestor={"health_refresh_sec": 10},
        ib_account_agent={"health_refresh_sec": 20},
        ib_operator={"health_refresh_sec": 25},
    )
    ing = resolve_ib_broker_lifecycle(cfg, "ib_ingestor")
    assert ing.service_heartbeat_interval_sec == 10.0
    assert ing.probe_interval_sec == 5.0

    agent = resolve_ib_broker_lifecycle(cfg, "ib_account_agent")
    assert agent.service_heartbeat_interval_sec == 20.0

    op = resolve_ib_broker_lifecycle(cfg, "ib_operator")
    assert op.service_heartbeat_interval_sec == 25.0
    assert op.connect_timeout_sec == 60.0

    custom = resolve_ib_broker_lifecycle(
        _minimal_ib_cfg(ib={"connect_timeout": 45.0, "probe_interval_sec": 5, "host": {"ip": "127.0.0.1", "port_type": "tws_paper", "client_id": {}}}),
        "ib_operator",
    )
    assert custom.connect_timeout_sec == 45.0

    fallback = resolve_ib_broker_lifecycle(
        _minimal_ib_cfg(ib_market_ingest={"health_refresh_sec": 12}),
        "ib_ingestor",
    )
    assert fallback.service_heartbeat_interval_sec == 12.0


def test_service_heartbeat_clock_tick_and_redis_fields() -> None:
    clock = ServiceHeartbeatClock(30.0, last_at=100.0)

    fields = clock.redis_fields(123.0)
    assert fields["service_heartbeat_interval_sec"] == 30.0
    assert abs(float(fields["next_service_heartbeat_in_s"]) - 7.0) < 0.02

    assert clock.tick(110.0) is False
    assert clock.tick(130.0) is True
    assert clock.last_at == 130.0
    assert "service_heartbeat_reconnect_in_progress" not in fields

    with_hint = clock.redis_fields(123.0, reconnect_in_progress="Host (client 50)")
    assert with_hint["service_heartbeat_reconnect_in_progress"] == "Host (client 50)"


@pytest.mark.asyncio
async def test_heartbeat_reconnect_slot_calls_ensure_connected_once() -> None:
    client = MagicMock()
    client.ensure_connected = AsyncMock()
    await heartbeat_reconnect_slot(client, slot_label="Host", client_id=50)
    client.ensure_connected.assert_awaited_once_with(max_connect_attempts=1)


@pytest.mark.asyncio
async def test_heartbeat_reconnect_slots_parallel_isolates_failures() -> None:
    calls: list[str] = []

    async def ok() -> None:
        calls.append("ok")

    async def fail() -> None:
        calls.append("fail")
        raise RuntimeError("boom")

    await heartbeat_reconnect_slots_parallel(
        [
            HeartbeatReconnectTarget(slot_label="Host", client_id=1, reconnect=ok),
            HeartbeatReconnectTarget(slot_label="Secondary", client_id=2, reconnect=fail),
        ],
        connect_timeout_sec=1.0,
    )
    assert calls == ["ok", "fail"]


class _FakeRedis:
    def __init__(self) -> None:
        self.stream: list[tuple[str, dict[str, str]]] = []

    def xadd(self, key: str, fields: dict[str, str], maxlen=None, approximate=None) -> str:
        entry_id = f"{len(self.stream) + 1}-0"
        self.stream.append((entry_id, {"_stream_key": key, **fields}))
        return entry_id


def test_publish_service_stopped_disconnects_dual_slot() -> None:
    redis = _FakeRedis()
    publish_service_stopped_disconnects(
        redis,
        mc_service="ib_account_agent",
        slot_client_ids=[("host", 60), ("secondary", 61)],
    )
    assert len(redis.stream) == 2
    slots = {p["slot"] for _, p in redis.stream}
    assert slots == {"host", "secondary"}
    for _eid, payload in redis.stream:
        assert payload["status_to"] == "disconnected"
        assert payload["reason"] == "Service stopped"


def test_heartbeat_reconnect_slot_timeout_does_not_raise() -> None:
    client = MagicMock()

    async def slow_connect(**_kwargs: object) -> None:
        await asyncio.sleep(2.0)

    client.ensure_connected = slow_connect
    asyncio.run(
        heartbeat_reconnect_slot(
            client,
            slot_label="Host",
            client_id=1,
            connect_timeout_sec=0.05,
        )
    )
