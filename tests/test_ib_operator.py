"""IB Operator protocol, executor, and Redis client (mocked)."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bifrost_core.ib_operator.client import IbOperatorClient, build_monitor_ib_status, read_operator_health
from bifrost_core.ib_operator.health_redis import (
    LEGACY_OPERATOR_HEALTH_HASH_FIELDS,
    operator_health_dict_from_redis_hash,
    operator_health_dict_to_redis_hash,
    prune_legacy_operator_health_hash_fields,
)
from bifrost_socket.ib.operator.config import effective_ib_operator_settings
from bifrost_socket.ib.operator.protocol import PROTOCOL_VERSION, parse_stream_fields
from bifrost_socket.ib.operator.redis_io import (
    ensure_stream_and_group,
    is_nogroup_error,
    parse_xreadgroup_reply,
)


def _executor(primary=None, account_secondary=None):
    from bifrost_socket.ib.operator.executor import IbOperatorExecutor

    return IbOperatorExecutor(primary=primary or MagicMock(), account_secondary=account_secondary)


def test_parse_stream_fields_ok() -> None:
    fields = {
        "req_id": "u1",
        "v": PROTOCOL_VERSION,
        "op": "ping",
        "payload": "{}",
        "caller": "test",
        "deadline_ms": "9999999999999",
    }
    msg, err = parse_stream_fields(fields, stream_id="1-0")
    assert err is None
    assert msg is not None
    assert msg.op == "ping"
    assert msg.req_id == "u1"


def test_parse_stream_fields_unknown_op() -> None:
    fields = {
        "req_id": "u1",
        "v": PROTOCOL_VERSION,
        "op": "nope",
        "payload": "{}",
    }
    msg, err = parse_stream_fields(fields, stream_id="1-0")
    assert msg is None
    assert err and "unknown_op" in err


@pytest.mark.asyncio
async def test_executor_ping() -> None:
    primary = MagicMock()
    primary.connected = False
    out = await _executor(primary=primary).execute("ping", {})
    assert out["ok"] is True
    assert "data" in out


@pytest.mark.asyncio
async def test_executor_fetch_bars_delegates() -> None:
    primary = MagicMock()
    primary.fetch_bars = AsyncMock(return_value=[{"open": 1.0}])
    out = await _executor(primary=primary).execute(
        "fetch_bars",
        {"symbol": "AAPL", "period": "1 D", "duration": "5 D"},
    )
    assert out["ok"] is True
    assert len(out["data"]["bars"]) == 1
    primary.fetch_bars.assert_awaited_once()


def test_effective_ib_operator_settings_redis_key_defaults() -> None:
    cfg = {"redis": {"enabled": True}}
    s = effective_ib_operator_settings(cfg)
    assert s["stream"] == "ib:operator:cmd"
    assert s["result_prefix"] == "ib:operator:result:"
    assert s["health_key"] == "bifrost:health:ws_ib_operator"


def test_operator_health_hash_roundtrip_no_secondary() -> None:
    h = {
        "host": {"connected": True, "client_id": 120, "last_error": None},
        "service_alive": True,
        "secondary": None,
        "updated_at": 1_700_000_000.0,
    }
    m = operator_health_dict_to_redis_hash(h)
    assert m["host_connected"] == "1"
    assert m["host_client_id"] == "120"
    h2 = operator_health_dict_from_redis_hash(m)
    assert h2 is not None
    assert h2["host"]["connected"] is True
    assert h2["secondary"] is None


def test_prune_legacy_operator_health_hash_fields() -> None:
    r = MagicMock()
    prune_legacy_operator_health_hash_fields(r, "k")
    r.hdel.assert_called_once_with("k", *LEGACY_OPERATOR_HEALTH_HASH_FIELDS)


def test_parse_xreadgroup_reply_empty() -> None:
    assert parse_xreadgroup_reply(None) == []
    assert parse_xreadgroup_reply([]) == []


def test_ib_operator_client_request_polls_result() -> None:
    class FakeRedis:
        def xadd(self, stream, fields):
            return "1-0"

        def get(self, key):
            if key.endswith("abc"):
                return json.dumps({"ok": True, "data": {"x": 1}})
            return None

    with patch("bifrost_core.ib_operator.client.redis.from_url", return_value=FakeRedis()):
        c = IbOperatorClient(
            redis_url="redis://localhost:6379/0",
            stream="s",
            result_prefix="p:",
            default_timeout_sec=1.0,
        )
        with patch("bifrost_core.ib_operator.client.new_req_id", return_value="abc"):
            out = c.request("ping", {}, caller="t")
    assert out == {"ok": True, "data": {"x": 1}}


def test_is_nogroup_error() -> None:
    from redis.exceptions import ResponseError

    assert is_nogroup_error(ResponseError("NOGROUP ..."))
    assert not is_nogroup_error(ResponseError("WRONGTYPE ..."))


def test_ensure_stream_and_group_busygroup_ignored() -> None:
    r = MagicMock()
    r.xgroup_create.side_effect = Exception("BUSYGROUP Consumer Group name already exists")
    ensure_stream_and_group(r, "stream", "grp")


def test_build_monitor_ib_status_disabled_skip() -> None:
    cfg = {"server": {"skip_monitor_ib": True}, "redis": {"enabled": True}}
    assert build_monitor_ib_status(cfg, {}) is None


def test_operator_health_writer_merges_without_delete() -> None:
    from bifrost_socket.ib.operator.health_writer import IbOperatorHealthWriter

    primary = MagicMock()
    primary.connected_snapshot = MagicMock(return_value=False)
    ex = _executor(primary=primary)
    r = MagicMock()
    writer = IbOperatorHealthWriter(r)
    writer.write(ex.health_dict())
    r.delete.assert_not_called()


def test_operator_probe_writes_health_without_asyncio() -> None:
    import time

    from bifrost_socket.ib.connection_lifecycle import ServiceHeartbeatClock
    from bifrost_socket.ib.operator.service import _OperatorProbeThread, _ReconnectHint

    primary = MagicMock()
    primary._connected_state = True
    primary.client_id = 20
    primary.last_error = None
    primary.reconnects = 0
    ex = _executor(primary=primary)
    r = MagicMock()
    from bifrost_socket.ib.operator.health_writer import IbOperatorHealthWriter

    writer = IbOperatorHealthWriter(r)
    writes: list = []
    writer.write = writes.append  # type: ignore[method-assign]
    probe = _OperatorProbeThread(
        stop=__import__("threading").Event(),
        executor=ex,
        writer=writer,
        probe_interval_sec=5.0,
        tracker=None,
        hb_clock=ServiceHeartbeatClock(30.0, last_at=time.time()),
        reconnect_hint=_ReconnectHint(),
    )
    probe.write_health_now()
    assert len(writes) == 1
    assert writes[0]["host"]["ib_probe_ok"] is True
    assert writes[0]["host"]["ib_probe_at"] > 0


def test_read_operator_health_hash() -> None:
    class FakeRedisHash:
        def type(self, _key: str) -> str:
            return "hash"

        def hgetall(self, _key: str) -> dict:
            return operator_health_dict_to_redis_hash(
                {
                    "host": {"connected": True, "client_id": 5, "last_error": None},
                    "service_alive": True,
                    "secondary": None,
                    "updated_at": 0.0,
                }
            )

        def get(self, _key: str) -> None:
            raise AssertionError("should not GET hash key")

        def close(self) -> None:
            pass

    with patch("bifrost_core.ib_operator.client.redis.from_url", return_value=FakeRedisHash()):
        h = read_operator_health("redis://x", "k")
    assert h is not None
    assert h["host"]["client_id"] == 5
