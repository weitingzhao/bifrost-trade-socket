"""W0 trade-k8s-native: IB mock gateway + ib.mode resolution."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from bifrost_socket import config as socket_config
from bifrost_socket.config import IB_MODE_LIVE, IB_MODE_MOCK, get_ib_mode


def test_get_ib_mode_defaults_to_live() -> None:
    assert get_ib_mode({}) == IB_MODE_LIVE
    assert get_ib_mode({"ib": {"host": {"ip": "1.2.3.4"}}}) == IB_MODE_LIVE


def test_get_ib_mode_reads_ib_mode_mock() -> None:
    assert get_ib_mode({"ib": {"mode": "mock"}}) == IB_MODE_MOCK
    assert get_ib_mode({"ib": {"mode": "MOCK"}}) == IB_MODE_MOCK
    assert get_ib_mode({"ib": {"mode": "live"}}) == IB_MODE_LIVE


def test_get_ib_mode_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BIFROST_IB_MODE", "mock")
    assert get_ib_mode({"ib": {"mode": "live"}}) == IB_MODE_MOCK
    monkeypatch.setenv("BIFROST_IB_MODE", "live")
    assert get_ib_mode({"ib": {"mode": "mock"}}) == IB_MODE_LIVE


@pytest.mark.parametrize("role", ["ib_ingestor", "ib_account_agent", "ib_operator"])
def test_mock_gateway_writes_mode_mock_without_tws(
    role: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    rds = MagicMock()
    rds.smembers.return_value = set()
    monkeypatch.setattr(socket_config, "make_redis_client", lambda cfg: rds)
    # Patch the symbol imported into mock_gateway module namespace too.
    from bifrost_socket.ib import mock_gateway as mg

    monkeypatch.setattr(mg, "make_redis_client", lambda cfg: rds)

    cfg = {"_config_file": "/x/config.dev.yaml", "redis": {"host": "127.0.0.1"}}
    gw = mg.MockIbGateway(cfg, role)
    gw._write_health()

    # Every role must mark the slot as mock and never own a real client_id.
    health_key = mg.MockIbGateway.HEALTH_KEYS[role]
    mode_calls = [
        c
        for c in rds.hset.call_args_list
        if c.kwargs.get("mapping", {}).get("mode") == "mock"
    ]
    assert mode_calls, f"role {role} must write mode=mock to {health_key}"
    assert mode_calls[-1].kwargs["mapping"]["client_id"] == 0
