"""Subscription channel rules for Massive WS (tier entitlements)."""

from bifrost_socket.massive.subscription_manager import (
    channels_for_symbols,
    massive_ws_enabled,
)


def test_massive_ws_enabled_starter_default_off() -> None:
    assert massive_ws_enabled("starter", {}) is False
    assert massive_ws_enabled("starter", {"ws_enabled": False}) is False


def test_massive_ws_enabled_developer_default_on() -> None:
    assert massive_ws_enabled("developer", {}) is True


def test_massive_ws_enabled_explicit_override() -> None:
    assert massive_ws_enabled("starter", {"ws_enabled": True}) is True
    assert massive_ws_enabled("developer", {"ws_enabled": False}) is False


def test_channels_for_symbols_starter_quotes_only() -> None:
    ch = channels_for_symbols({"NVDA", "TSLA"}, "starter", trades_enabled=False)
    assert ch == "Q.O:NVDA*,Q.O:TSLA*"
    assert "AM.O:" not in ch
    assert "T.O:" not in ch


def test_channels_for_symbols_developer_includes_aggregates() -> None:
    ch = channels_for_symbols({"NVDA"}, "developer", trades_enabled=False)
    assert ch == "Q.O:NVDA*,AM.O:NVDA*"


def test_channels_for_symbols_developer_trades_when_enabled() -> None:
    ch = channels_for_symbols({"NVDA"}, "developer", trades_enabled=True)
    assert ch == "Q.O:NVDA*,AM.O:NVDA*,T.O:NVDA*"
