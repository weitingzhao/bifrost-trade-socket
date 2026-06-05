"""IB connection policy defaults."""

from bifrost_core.config.connection_policy import (
    DEFAULT_RECONNECT_BASE_SEC,
    get_ib_connection_policy,
    reconnect_delay_s,
)


def test_reconnect_delay_exponential():
    assert reconnect_delay_s(1) == DEFAULT_RECONNECT_BASE_SEC
    assert reconnect_delay_s(3) > reconnect_delay_s(2)


def test_get_ib_connection_policy_defaults():
    p = get_ib_connection_policy({})
    assert p["reconnect_base_sec"] >= 0.5
    assert p["ib_probe_interval_sec"] >= 1.0
