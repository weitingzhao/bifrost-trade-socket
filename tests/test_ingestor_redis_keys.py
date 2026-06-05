"""IB ingestor Redis key constants."""

from bifrost_socket.ib.ingestor.redis_keys import (
    IB_INGESTER_CHANNEL,
    IB_INGESTER_HEALTH_KEY,
    IB_INGESTER_ON_DEMAND_STK,
    IB_INGESTER_SUBSCRIPTIONS_KEY,
    IB_INGESTER_TICK_PREFIX,
    IB_INGESTER_TICK_TTL_SEC,
)


def test_ingestor_health_key() -> None:
    assert IB_INGESTER_HEALTH_KEY == "bifrost:health:ws_ib_ingestor"


def test_ingestor_tick_prefix_and_ttl() -> None:
    assert IB_INGESTER_TICK_PREFIX == "ib:ingester:tick:"
    assert IB_INGESTER_TICK_TTL_SEC == 300


def test_ingestor_meta_keys() -> None:
    assert IB_INGESTER_SUBSCRIPTIONS_KEY == "ib:ingester:meta:subscriptions"
    assert IB_INGESTER_CHANNEL == "ib:ingester:channel"
    assert IB_INGESTER_ON_DEMAND_STK == "ib:ingester:control:on_demand_stk"
