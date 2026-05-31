"""Redis key names for IB ingestor."""

IB_INGESTER_HEALTH_KEY = "bifrost:health:ws_ib_ingestor"
IB_INGESTER_SUBSCRIPTIONS_KEY = "ib:ingester:meta:subscriptions"
IB_INGESTER_CHANNEL = "ib:ingester:channel"
IB_INGESTER_TICK_PREFIX = "ib:ingester:tick:"
IB_INGESTER_TICK_TTL_SEC = 300
IB_INGESTER_ON_DEMAND_STK = "ib:ingester:control:on_demand_stk"
