"""Redis keys for Massive (Polygon) WS Ingestor."""

MASSIVE_HEALTH_KEY = "bifrost:health:ws_massive_option"
MASSIVE_KEY_PREFIX = "massive:"
MASSIVE_META_SUBS = "massive:meta:subscriptions"
MASSIVE_STREAM_KEY = "massive:stream"
MASSIVE_STREAM_MAXLEN = 10000
MASSIVE_KEY_TTL_SEC = 300

# 30s: let websockets ping_interval=20/ping_timeout=10 handle detection;
# genuine 30s silence during market hours means something is wrong.
HEARTBEAT_TIMEOUT_SEC = 30
HEALTH_HEARTBEAT_INTERVAL_SEC = 30
WATCHLIST_POLL_SEC = 60
PG_SAMPLE_INTERVAL_SEC = 60
