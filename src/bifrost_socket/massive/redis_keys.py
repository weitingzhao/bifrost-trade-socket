"""Redis keys for Massive (Polygon) WS Ingestor."""

MASSIVE_HEALTH_KEY = "bifrost:health:ws_massive_option"
MASSIVE_KEY_PREFIX = "massive:"
MASSIVE_META_SUBS = "massive:meta:subscriptions"
MASSIVE_STREAM_KEY = "massive:stream"
MASSIVE_STREAM_MAXLEN = 10000
MASSIVE_KEY_TTL_SEC = 300

# Options WS can be quiet for extended periods (delayed tier, between AM bars);
# websockets ping_interval=20/ping_timeout=10 still detect dead connections.
HEARTBEAT_TIMEOUT_SEC = 120
HEALTH_HEARTBEAT_INTERVAL_SEC = 30
WATCHLIST_POLL_SEC = 60
PG_SAMPLE_INTERVAL_SEC = 60
