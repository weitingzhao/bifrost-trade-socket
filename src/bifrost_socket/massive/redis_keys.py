"""Redis keys for Massive (Polygon) WS Ingestor."""

MASSIVE_HEALTH_KEY = "bifrost:health:ws_massive_option"
MASSIVE_KEY_PREFIX = "massive:"
MASSIVE_META_SUBS = "massive:meta:subscriptions"
MASSIVE_STREAM_KEY = "massive:stream"
MASSIVE_STREAM_MAXLEN = 10000
MASSIVE_KEY_TTL_SEC = 300

# Options WS can be quiet for extended periods (delayed tier, between AM bars).
# Polygon delayed endpoint does not reliably answer client WebSocket pings — keep
# ping disabled (same as auth probe) and use recv idle timeout for stale detection.
HEARTBEAT_TIMEOUT_SEC = 120
HEALTH_HEARTBEAT_INTERVAL_SEC = 30
WATCHLIST_POLL_SEC = 60
PG_SAMPLE_INTERVAL_SEC = 60
