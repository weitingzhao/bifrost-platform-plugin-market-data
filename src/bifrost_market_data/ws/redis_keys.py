"""Redis keys for Polygon Options WS Ingestor.

Key format is fully compatible with the legacy massive-ws service so that
Trade consumers can switch to redis-massive without code changes.
"""

MASSIVE_HEALTH_KEY = "bifrost:health:ws_massive_option"
MASSIVE_KEY_PREFIX = "massive:"
MASSIVE_META_SUBS = "massive:meta:subscriptions"
MASSIVE_STREAM_KEY = "massive:stream"
MASSIVE_STREAM_MAXLEN = 10_000
MASSIVE_KEY_TTL_SEC = 300

HEARTBEAT_TIMEOUT_SEC = 120
HEALTH_HEARTBEAT_INTERVAL_SEC = 30
WATCHLIST_POLL_SEC = 60
