"""Polygon.io REST client and rate limiting (P2)."""

from bifrost_market_data.polygon.client import PolygonClient
from bifrost_market_data.polygon.errors import PolygonAPIError, PolygonRateLimitError
from bifrost_market_data.polygon.rate_limit import (
    TIER_DEVELOPER,
    TIER_PROFILES,
    TIER_STARTER,
    TokenBucket,
    get_tier_profile,
)

__all__ = [
    "PolygonAPIError",
    "PolygonClient",
    "PolygonRateLimitError",
    "TIER_DEVELOPER",
    "TIER_PROFILES",
    "TIER_STARTER",
    "TokenBucket",
    "get_tier_profile",
]
