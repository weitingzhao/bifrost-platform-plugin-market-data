"""Async token-bucket rate limiter for Polygon plan tiers."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class TierProfile:
    """Named rate profile for a Polygon plan tier."""

    name: str
    rate: float  # tokens replenished per second
    capacity: int  # max burst

    def make_bucket(self) -> TokenBucket:
        return TokenBucket(rate=self.rate, capacity=self.capacity)


# Basic (free): hard 5 requests / minute.
TIER_BASIC = TierProfile(name="basic", rate=5.0 / 60.0, capacity=5)

# Starter (paid — the Owner's Stocks Starter / Options Starter): unlimited API
# calls. A soft per-process ceiling keeps one worker from monopolising the key;
# 15 back-to-back requests measured 0.9s with no 429 (2026-09-06).
TIER_STARTER = TierProfile(name="starter", rate=8.0, capacity=16)

# Developer: effectively unlimited for practical ingest; soft throttle 100 req/s burst
TIER_DEVELOPER = TierProfile(name="developer", rate=100.0, capacity=100)

TIER_PROFILES: dict[str, TierProfile] = {
    "basic": TIER_BASIC,
    "starter": TIER_STARTER,
    "developer": TIER_DEVELOPER,
}


def get_tier_profile(tier: str) -> TierProfile:
    key = (tier or "starter").strip().lower()
    if key not in TIER_PROFILES:
        key = "starter"
    return TIER_PROFILES[key]


def bucket_from_config(polygon_cfg: Mapping[str, Any] | None) -> TokenBucket:
    """Limiter for the ``polygon`` config block.

    ``rate_per_sec`` / ``burst`` override the tier profile so the ceiling can be
    tuned without a code change; otherwise the tier's own numbers apply.
    """
    cfg = dict(polygon_cfg or {})
    profile = get_tier_profile(str(cfg.get("tier") or "starter"))
    rate = profile.rate
    capacity = profile.capacity
    raw_rate = cfg.get("rate_per_sec")
    if raw_rate is not None:
        try:
            rate = float(raw_rate)
        except (TypeError, ValueError):
            rate = profile.rate
    raw_burst = cfg.get("burst")
    if raw_burst is not None:
        try:
            capacity = int(raw_burst)
        except (TypeError, ValueError):
            capacity = profile.capacity
    if rate <= 0:
        rate = profile.rate
    if capacity < 1:
        capacity = max(1, profile.capacity)
    return TokenBucket(rate=rate, capacity=capacity)


class TokenBucket:
    """Async token bucket. ``acquire()`` waits until tokens are available."""

    def __init__(self, rate: float, capacity: int) -> None:
        if rate <= 0:
            raise ValueError("rate must be > 0")
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self.rate = float(rate)
        self.capacity = int(capacity)
        self._tokens = float(capacity)
        self._updated_at = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self, now: float) -> None:
        elapsed = max(0.0, now - self._updated_at)
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._updated_at = now

    async def acquire(self, tokens: float = 1.0) -> float:
        """Wait until ``tokens`` are available. Returns seconds waited."""
        if tokens <= 0:
            return 0.0
        waited = 0.0
        while True:
            async with self._lock:
                now = time.monotonic()
                self._refill(now)
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return waited
                deficit = tokens - self._tokens
                sleep_for = deficit / self.rate
            await asyncio.sleep(sleep_for)
            waited += sleep_for

    @property
    def tokens(self) -> float:
        """Best-effort current token count (not locked; for tests/diagnostics)."""
        now = time.monotonic()
        elapsed = max(0.0, now - self._updated_at)
        return min(self.capacity, self._tokens + elapsed * self.rate)
