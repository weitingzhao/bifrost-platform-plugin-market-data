"""Async token-bucket rate limiter for Polygon plan tiers."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class TierProfile:
    """Named rate profile for a Polygon plan tier."""

    name: str
    rate: float  # tokens replenished per second
    capacity: int  # max burst

    def make_bucket(self) -> TokenBucket:
        return TokenBucket(rate=self.rate, capacity=self.capacity)


# Starter: hard 5 requests / minute
TIER_STARTER = TierProfile(name="starter", rate=5.0 / 60.0, capacity=5)

# Developer: effectively unlimited for practical ingest; soft throttle 100 req/s burst
TIER_DEVELOPER = TierProfile(name="developer", rate=100.0, capacity=100)

TIER_PROFILES: dict[str, TierProfile] = {
    "starter": TIER_STARTER,
    "developer": TIER_DEVELOPER,
}


def get_tier_profile(tier: str) -> TierProfile:
    key = (tier or "starter").strip().lower()
    if key not in TIER_PROFILES:
        key = "starter"
    return TIER_PROFILES[key]


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
