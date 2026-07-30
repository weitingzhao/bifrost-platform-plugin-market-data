"""TokenBucket unit tests."""

from __future__ import annotations

import asyncio
import time

import pytest

from bifrost_market_data.polygon.rate_limit import (
    TIER_DEVELOPER,
    TIER_STARTER,
    TokenBucket,
    get_tier_profile,
)


def test_tier_profiles() -> None:
    assert get_tier_profile("starter").name == "starter"
    assert get_tier_profile("developer").name == "developer"
    assert get_tier_profile("unknown").name == "starter"
    assert TIER_STARTER.rate == pytest.approx(5.0 / 60.0)
    assert TIER_DEVELOPER.capacity == 100


@pytest.mark.asyncio
async def test_token_bucket_waits_when_empty() -> None:
    bucket = TokenBucket(rate=20.0, capacity=1)  # 20 tokens/sec after burst of 1
    waited1 = await bucket.acquire()
    assert waited1 == pytest.approx(0.0, abs=0.05)
    t0 = time.monotonic()
    waited2 = await bucket.acquire()
    elapsed = time.monotonic() - t0
    assert waited2 >= 0.04
    assert elapsed >= 0.04


@pytest.mark.asyncio
async def test_starter_profile_shape() -> None:
    """Starter is 5/min; assert profile math without sleeping a full minute."""
    assert TIER_STARTER.capacity == 5
    assert TIER_STARTER.rate == pytest.approx(5.0 / 60.0)
    # Scaled stand-in: same capacity ratio, faster refill for CI
    fast = TokenBucket(rate=5.0, capacity=5)  # 5/sec
    for _ in range(5):
        await fast.acquire()
    t0 = time.monotonic()
    await fast.acquire()
    assert time.monotonic() - t0 >= 0.15


@pytest.mark.asyncio
async def test_concurrent_acquire_serializes() -> None:
    bucket = TokenBucket(rate=50.0, capacity=2)

    async def one() -> float:
        return await bucket.acquire()

    waits = await asyncio.gather(*(one() for _ in range(4)))
    assert sum(1 for w in waits if w == 0.0) <= 2
