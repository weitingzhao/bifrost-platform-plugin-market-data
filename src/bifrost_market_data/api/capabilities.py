"""GET /market/capabilities — what the current subscriptions cover."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from bifrost_market_data.subscription import capability_matrix

router = APIRouter(tags=["capabilities"])


@router.get("/capabilities")
def market_capabilities() -> dict[str, Any]:
    """Entitled, planned-on-upgrade and unavailable capabilities, with the policy behind them."""
    return capability_matrix()
