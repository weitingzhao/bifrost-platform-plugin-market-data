"""Technical indicators — Polygon pass-through."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from bifrost_market_data.api.deps import get_polygon_client, polygon_error_to_http
from bifrost_market_data.polygon.client import PolygonClient
from bifrost_market_data.polygon.errors import PolygonAPIError

router = APIRouter(prefix="/technical-indicators", tags=["technical"])

_ALLOWED = frozenset({"sma", "ema", "rsi", "macd"})


@router.get("/{indicator}/{ticker:path}")
async def technical_indicator(
    indicator: str,
    ticker: str,
    timespan: str = Query("day"),
    window: int = Query(14, ge=1, le=500),
    series_type: str = Query("close"),
    adjusted: bool = Query(True),
    order: str = Query("desc"),
    limit: int = Query(50, ge=1, le=5000),
    short_window: int | None = Query(None, ge=1, description="MACD only"),
    long_window: int | None = Query(None, ge=1, description="MACD only"),
    signal_window: int | None = Query(None, ge=1, description="MACD only"),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    """GET /v1/indicators/{sma|ema|macd|rsi}/{ticker}."""
    ind = indicator.strip().lower()
    if ind not in _ALLOWED:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown indicator '{indicator}'. Allowed: {', '.join(sorted(_ALLOWED))}",
        )
    try:
        return await client.fetch_indicator(
            ind,
            ticker,
            timespan=timespan,
            window=window,
            series_type=series_type,
            adjusted=adjusted,
            order=order,
            limit=limit,
            short_window=short_window,
            long_window=long_window,
            signal_window=signal_window,
        )
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc
