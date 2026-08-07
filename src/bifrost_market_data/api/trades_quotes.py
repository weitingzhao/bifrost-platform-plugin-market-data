"""Options trades and quotes — Polygon pass-through."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query

from bifrost_market_data.api.deps import get_polygon_client, polygon_error_to_http
from bifrost_market_data.polygon.client import PolygonClient
from bifrost_market_data.polygon.errors import PolygonAPIError

router = APIRouter(prefix="/trades-quotes", tags=["trades-quotes"])


@router.get("/last-trade/{options_ticker:path}")
async def last_trade(
    options_ticker: str,
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    """GET /v2/last/trade/{optionsTicker}."""
    try:
        return await client.fetch_last_trade(options_ticker)
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/quotes/{options_ticker:path}")
async def option_quotes(
    options_ticker: str,
    timestamp_gte: str | None = Query(None, description="Nanosecond timestamp lower bound"),
    timestamp_lte: str | None = Query(None, description="Nanosecond timestamp upper bound"),
    limit: int = Query(100, ge=1, le=50000),
    sort: str = Query("timestamp"),
    order: str = Query("asc"),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    """GET /v3/quotes/{optionsTicker}."""
    try:
        return await client.fetch_option_quotes(
            options_ticker,
            timestamp_gte=timestamp_gte,
            timestamp_lte=timestamp_lte,
            limit=limit,
            sort=sort,
            order=order,
        )
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/trades/{options_ticker:path}")
async def option_trades(
    options_ticker: str,
    timestamp_gte: str | None = Query(None, description="Nanosecond timestamp lower bound"),
    timestamp_lte: str | None = Query(None, description="Nanosecond timestamp upper bound"),
    limit: int = Query(100, ge=1, le=50000),
    sort: str = Query("timestamp"),
    order: str = Query("asc"),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    """GET /v3/trades/{optionsTicker}."""
    try:
        return await client.fetch_option_trades(
            options_ticker,
            timestamp_gte=timestamp_gte,
            timestamp_lte=timestamp_lte,
            limit=limit,
            sort=sort,
            order=order,
        )
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc
