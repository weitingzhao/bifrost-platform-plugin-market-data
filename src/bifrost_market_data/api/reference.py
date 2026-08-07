"""Ticker reference — Polygon pass-through."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query

from bifrost_market_data.api.deps import get_polygon_client, polygon_error_to_http
from bifrost_market_data.polygon.client import PolygonClient
from bifrost_market_data.polygon.errors import PolygonAPIError

router = APIRouter(tags=["reference"])


@router.get("/tickers")
async def reference_tickers(
    ticker: str | None = Query(None),
    instrument_type: str | None = Query(None, alias="type", description="Instrument type filter"),
    market: str | None = Query(None),
    exchange: str | None = Query(None),
    search: str | None = Query(None),
    active: bool | None = Query(None),
    date: str | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    sort: str = Query("ticker"),
    order: str = Query("asc"),
    cursor: str | None = Query(None),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    """GET /v3/reference/tickers."""
    try:
        return await client.fetch_reference_tickers_query(
            ticker=ticker,
            instrument_type=instrument_type,
            market=market,
            exchange=exchange,
            search=search,
            active=active,
            date=date,
            limit=limit,
            sort=sort,
            order=order,
            cursor=cursor,
        )
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/tickers/types")
async def ticker_types(
    asset_class: str | None = Query(None),
    locale: str | None = Query(None),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    """GET /v3/reference/tickers/types."""
    try:
        return await client.fetch_ticker_types(asset_class=asset_class, locale=locale)
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/tickers/{ticker:path}")
async def ticker_detail(
    ticker: str,
    date: str | None = Query(None),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    """GET /v3/reference/tickers/{ticker}."""
    try:
        return await client.fetch_ticker_detail(ticker, date=date)
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/related-companies/{ticker:path}")
async def related_companies(
    ticker: str,
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    """GET /v1/related-companies/{ticker}."""
    try:
        return await client.fetch_related_companies(ticker)
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/instrument-types")
async def instrument_types(
    asset_class: str | None = Query(None),
    locale: str | None = Query(None),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    """Alias for GET /tickers/types."""
    try:
        return await client.fetch_ticker_types(asset_class=asset_class, locale=locale)
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc
