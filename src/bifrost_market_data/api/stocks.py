"""Stock bars, reference, news — Polygon pass-through."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query

from bifrost_market_data.api.deps import get_polygon_client, polygon_error_to_http
from bifrost_market_data.polygon import endpoints as ep
from bifrost_market_data.polygon.client import PolygonClient
from bifrost_market_data.polygon.errors import PolygonAPIError

router = APIRouter(prefix="/stocks", tags=["stocks"])


@router.get("/bars/range")
async def stock_bars_range(
    ticker: str = Query(..., description="Stock symbol, e.g. AAPL"),
    multiplier: int = Query(1, ge=1),
    timespan: str = Query("day"),
    from_: str = Query(..., alias="from", description="Range start (YYYY-MM-DD or Unix ms)"),
    to: str = Query(..., description="Range end (YYYY-MM-DD or Unix ms)"),
    adjusted: bool | None = Query(None),
    sort: str = Query("asc"),
    limit: int = Query(50000, ge=1, le=50000),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    """GET /v2/aggs/ticker/{ticker}/range/... — custom OHLCV bars."""
    try:
        path = ep.aggs_range_path(
            ticker,
            multiplier=multiplier,
            timespan=timespan,
            from_value=from_,
            to_value=to,
        )
        params = ep.aggs_range_params(
            ticker=ticker,
            adjusted=adjusted,
            sort=sort,
            limit=limit,
        )
        return await client.get_json(path, params)
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/bars/grouped-daily/{date}")
async def stock_bars_grouped_daily(
    date: str,
    adjusted: bool = Query(True),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    """GET /v2/aggs/grouped/locale/us/market/stocks/{date}."""
    try:
        return await client.fetch_grouped_daily(date, adjusted=adjusted)
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/bars/open-close/{ticker:path}/{date}")
async def stock_bars_open_close(
    ticker: str,
    date: str,
    adjusted: bool = Query(True),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    """GET /v1/open-close/{ticker}/{date}."""
    try:
        return await client.fetch_open_close(ticker, date, adjusted=adjusted)
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/bars/prev/{ticker:path}")
async def stock_bars_prev(
    ticker: str,
    adjusted: bool = Query(True),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    """GET /v2/aggs/ticker/{ticker}/prev."""
    try:
        return await client.fetch_prev_agg(ticker, adjusted=adjusted)
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/news")
async def stock_news(
    ticker: str | None = Query(None),
    published_utc_gte: str | None = Query(None),
    published_utc_lte: str | None = Query(None),
    limit: int = Query(10, ge=1, le=1000),
    sort: str | None = Query(None),
    order: str | None = Query(None),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    """GET /v2/reference/news."""
    try:
        return await client.fetch_news(
            ticker=ticker,
            published_utc_gte=published_utc_gte,
            published_utc_lte=published_utc_lte,
            limit=limit,
            sort=sort,
            order=order,
        )
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/search")
async def stock_search(
    q: str = Query("", max_length=128),
    limit: int = Query(20, ge=1, le=100),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    """Ticker search via /v3/reference/tickers search param."""
    try:
        return await client.fetch_reference_tickers_query(search=q or None, limit=limit)
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/{symbol:path}/related")
async def stock_related(
    symbol: str,
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    """GET /v1/related-companies/{ticker}."""
    try:
        return await client.fetch_related_companies(symbol)
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/{symbol:path}")
async def stock_detail(
    symbol: str,
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    """GET /v3/reference/tickers/{ticker} — ticker details."""
    try:
        return await client.fetch_ticker_details(symbol)
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc
