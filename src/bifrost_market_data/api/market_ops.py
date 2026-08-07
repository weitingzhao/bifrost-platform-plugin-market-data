"""Market operations — conditions, exchanges, holidays, status."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query

from bifrost_market_data.api.deps import get_polygon_client, polygon_error_to_http
from bifrost_market_data.polygon.client import PolygonClient
from bifrost_market_data.polygon.errors import PolygonAPIError

router = APIRouter(prefix="/market-ops", tags=["market-ops"])


@router.get("/conditions")
async def market_conditions(
    asset_class: str | None = Query(None, description="options | stocks | crypto | fx"),
    data_type: str | None = Query(None, description="trade | bbo | nbbo"),
    limit: int = Query(1000, ge=1, le=1000),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    """GET /v3/reference/conditions."""
    try:
        return await client.fetch_market_conditions(
            asset_class=asset_class,
            data_type=data_type,
            limit=limit,
        )
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/exchanges")
async def market_exchanges(
    asset_class: str | None = Query(None, description="stocks | options | crypto | fx"),
    locale: str | None = Query(None, description="us | global"),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    """GET /v3/reference/exchanges."""
    try:
        return await client.fetch_market_exchanges(asset_class=asset_class, locale=locale)
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/holidays")
async def market_holidays(
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    """GET /v1/marketstatus/upcoming — upcoming market holidays."""
    try:
        return await client.fetch_market_status_upcoming()
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/status")
async def market_status(
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    """GET /v1/marketstatus/now — current market status."""
    try:
        return await client.fetch_market_status_now()
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc
