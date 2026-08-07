"""Stock fundamentals — Polygon financials pass-through."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query

from bifrost_market_data.api.deps import get_polygon_client, polygon_error_to_http
from bifrost_market_data.polygon.client import PolygonClient
from bifrost_market_data.polygon.errors import PolygonAPIError

router = APIRouter(prefix="/stocks/fundamentals", tags=["fundamentals"])


def _statement_params(
    *,
    ticker: str,
    timeframe: str | None,
    fiscal_year: int | None,
    fiscal_quarter: int | None,
    period_end: str | None,
    filing_date: str | None,
    limit: int,
    sort: str | None,
) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "timeframe": timeframe,
        "fiscal_year": fiscal_year,
        "fiscal_quarter": fiscal_quarter,
        "period_end": period_end,
        "filing_date": filing_date,
        "limit": limit,
        "sort": sort,
    }


@router.get("/income-statements")
async def income_statements(
    ticker: str = Query(...),
    timeframe: str | None = Query(None),
    fiscal_year: int | None = Query(None),
    fiscal_quarter: int | None = Query(None, ge=1, le=4),
    period_end: str | None = Query(None),
    filing_date: str | None = Query(None),
    limit: int = Query(10, ge=1, le=1000),
    sort: str | None = Query(None),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    try:
        return await client.fetch_financial_statement(
            "income-statements",
            **_statement_params(
                ticker=ticker,
                timeframe=timeframe,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                period_end=period_end,
                filing_date=filing_date,
                limit=limit,
                sort=sort,
            ),
        )
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/balance-sheets")
async def balance_sheets(
    ticker: str = Query(...),
    timeframe: str | None = Query(None),
    fiscal_year: int | None = Query(None),
    fiscal_quarter: int | None = Query(None, ge=1, le=4),
    period_end: str | None = Query(None),
    filing_date: str | None = Query(None),
    limit: int = Query(10, ge=1, le=1000),
    sort: str | None = Query(None),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    try:
        return await client.fetch_financial_statement(
            "balance-sheets",
            **_statement_params(
                ticker=ticker,
                timeframe=timeframe,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                period_end=period_end,
                filing_date=filing_date,
                limit=limit,
                sort=sort,
            ),
        )
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/cash-flow-statements")
async def cash_flow_statements(
    ticker: str = Query(...),
    timeframe: str | None = Query(None),
    fiscal_year: int | None = Query(None),
    fiscal_quarter: int | None = Query(None, ge=1, le=4),
    period_end: str | None = Query(None),
    filing_date: str | None = Query(None),
    limit: int = Query(10, ge=1, le=1000),
    sort: str | None = Query(None),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    try:
        return await client.fetch_financial_statement(
            "cash-flow-statements",
            **_statement_params(
                ticker=ticker,
                timeframe=timeframe,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                period_end=period_end,
                filing_date=filing_date,
                limit=limit,
                sort=sort,
            ),
        )
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/ratios")
async def ratios(
    ticker: str = Query(...),
    limit: int = Query(10, ge=1, le=1000),
    sort: str | None = Query(None),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    try:
        return await client.fetch_ratios(ticker=ticker, limit=limit, sort=sort)
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/short-interest")
async def short_interest(
    ticker: str = Query(...),
    settlement_date: str | None = Query(None),
    limit: int = Query(10, ge=1, le=1000),
    sort: str | None = Query(None),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    try:
        return await client.fetch_short_interest(
            ticker=ticker,
            settlement_date=settlement_date,
            limit=limit,
            sort=sort,
        )
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/short-volume")
async def short_volume(
    ticker: str = Query(...),
    date: str | None = Query(None),
    limit: int = Query(10, ge=1, le=1000),
    sort: str | None = Query(None),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    try:
        return await client.fetch_short_volume(
            ticker=ticker,
            date=date,
            limit=limit,
            sort=sort,
        )
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/float")
async def float_shares(
    ticker: str = Query(...),
    limit: int = Query(10, ge=1, le=5000),
    sort: str | None = Query(None),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    try:
        return await client.fetch_float(ticker=ticker, limit=limit, sort=sort)
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc
