"""SEC filings — Polygon pass-through."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query

from bifrost_market_data.api.deps import get_polygon_client, polygon_error_to_http
from bifrost_market_data.polygon.client import PolygonClient
from bifrost_market_data.polygon.errors import PolygonAPIError

router = APIRouter(prefix="/stocks/filings", tags=["filings"])


@router.get("/edgar-index")
async def edgar_index(
    ticker: str | None = Query(None),
    cik: str | None = Query(None),
    form_type: str | None = Query(None),
    filing_date: str | None = Query(None),
    filing_date_gt: str | None = Query(None),
    filing_date_gte: str | None = Query(None),
    filing_date_lt: str | None = Query(None),
    filing_date_lte: str | None = Query(None),
    limit: int = Query(100, ge=1, le=50000),
    sort: str | None = Query(None),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    try:
        return await client.fetch_edgar_index(
            ticker=ticker,
            cik=cik,
            form_type=form_type,
            filing_date=filing_date,
            filing_date_gt=filing_date_gt,
            filing_date_gte=filing_date_gte,
            filing_date_lt=filing_date_lt,
            filing_date_lte=filing_date_lte,
            limit=limit,
            sort=sort,
        )
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/10k-sections")
async def filing_10k_sections(
    ticker: str | None = Query(None),
    cik: str | None = Query(None),
    section: str | None = Query(None),
    filing_date: str | None = Query(None),
    filing_date_gt: str | None = Query(None),
    filing_date_gte: str | None = Query(None),
    filing_date_lt: str | None = Query(None),
    filing_date_lte: str | None = Query(None),
    period_end: str | None = Query(None),
    period_end_gte: str | None = Query(None),
    period_end_lte: str | None = Query(None),
    limit: int = Query(10, ge=1, le=99),
    sort: str | None = Query(None),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    try:
        return await client.fetch_10k_sections(
            ticker=ticker,
            cik=cik,
            section=section,
            filing_date=filing_date,
            filing_date_gt=filing_date_gt,
            filing_date_gte=filing_date_gte,
            filing_date_lt=filing_date_lt,
            filing_date_lte=filing_date_lte,
            period_end=period_end,
            period_end_gte=period_end_gte,
            period_end_lte=period_end_lte,
            limit=limit,
            sort=sort,
        )
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/8k-text")
async def filing_8k_text(
    ticker: str | None = Query(None),
    cik: str | None = Query(None),
    form_type: str | None = Query(None),
    filing_date: str | None = Query(None),
    filing_date_gt: str | None = Query(None),
    filing_date_gte: str | None = Query(None),
    filing_date_lt: str | None = Query(None),
    filing_date_lte: str | None = Query(None),
    limit: int = Query(10, ge=1, le=99),
    sort: str | None = Query(None),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    try:
        return await client.fetch_8k_text(
            ticker=ticker,
            cik=cik,
            form_type=form_type,
            filing_date=filing_date,
            filing_date_gt=filing_date_gt,
            filing_date_gte=filing_date_gte,
            filing_date_lt=filing_date_lt,
            filing_date_lte=filing_date_lte,
            limit=limit,
            sort=sort,
        )
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/13f")
async def filing_13f(
    filer_cik: str | None = Query(None),
    filing_date: str | None = Query(None),
    filing_date_gt: str | None = Query(None),
    filing_date_gte: str | None = Query(None),
    filing_date_lt: str | None = Query(None),
    filing_date_lte: str | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    sort: str | None = Query(None),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    try:
        return await client.fetch_13f_filings(
            filer_cik=filer_cik,
            filing_date=filing_date,
            filing_date_gt=filing_date_gt,
            filing_date_gte=filing_date_gte,
            filing_date_lt=filing_date_lt,
            filing_date_lte=filing_date_lte,
            limit=limit,
            sort=sort,
        )
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/risk-factors")
async def filing_risk_factors(
    ticker: str | None = Query(None),
    cik: str | None = Query(None),
    filing_date: str | None = Query(None),
    filing_date_gt: str | None = Query(None),
    filing_date_gte: str | None = Query(None),
    filing_date_lt: str | None = Query(None),
    filing_date_lte: str | None = Query(None),
    limit: int = Query(100, ge=1, le=49999),
    sort: str | None = Query(None),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    try:
        return await client.fetch_risk_factors(
            ticker=ticker,
            cik=cik,
            filing_date=filing_date,
            filing_date_gt=filing_date_gt,
            filing_date_gte=filing_date_gte,
            filing_date_lt=filing_date_lt,
            filing_date_lte=filing_date_lte,
            limit=limit,
            sort=sort,
        )
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/risk-categories")
async def filing_risk_categories(
    taxonomy: int | None = Query(None),
    primary_category: str | None = Query(None),
    secondary_category: str | None = Query(None),
    tertiary_category: str | None = Query(None),
    limit: int = Query(200, ge=1, le=999),
    sort: str | None = Query(None),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    try:
        return await client.fetch_risk_categories(
            taxonomy=taxonomy,
            primary_category=primary_category,
            secondary_category=secondary_category,
            tertiary_category=tertiary_category,
            limit=limit,
            sort=sort,
        )
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/form-3")
async def filing_form_3(
    issuer_cik: str | None = Query(None),
    owner_cik: str | None = Query(None),
    tickers: str | None = Query(None),
    form_type: str | None = Query(None),
    filing_date: str | None = Query(None),
    filing_date_gt: str | None = Query(None),
    filing_date_gte: str | None = Query(None),
    filing_date_lt: str | None = Query(None),
    filing_date_lte: str | None = Query(None),
    limit: int = Query(100, ge=1, le=10000),
    sort: str | None = Query(None),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    try:
        return await client.fetch_form_3(
            issuer_cik=issuer_cik,
            owner_cik=owner_cik,
            tickers=tickers,
            form_type=form_type,
            filing_date=filing_date,
            filing_date_gt=filing_date_gt,
            filing_date_gte=filing_date_gte,
            filing_date_lt=filing_date_lt,
            filing_date_lte=filing_date_lte,
            limit=limit,
            sort=sort,
        )
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc


@router.get("/form-4")
async def filing_form_4(
    issuer_cik: str | None = Query(None),
    owner_cik: str | None = Query(None),
    tickers: str | None = Query(None),
    form_type: str | None = Query(None),
    transaction_code: str | None = Query(None),
    filing_date: str | None = Query(None),
    filing_date_gt: str | None = Query(None),
    filing_date_gte: str | None = Query(None),
    filing_date_lt: str | None = Query(None),
    filing_date_lte: str | None = Query(None),
    limit: int = Query(100, ge=1, le=10000),
    sort: str | None = Query(None),
    client: PolygonClient = Depends(get_polygon_client),
) -> dict[str, Any]:
    try:
        return await client.fetch_form_4(
            issuer_cik=issuer_cik,
            owner_cik=owner_cik,
            tickers=tickers,
            form_type=form_type,
            transaction_code=transaction_code,
            filing_date=filing_date,
            filing_date_gt=filing_date_gt,
            filing_date_gte=filing_date_gte,
            filing_date_lt=filing_date_lt,
            filing_date_lte=filing_date_lte,
            limit=limit,
            sort=sort,
        )
    except PolygonAPIError as exc:
        raise polygon_error_to_http(exc) from exc
