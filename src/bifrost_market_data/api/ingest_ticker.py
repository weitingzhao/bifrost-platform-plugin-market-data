"""Ticker reference write routes (W0-P2).

POST /market/reference/ticker/upsert         — single ticker UPSERT
POST /market/reference/ticker/upsert-batch   — batch ticker UPSERT
POST /market/reference/ticker/upsert-overview — overview fields merge (COALESCE/NULLIF)
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from bifrost_market_data.api.deps import require_db, require_write_token

router = APIRouter(prefix="/reference/ticker", tags=["reference-ingest"])

_MARKET_TICKER_COLS = (
    "symbol",
    "name",
    "market",
    "locale",
    "primary_exchange",
    "instrument_type",
    "active",
    "currency",
    "cik",
    "composite_figi",
    "sic_code",
    "sector",
    "industry",
    "market_cap",
    "list_date",
    "homepage_url",
    "total_employees",
    "description",
    "updated_at",
)

_UPSERT_UPDATE_PARTS = [
    "name = COALESCE(EXCLUDED.name, market.ticker.name)",
    "market = COALESCE(EXCLUDED.market, market.ticker.market)",
    "locale = COALESCE(EXCLUDED.locale, market.ticker.locale)",
    "primary_exchange = COALESCE(EXCLUDED.primary_exchange, market.ticker.primary_exchange)",
    "instrument_type = COALESCE(EXCLUDED.instrument_type, market.ticker.instrument_type)",
    "active = COALESCE(EXCLUDED.active, market.ticker.active)",
    "currency = COALESCE(EXCLUDED.currency, market.ticker.currency)",
    "cik = COALESCE(EXCLUDED.cik, market.ticker.cik)",
    "composite_figi = COALESCE(EXCLUDED.composite_figi, market.ticker.composite_figi)",
    "sic_code = COALESCE(EXCLUDED.sic_code, market.ticker.sic_code)",
    "sector = COALESCE(EXCLUDED.sector, market.ticker.sector)",
    "industry = COALESCE(EXCLUDED.industry, market.ticker.industry)",
    "market_cap = COALESCE(EXCLUDED.market_cap, market.ticker.market_cap)",
    "list_date = COALESCE(EXCLUDED.list_date, market.ticker.list_date)",
    "homepage_url = COALESCE(EXCLUDED.homepage_url, market.ticker.homepage_url)",
    "total_employees = COALESCE(EXCLUDED.total_employees, market.ticker.total_employees)",
    "description = COALESCE(EXCLUDED.description, market.ticker.description)",
    "updated_at = EXCLUDED.updated_at",
]

_UPSERT_SQL = (
    f"INSERT INTO market.ticker ({', '.join(_MARKET_TICKER_COLS)}) "
    f"VALUES ({', '.join(['%s'] * len(_MARKET_TICKER_COLS))}) "
    f"ON CONFLICT (symbol) DO UPDATE SET {', '.join(_UPSERT_UPDATE_PARTS)}"
)


# -- Pydantic models -------------------------------------------------------


class TickerUpsertBody(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=20)
    name: str | None = None
    market: str | None = None
    locale: str | None = None
    primary_exchange: str | None = None
    type: str | None = Field(None, description="Polygon 'type' → instrument_type")
    instrument_type: str | None = None
    active: bool | None = None
    currency: str | None = None
    currency_name: str | None = None
    cik: str | None = None
    composite_figi: str | None = None
    sic_code: str | None = None
    sector: str | None = None
    industry: str | None = None
    market_cap: float | None = None
    list_date: date | str | None = None
    homepage_url: str | None = None
    total_employees: int | None = None
    description: str | None = None


class TickerBatchUpsertBody(BaseModel):
    tickers: list[TickerUpsertBody] = Field(..., min_length=1, max_length=5000)


class TickerOverviewBody(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=20)
    sector: str | None = None
    industry: str | None = None
    primary_exchange: str | None = None
    exchange: str | None = None
    list_date: date | str | None = None
    sic_code: str | None = None
    market_cap: float | None = None
    total_employees: int | None = None
    description: str | None = None
    homepage_url: str | None = None


# -- helpers ----------------------------------------------------------------


def _normalize_symbol(val: str) -> str:
    return val.strip().upper()


def _parse_list_date(val: Any) -> date | None:
    if val is None:
        return None
    if isinstance(val, date):
        return val
    s = str(val).strip()[:10]
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def _row_values(body: TickerUpsertBody) -> tuple[Any, ...]:
    """Build a tuple matching _MARKET_TICKER_COLS order."""
    sym = _normalize_symbol(body.symbol)
    itype = body.instrument_type or body.type
    currency = body.currency or body.currency_name
    now = datetime.now(timezone.utc)
    return (
        sym,
        body.name,
        body.market,
        body.locale,
        body.primary_exchange,
        itype,
        body.active,
        currency,
        body.cik,
        body.composite_figi,
        body.sic_code,
        body.sector,
        body.industry,
        body.market_cap,
        _parse_list_date(body.list_date),
        body.homepage_url,
        body.total_employees,
        body.description,
        now,
    )


def _upsert_single(conn: Any, body: TickerUpsertBody) -> str:
    """UPSERT one row, return 'inserted' or 'updated'."""
    sym = _normalize_symbol(body.symbol)
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM market.ticker WHERE symbol = %s", (sym,))
        existed = cur.fetchone() is not None
        cur.execute(_UPSERT_SQL, _row_values(body))
    conn.commit()
    return "updated" if existed else "inserted"


def _upsert_batch(conn: Any, tickers: list[TickerUpsertBody]) -> int:
    """UPSERT many rows in a single transaction."""
    rows = [_row_values(t) for t in tickers]
    with conn.cursor() as cur:
        cur.executemany(_UPSERT_SQL, rows)
    conn.commit()
    return len(rows)


def _upsert_overview(conn: Any, body: TickerOverviewBody) -> bool:
    """Merge overview fields with COALESCE(NULLIF(new, ''), existing) semantics.

    Returns True when at least one row was affected.
    """
    sym = _normalize_symbol(body.symbol)
    exchange = body.primary_exchange or body.exchange
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE market.ticker SET
              sector        = COALESCE(NULLIF(%s, ''), sector),
              industry      = COALESCE(NULLIF(%s, ''), industry),
              primary_exchange = COALESCE(%s, primary_exchange),
              list_date     = COALESCE(%s, list_date),
              sic_code      = COALESCE(%s, sic_code),
              market_cap    = COALESCE(%s, market_cap),
              total_employees = COALESCE(%s, total_employees),
              description   = COALESCE(%s, description),
              homepage_url  = COALESCE(%s, homepage_url),
              updated_at    = now()
            WHERE symbol = %s
            """,
            (
                body.sector or "",
                body.industry or "",
                exchange,
                _parse_list_date(body.list_date),
                body.sic_code,
                body.market_cap,
                body.total_employees,
                body.description,
                body.homepage_url,
                sym,
            ),
        )
        affected = cur.rowcount
    conn.commit()
    return affected > 0


# -- routes -----------------------------------------------------------------


@router.post("/upsert", dependencies=[Depends(require_write_token)])
def upsert_ticker(body: TickerUpsertBody) -> dict[str, Any]:
    """UPSERT a single ticker into ``market.ticker``."""
    sym = _normalize_symbol(body.symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required")
    conn = require_db()
    try:
        action = _upsert_single(conn, body)
        return {"ok": True, "symbol": sym, "action": action}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"upsert failed: {exc}") from exc
    finally:
        conn.close()


@router.post("/upsert-batch", dependencies=[Depends(require_write_token)])
def upsert_ticker_batch(body: TickerBatchUpsertBody) -> dict[str, Any]:
    """Batch UPSERT tickers into ``market.ticker``."""
    if not body.tickers:
        raise HTTPException(status_code=400, detail="tickers list is empty")
    conn = require_db()
    try:
        written = _upsert_batch(conn, body.tickers)
        return {"ok": True, "written": written}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"batch upsert failed: {exc}") from exc
    finally:
        conn.close()


@router.post("/upsert-overview", dependencies=[Depends(require_write_token)])
def upsert_ticker_overview(body: TickerOverviewBody) -> dict[str, Any]:
    """Merge overview fields into ``market.ticker`` (COALESCE/NULLIF semantics)."""
    sym = _normalize_symbol(body.symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required")
    conn = require_db()
    try:
        found = _upsert_overview(conn, body)
        if not found:
            raise HTTPException(
                status_code=404,
                detail=f"ticker {sym} not found in market.ticker",
            )
        return {"ok": True, "symbol": sym}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"overview upsert failed: {exc}") from exc
    finally:
        conn.close()
