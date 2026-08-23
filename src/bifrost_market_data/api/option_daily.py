"""Option daily bars endpoint — reads from market.option_daily.

Provides the same data as bifrost-trade-api's greeks.py but via Plugin API.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Query

from bifrost_market_data.api.deps import (
    normalize_symbol,
    require_db,
    table_exists,
)

router = APIRouter(prefix="/options", tags=["options-daily"])


def _iso_date(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, datetime):
        return v.date().isoformat()
    s = str(v).strip()[:10]
    return s if s else None


def query_option_daily(
    conn: Any,
    *,
    symbol: str,
    expiry: str | None = None,
    days: int = 30,
    limit: int = 2000,
) -> dict[str, Any]:
    """Return option_daily rows for the given symbol within the lookback window."""
    sym = normalize_symbol(symbol)
    if not sym:
        return {"ok": False, "error": "symbol is required"}
    if not table_exists(conn, "market", "option_daily"):
        return {"ok": True, "symbol": sym, "rows": [], "count": 0}

    clauses = ["UPPER(TRIM(underlying)) = %s", "bar_date >= (CURRENT_DATE - %s)"]
    params: list[Any] = [sym, days]

    if expiry:
        clauses.append("expiry = %s")
        params.append(expiry)

    params.append(min(limit, 5000))

    where = " AND ".join(clauses)
    sql = f"""
        SELECT
            option_ticker, underlying, expiry, strike, option_right,
            bar_date, open, high, low, close, volume
        FROM raw_market.option_daily
        WHERE {where}
        ORDER BY bar_date DESC, expiry ASC, strike ASC, option_right ASC
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        raw = cur.fetchall() or []

    cols = (
        "option_ticker", "underlying", "expiry", "strike", "option_right",
        "bar_date", "open", "high", "low", "close", "volume",
    )
    rows: list[dict[str, Any]] = []
    for r in raw:
        if hasattr(r, "keys"):
            d = dict(r)
        else:
            d = {cols[i]: r[i] for i in range(min(len(cols), len(r)))}
        d["expiry"] = _iso_date(d.get("expiry"))
        d["bar_date"] = _iso_date(d.get("bar_date"))
        rows.append(d)

    return {"ok": True, "symbol": sym, "rows": rows, "count": len(rows)}


def query_option_daily_available_dates(
    conn: Any,
    *,
    symbol: str,
    limit: int = 90,
) -> dict[str, Any]:
    """Return distinct trade dates for the given symbol in option_daily."""
    sym = normalize_symbol(symbol)
    if not sym:
        return {"ok": False, "error": "symbol is required"}
    if not table_exists(conn, "market", "option_daily"):
        return {"ok": True, "symbol": sym, "dates": []}

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT bar_date
            FROM raw_market.option_daily
            WHERE UPPER(TRIM(underlying)) = %s
            ORDER BY bar_date DESC
            LIMIT %s
            """,
            (sym, limit),
        )
        dates = [_iso_date(r[0]) for r in (cur.fetchall() or []) if r[0] is not None]

    return {"ok": True, "symbol": sym, "dates": dates}


@router.get("/daily")
def options_daily(
    symbol: str = Query(..., description="Underlying symbol (e.g. NVDA)"),
    expiry: str | None = Query(None, description="Filter by expiry YYYY-MM-DD"),
    days: int = Query(30, ge=1, le=365, description="Lookback days from today"),
    limit: int = Query(2000, ge=1, le=5000),
) -> dict[str, Any]:
    """Option daily OHLCV bars from market.option_daily."""
    conn = require_db()
    try:
        return query_option_daily(conn, symbol=symbol, expiry=expiry, days=days, limit=limit)
    finally:
        conn.close()


@router.get("/daily/available-dates")
def options_daily_available_dates(
    symbol: str = Query(..., description="Underlying symbol (e.g. NVDA)"),
    limit: int = Query(90, ge=1, le=365),
) -> dict[str, Any]:
    """Distinct bar_dates in option_daily for a symbol."""
    conn = require_db()
    try:
        return query_option_daily_available_dates(conn, symbol=symbol, limit=limit)
    finally:
        conn.close()
