"""Option daily bars endpoint — reads from market.option_daily.

Provides the same data as bifrost-trade-api's greeks.py but via Plugin API.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from bifrost_market_data.api.deps import (
    normalize_symbol,
    reject_unknown_params,
    require_db,
    table_exists,
)

router = APIRouter(prefix="/options", tags=["options-daily"])

#: Parameters callers reach for on this route, and what they are called here.
DAILY_PARAM_ALIASES = {
    "expiration": "expiry",
    "trade_date": "from / to",
    "date": "from / to",
    "ticker": "option_ticker",
}


def _iso_date(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, datetime):
        return v.date().isoformat()
    s = str(v).strip()[:10]
    return s if s else None


def _norm_right(value: str | None) -> str | None:
    """``C`` / ``P`` from any of call/put/c/p; anything else is a mistake worth saying."""
    if value is None or not str(value).strip():
        return None
    s = str(value).strip().upper()
    if s in ("C", "CALL"):
        return "C"
    if s in ("P", "PUT"):
        return "P"
    raise HTTPException(status_code=422, detail=f"right must be C or P, got {value!r}")


def query_option_daily(
    conn: Any,
    *,
    symbol: str,
    expiry: str | None = None,
    days: int = 30,
    limit: int = 2000,
    option_ticker: str | None = None,
    strike: float | None = None,
    right: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
) -> dict[str, Any]:
    """Option daily rows for a symbol, over a window and optionally one contract.

    ``date_from`` / ``date_to`` bound the window when given; ``days`` is the
    lookback used when neither is. A single contract is either its
    ``option_ticker`` or its ``strike`` + ``right`` (with ``expiry``).
    """
    sym = normalize_symbol(symbol)
    if not sym:
        return {"ok": False, "error": "symbol is required"}
    if not table_exists(conn, "market", "option_daily"):
        return {"ok": True, "symbol": sym, "rows": [], "count": 0}

    clauses = ["UPPER(TRIM(underlying)) = %s"]
    params: list[Any] = [sym]

    window: dict[str, str | None]
    if date_from is not None or date_to is not None:
        if date_from is not None:
            clauses.append("bar_date >= %s")
            params.append(date_from)
        if date_to is not None:
            clauses.append("bar_date <= %s")
            params.append(date_to)
        window = {
            "from": _iso_date(date_from),
            "to": _iso_date(date_to),
            "basis": "explicit range",
        }
    else:
        clauses.append("bar_date >= (CURRENT_DATE - %s)")
        params.append(days)
        window = {"from": None, "to": None, "basis": f"last {days} days"}

    if expiry:
        clauses.append("expiry = %s")
        params.append(expiry)
    if option_ticker:
        clauses.append("UPPER(TRIM(option_ticker)) = %s")
        params.append(normalize_symbol(option_ticker))
    if strike is not None:
        # Strikes are numeric(…); compare with a tolerance rather than on equality
        # of a float the caller typed.
        clauses.append("abs(strike - %s) < 1e-4")
        params.append(float(strike))
    if right is not None:
        clauses.append("UPPER(TRIM(option_right)) = %s")
        params.append(right)

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

    return {
        "ok": True,
        "symbol": sym,
        "window": window,
        "contract": {
            "option_ticker": normalize_symbol(option_ticker) or None,
            "expiry": expiry,
            "strike": strike,
            "right": right,
        },
        "rows": rows,
        "count": len(rows),
    }


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
    request: Request,
    symbol: str = Query(..., description="Underlying symbol (e.g. NVDA)"),
    expiry: str | None = Query(None, description="Filter by expiry YYYY-MM-DD"),
    days: int = Query(30, ge=1, le=365, description="Lookback days when no from/to"),
    limit: int = Query(2000, ge=1, le=5000),
    option_ticker: str | None = Query(
        None, description="One contract, e.g. O:NVDA261120C00245000"
    ),
    strike: float | None = Query(None, description="One strike (with right, and expiry)"),
    right: str | None = Query(None, description="C or P"),
    date_from: date | None = Query(None, alias="from", description="First bar_date"),
    date_to: date | None = Query(None, alias="to", description="Last bar_date"),
) -> dict[str, Any]:
    """Option daily OHLCV bars from market.option_daily."""
    reject_unknown_params(request, DAILY_PARAM_ALIASES)
    side = _norm_right(right)
    conn = require_db()
    try:
        return query_option_daily(
            conn,
            symbol=symbol,
            expiry=expiry,
            days=days,
            limit=limit,
            option_ticker=option_ticker,
            strike=strike,
            right=side,
            date_from=date_from,
            date_to=date_to,
        )
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
