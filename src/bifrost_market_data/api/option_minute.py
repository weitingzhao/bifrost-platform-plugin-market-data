"""Option minute bars endpoint — reads from market.option_minute."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Query

from bifrost_market_data.api.deps import (
    normalize_symbol,
    require_db,
    table_exists,
)

router = APIRouter(prefix="/options", tags=["options-minute"])


def _iso_date(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, datetime):
        return v.date().isoformat()
    s = str(v).strip()[:10]
    return s if s else None


_PERIOD_MAP = {
    "1 min": "1 minute",
    "1 minute": "1 minute",
    "5 min": "5 minute",
    "5 mins": "5 minute",
    "5 minute": "5 minute",
    "5 minutes": "5 minute",
    "1 hour": "1 hour",
}


def query_option_minute(
    conn: Any,
    *,
    underlying: str,
    expiry: str,
    strike: float,
    option_right: str,
    period: str = "1 minute",
    limit: int = 200,
) -> dict[str, Any]:
    sym = normalize_symbol(underlying)
    if not sym:
        return {"ok": False, "error": "underlying is required"}
    if not table_exists(conn, "market", "option_minute"):
        return {"ok": True, "underlying": sym, "rows": [], "count": 0}

    db_period = _PERIOD_MAP.get(period.strip(), period.strip())
    lim = max(1, min(limit, 500))

    sql = """
        SELECT extract(epoch from bar_time) AS time,
               open, high, low, close, volume, vwap
        FROM raw_market.option_minute
        WHERE underlying = %s AND expiry = %s
          AND strike = %s AND option_right = %s
          AND period = %s
        ORDER BY bar_time DESC NULLS LAST
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (sym, expiry, strike, option_right, db_period, lim))
        raw = cur.fetchall() or []

    cols = ("time", "open", "high", "low", "close", "volume", "vwap")
    rows: list[dict[str, Any]] = []
    for r in raw:
        if hasattr(r, "keys"):
            d = dict(r)
        else:
            d = {cols[i]: r[i] for i in range(min(len(cols), len(r)))}
        rows.append(d)

    return {"ok": True, "underlying": sym, "rows": rows, "count": len(rows)}


@router.get("/minute")
def options_minute(
    underlying: str = Query(..., description="Underlying symbol (e.g. NVDA)"),
    expiry: str = Query(..., description="Expiry date YYYY-MM-DD"),
    strike: float = Query(..., description="Strike price"),
    option_right: str = Query(..., description="P or C"),
    period: str = Query("1 minute", description="Bar period (1 minute, 5 minute, 1 hour)"),
    limit: int = Query(200, ge=1, le=500),
) -> dict[str, Any]:
    """Option minute OHLCV bars from market.option_minute."""
    conn = require_db()
    try:
        return query_option_minute(
            conn,
            underlying=underlying,
            expiry=expiry,
            strike=strike,
            option_right=option_right,
            period=period,
            limit=limit,
        )
    finally:
        conn.close()
