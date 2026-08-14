"""DB-read stock daily bar routes under ``/market/stocks/db/*`` (W0-P1).

Reads from ``market.stock_daily`` — the persisted Polygon daily bars.
Separate from ``stocks.py`` which is Polygon REST pass-through.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from bifrost_market_data.api.deps import (
    normalize_symbol,
    require_db,
    row_dict,
    table_exists,
)

router = APIRouter(prefix="/stocks/db", tags=["stocks-db"])

_BARS_COLS = ("symbol", "bar_date", "open", "high", "low", "close", "volume")
_CLOSE_COLS = ("symbol", "bar_date", "close")


def _parse_symbols(raw: str) -> list[str]:
    """Split comma-separated symbols, normalize, deduplicate preserving order."""
    seen: set[str] = set()
    result: list[str] = []
    for part in raw.split(","):
        sym = normalize_symbol(part)
        if sym and sym not in seen:
            seen.add(sym)
            result.append(sym)
    return result


def _bar_row(row: Any, columns: tuple[str, ...]) -> dict[str, Any]:
    """Serialize a bar row, renaming bar_date → bar_time for consumer compat."""
    d = row_dict(row, columns)
    if "bar_date" in d:
        d["bar_time"] = d.pop("bar_date")
    return d


def query_daily_bars(
    conn: Any, *, symbols: list[str], days: int
) -> dict[str, list[dict[str, Any]]]:
    """Full OHLCV bars from market.stock_daily grouped by symbol."""
    if not table_exists(conn, "market", "stock_daily"):
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT symbol, bar_date, open, high, low, close, volume
            FROM market.stock_daily
            WHERE symbol = ANY(%s)
              AND bar_date >= (CURRENT_DATE - %s * INTERVAL '1 day')
            ORDER BY symbol, bar_date ASC
            """,
            (symbols, days),
        )
        rows = cur.fetchall() or []
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        d = _bar_row(r, _BARS_COLS)
        d["source"] = "massive"
        grouped[d["symbol"]].append(d)
    return dict(grouped)


def query_daily_close(
    conn: Any, *, symbols: list[str], days: int
) -> dict[str, list[dict[str, Any]]]:
    """Close-only series from market.stock_daily grouped by symbol."""
    if not table_exists(conn, "market", "stock_daily"):
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT symbol, bar_date, close
            FROM market.stock_daily
            WHERE symbol = ANY(%s)
              AND bar_date >= (CURRENT_DATE - %s * INTERVAL '1 day')
              AND close IS NOT NULL
            ORDER BY symbol, bar_date ASC
            """,
            (symbols, days),
        )
        rows = cur.fetchall() or []
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        grouped[_bar_row(r, _CLOSE_COLS)["symbol"]].append(_bar_row(r, _CLOSE_COLS))
    return dict(grouped)


def query_spy_close(conn: Any, *, days: int) -> list[float]:
    """SPY close values ascending — used for correlation/relative-strength."""
    if not table_exists(conn, "market", "stock_daily"):
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT close
            FROM market.stock_daily
            WHERE symbol = 'SPY'
              AND bar_date >= (CURRENT_DATE - %s * INTERVAL '1 day')
              AND close IS NOT NULL
            ORDER BY bar_date ASC
            """,
            (days,),
        )
        rows = cur.fetchall() or []
    return [float(r[0]) for r in rows if r and r[0] is not None]


# --- HTTP routes ---


@router.get("/bars/daily")
def stocks_db_bars_daily(
    symbols: str = Query(..., description="Comma-separated stock symbols"),
    days: int = Query(400, ge=1, le=3000, description="Lookback days"),
) -> dict[str, Any]:
    """Multi-symbol daily OHLCV from local DB (market.stock_daily)."""
    parsed = _parse_symbols(symbols)
    if not parsed:
        raise HTTPException(status_code=400, detail="No valid symbols provided")
    conn = require_db()
    try:
        data = query_daily_bars(conn, symbols=parsed, days=days)
        total = sum(len(v) for v in data.values())
        return {"ok": True, "data": data, "count": total}
    finally:
        conn.close()


@router.get("/bars/daily/close")
def stocks_db_bars_daily_close(
    symbols: str = Query(..., description="Comma-separated stock symbols"),
    days: int = Query(420, ge=1, le=3000, description="Lookback days"),
) -> dict[str, Any]:
    """Multi-symbol close-only series from local DB (market.stock_daily)."""
    parsed = _parse_symbols(symbols)
    if not parsed:
        raise HTTPException(status_code=400, detail="No valid symbols provided")
    conn = require_db()
    try:
        data = query_daily_close(conn, symbols=parsed, days=days)
        total = sum(len(v) for v in data.values())
        return {"ok": True, "data": data, "count": total}
    finally:
        conn.close()


@router.get("/bars/daily/spy-close")
def stocks_db_spy_close(
    days: int = Query(420, ge=1, le=3000, description="Lookback days"),
) -> dict[str, Any]:
    """SPY close series (ascending) from local DB — for CRS/correlation."""
    conn = require_db()
    try:
        values = query_spy_close(conn, days=days)
        return {"ok": True, "values": values, "count": len(values)}
    finally:
        conn.close()


_connect = require_db
