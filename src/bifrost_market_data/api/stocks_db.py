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


# ---------------------------------------------------------------------------
# Extended bar queries for Trade Core monitor/reader/market.py (W2-P3)
# ---------------------------------------------------------------------------

_MINUTE_PERIOD_TO_DB: dict[str, str] = {
    "1 min": "1 minute",
    "1 minute": "1 minute",
    "5 mins": "5 minute",
    "5 min": "5 minute",
    "5 minutes": "5 minute",
    "5 minute": "5 minute",
    "1 hour": "1 hour",
    "1 hours": "1 hour",
}


def _minute_period_db(period: str) -> str:
    per = (period or "").strip()
    return _MINUTE_PERIOD_TO_DB.get(per, per)


def query_bars(
    conn: Any,
    *,
    symbol: str,
    period: str = "1 D",
    limit: int = 200,
) -> list[dict[str, Any]]:
    """Return bars from market.stock_daily (1 D) or market.stock_minute. Newest first."""
    per = (period or "1 D").strip()
    with conn.cursor() as cur:
        if per.upper() == "1 D":
            if not table_exists(conn, "market", "stock_daily"):
                return []
            cur.execute(
                """
                SELECT symbol, '1 D' AS period, extract(epoch from bar_date) AS time,
                       open, high, low, close, volume
                FROM market.stock_daily
                WHERE symbol = %s
                ORDER BY bar_date DESC NULLS LAST
                LIMIT %s
                """,
                (symbol, limit),
            )
        else:
            if not table_exists(conn, "market", "stock_minute"):
                return []
            cur.execute(
                """
                SELECT symbol, %s AS period, extract(epoch from bar_time) AS time,
                       open, high, low, close, volume
                FROM market.stock_minute
                WHERE symbol = %s AND period = %s
                ORDER BY bar_time DESC NULLS LAST
                LIMIT %s
                """,
                (per, symbol, _minute_period_db(per), limit),
            )
        rows = cur.fetchall() or []
    cols = ("symbol", "period", "time", "open", "high", "low", "close", "volume")
    return [row_dict(r, cols) for r in rows]


def query_bars_latest(
    conn: Any,
    *,
    symbol: str,
    period: str = "1 D",
) -> float | None:
    """Return Unix time of the latest bar for symbol+period."""
    per = (period or "1 D").strip()
    with conn.cursor() as cur:
        if per.upper() == "1 D":
            if not table_exists(conn, "market", "stock_daily"):
                return None
            cur.execute(
                """
                SELECT extract(epoch from bar_date) AS t
                FROM market.stock_daily WHERE symbol = %s
                ORDER BY bar_date DESC LIMIT 1
                """,
                (symbol,),
            )
        else:
            if not table_exists(conn, "market", "stock_minute"):
                return None
            cur.execute(
                """
                SELECT extract(epoch from bar_time) AS t
                FROM market.stock_minute WHERE symbol = %s AND period = %s
                ORDER BY bar_time DESC LIMIT 1
                """,
                (symbol, _minute_period_db(per)),
            )
        row = cur.fetchone()
    if row is None:
        return None
    val = row[0] if not isinstance(row, dict) else next(iter(row.values()), None)
    return float(val) if val is not None else None


def query_bar_times_in_range(
    conn: Any,
    *,
    symbol: str,
    period: str = "1 D",
    start_ts: float,
    end_ts: float,
) -> list[float]:
    """Return bar timestamps within [start_ts, end_ts] ordered ascending."""
    per = (period or "1 D").strip()
    with conn.cursor() as cur:
        if per.upper() == "1 D":
            if not table_exists(conn, "market", "stock_daily"):
                return []
            cur.execute(
                """
                SELECT extract(epoch from bar_date) AS t
                FROM market.stock_daily
                WHERE symbol = %s
                  AND bar_date >= to_timestamp(%s)::date
                  AND bar_date <= to_timestamp(%s)::date
                ORDER BY bar_date ASC
                """,
                (symbol, start_ts, end_ts),
            )
        else:
            if not table_exists(conn, "market", "stock_minute"):
                return []
            cur.execute(
                """
                SELECT extract(epoch from bar_time) AS t
                FROM market.stock_minute
                WHERE symbol = %s AND period = %s
                  AND bar_time >= to_timestamp(%s)
                  AND bar_time <= to_timestamp(%s)
                ORDER BY bar_time ASC
                """,
                (symbol, _minute_period_db(per), start_ts, end_ts),
            )
        rows = cur.fetchall() or []
    return [float(r[0]) for r in rows if r and r[0] is not None]


def query_bars_benchmark(
    conn: Any,
    *,
    symbols: list[str],
    on_or_before: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Latest daily bar on or before given date per symbol."""
    if not table_exists(conn, "market", "stock_daily"):
        return {}
    from datetime import date as _date

    ref = _date.fromisoformat(on_or_before) if on_or_before else _date.today()
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH ordered AS (
                SELECT symbol, bar_date, close,
                       LEAD(close) OVER (PARTITION BY symbol ORDER BY bar_date DESC) AS prev_close
                FROM market.stock_daily
                WHERE symbol = ANY(%s) AND bar_date <= %s
            )
            SELECT DISTINCT ON (symbol) symbol,
                   extract(epoch from bar_date) AS bar_time,
                   close,
                   prev_close
            FROM ordered
            ORDER BY symbol, bar_date DESC
            """,
            (symbols, ref),
        )
        rows = cur.fetchall() or []
    result: dict[str, dict[str, Any]] = {}
    for r in rows:
        sym = str(r[0]).strip() if r[0] else ""
        if not sym:
            continue
        result[sym] = {
            "bar_time": float(r[1]) if r[1] is not None else 0,
            "close": float(r[2]) if r[2] is not None else 0,
            "prev_close": float(r[3]) if r[3] is not None else None,
        }
    return result


def query_fallback_price(
    conn: Any,
    *,
    symbol: str,
) -> dict[str, Any]:
    """Return (close, bar_time_epoch, prev_close) from market.stock_daily as fallback."""
    if not table_exists(conn, "market", "stock_daily"):
        return {"ok": True, "symbol": symbol, "found": False}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT bar_date, close, extract(epoch from bar_date) AS bar_time_epoch
            FROM market.stock_daily
            WHERE symbol = %s
            ORDER BY bar_date DESC
            LIMIT 3
            """,
            (symbol,),
        )
        rows = cur.fetchall() or []
    if not rows:
        return {"ok": True, "symbol": symbol, "found": False}

    r = rows[0]
    close_val = r[1] if not isinstance(r, dict) else r.get("close")
    ts_val = r[2] if not isinstance(r, dict) else r.get("bar_time_epoch")
    prev_close = None
    if len(rows) > 1:
        pc = rows[1][1] if not isinstance(rows[1], dict) else rows[1].get("close")
        if pc is not None:
            prev_close = float(pc)

    if close_val is None or ts_val is None:
        return {"ok": True, "symbol": symbol, "found": False}

    return {
        "ok": True,
        "symbol": symbol,
        "found": True,
        "close": float(close_val),
        "bar_time": float(ts_val),
        "prev_close": prev_close,
    }


def query_bars_stats(
    conn: Any,
    *,
    symbol: str,
) -> dict[str, Any]:
    """Row counts in market.stock_daily and market.stock_minute for one symbol."""
    out: dict[str, Any] = {"stock_day": 0, "stock_min": {}}
    if table_exists(conn, "market", "stock_daily"):
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM market.stock_daily WHERE symbol = %s", (symbol,))
            row = cur.fetchone()
            out["stock_day"] = int(row[0]) if row and row[0] is not None else 0
    if table_exists(conn, "market", "stock_minute"):
        with conn.cursor() as cur:
            for api_per in ("1 min", "5 mins", "1 hour"):
                cur.execute(
                    "SELECT COUNT(*) FROM market.stock_minute WHERE symbol = %s AND period = %s",
                    (symbol, _minute_period_db(api_per)),
                )
                r = cur.fetchone()
                out["stock_min"][api_per] = int(r[0]) if r and r[0] is not None else 0
    return out


def query_bars_coverage(
    conn: Any,
    *,
    symbols: list[str],
) -> list[dict[str, Any]]:
    """Per-symbol coverage for stock_daily and stock_minute."""
    if not symbols:
        return []
    empty_day: dict[str, Any] = {"count": 0, "min_day": None, "max_day": None, "min_ts": None, "max_ts": None}
    out: list[dict[str, Any]] = []

    day_rows: dict[str, dict[str, Any]] = {}
    if table_exists(conn, "market", "stock_daily"):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT UPPER(TRIM(symbol)) AS sym,
                       COUNT(*)::bigint AS cnt,
                       MIN(bar_date)::text AS min_day,
                       MAX(bar_date)::text AS max_day,
                       extract(epoch from MIN(bar_date)) AS min_ts,
                       extract(epoch from MAX(bar_date)) AS max_ts
                FROM market.stock_daily
                WHERE UPPER(TRIM(symbol)) = ANY(%s)
                GROUP BY UPPER(TRIM(symbol))
                """,
                ([s.upper() for s in symbols],),
            )
            for row in cur.fetchall() or []:
                sn = str(row[0]).strip().upper()
                day_rows[sn] = {
                    "count": int(row[1]),
                    "min_day": str(row[2])[:10] if row[2] else None,
                    "max_day": str(row[3])[:10] if row[3] else None,
                    "min_ts": float(row[4]) if row[4] is not None else None,
                    "max_ts": float(row[5]) if row[5] is not None else None,
                }

    min_rows: dict[str, dict[str, dict[str, Any]]] = {}
    if table_exists(conn, "market", "stock_minute"):
        db_periods = [_minute_period_db(p) for p in ("1 min", "5 mins", "1 hour")]
        db_to_api = {_minute_period_db(p): p for p in ("1 min", "5 mins", "1 hour")}
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT UPPER(TRIM(symbol)) AS sym, period,
                       COUNT(*)::bigint AS cnt,
                       extract(epoch from MIN(bar_time)) AS min_ts,
                       extract(epoch from MAX(bar_time)) AS max_ts
                FROM market.stock_minute
                WHERE UPPER(TRIM(symbol)) = ANY(%s)
                  AND period = ANY(%s)
                GROUP BY UPPER(TRIM(symbol)), period
                """,
                ([s.upper() for s in symbols], db_periods),
            )
            for row in cur.fetchall() or []:
                sn = str(row[0]).strip().upper()
                per = db_to_api.get(str(row[1]), str(row[1]))
                if sn not in min_rows:
                    min_rows[sn] = {}
                min_rows[sn][per] = {
                    "count": int(row[2]),
                    "min_ts": float(row[3]) if row[3] is not None else None,
                    "max_ts": float(row[4]) if row[4] is not None else None,
                }

    for sym in symbols:
        n = sym.upper()
        day = day_rows.get(n, dict(empty_day))
        mins = min_rows.get(n, {})
        out.append({
            "symbol": sym,
            "stock_day": day,
            "stock_min": {
                "1 min": mins.get("1 min", {"count": 0, "min_ts": None, "max_ts": None}),
                "5 mins": mins.get("5 mins", {"count": 0, "min_ts": None, "max_ts": None}),
                "1 hour": mins.get("1 hour", {"count": 0, "min_ts": None, "max_ts": None}),
            },
        })
    return out


def query_caret_symbols(conn: Any) -> list[str]:
    """Symbols starting with ^ in stock_daily or stock_minute."""
    out: set[str] = set()
    if table_exists(conn, "market", "stock_daily"):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT symbol FROM market.stock_daily
                WHERE symbol LIKE '^%%' OR symbol LIKE U&'\\FF3E%%'
                """
            )
            for row in cur.fetchall() or []:
                s = str(row[0]).strip() if row[0] else ""
                if s:
                    out.add(s)
    if table_exists(conn, "market", "stock_minute"):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT symbol FROM market.stock_minute
                WHERE symbol LIKE '^%%' OR symbol LIKE U&'\\FF3E%%'
                """
            )
            for row in cur.fetchall() or []:
                s = str(row[0]).strip() if row[0] else ""
                if s:
                    out.add(s)
    return sorted(out)


# --- HTTP routes (W2-P3) ---


@router.get("/bars")
def stocks_db_bars(
    symbol: str = Query(..., description="Stock symbol"),
    period: str = Query("1 D", description="Bar period: '1 D', '1 min', '5 mins', '1 hour'"),
    limit: int = Query(200, ge=1, le=5000),
) -> dict[str, Any]:
    """Return bars from market.stock_daily or market.stock_minute. Newest first."""
    sym = normalize_symbol(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required")
    conn = require_db()
    try:
        rows = query_bars(conn, symbol=sym, period=period, limit=limit)
        return {"ok": True, "symbol": sym, "period": period, "rows": rows, "count": len(rows)}
    finally:
        conn.close()


@router.get("/bars/latest")
def stocks_db_bars_latest(
    symbol: str = Query(..., description="Stock symbol"),
    period: str = Query("1 D", description="Bar period"),
) -> dict[str, Any]:
    """Unix timestamp of the latest bar for symbol+period."""
    sym = normalize_symbol(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required")
    conn = require_db()
    try:
        ts = query_bars_latest(conn, symbol=sym, period=period)
        return {"ok": True, "symbol": sym, "period": period, "latest_ts": ts}
    finally:
        conn.close()


@router.get("/bars/range")
def stocks_db_bars_range(
    symbol: str = Query(..., description="Stock symbol"),
    period: str = Query("1 D", description="Bar period"),
    start_ts: float = Query(..., description="Start Unix timestamp"),
    end_ts: float = Query(..., description="End Unix timestamp"),
) -> dict[str, Any]:
    """Bar timestamps within [start_ts, end_ts] ascending."""
    sym = normalize_symbol(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required")
    conn = require_db()
    try:
        times = query_bar_times_in_range(conn, symbol=sym, period=period, start_ts=start_ts, end_ts=end_ts)
        return {"ok": True, "symbol": sym, "period": period, "times": times, "count": len(times)}
    finally:
        conn.close()


@router.get("/bars/benchmark")
def stocks_db_bars_benchmark(
    symbols: str = Query(..., description="Comma-separated symbols (e.g. SPY,QQQ)"),
    on_or_before: str | None = Query(None, description="Date YYYY-MM-DD (default: today)"),
) -> dict[str, Any]:
    """Latest daily bar on or before date per symbol."""
    parsed = _parse_symbols(symbols)
    if not parsed:
        raise HTTPException(status_code=400, detail="No valid symbols provided")
    conn = require_db()
    try:
        data = query_bars_benchmark(conn, symbols=parsed, on_or_before=on_or_before)
        return {"ok": True, "data": data, "count": len(data)}
    finally:
        conn.close()


@router.get("/bars/fallback-price")
def stocks_db_bars_fallback_price(
    symbol: str = Query(..., description="Stock symbol"),
) -> dict[str, Any]:
    """Fallback close price from market.stock_daily when live quote is unavailable."""
    sym = normalize_symbol(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required")
    conn = require_db()
    try:
        return query_fallback_price(conn, symbol=sym)
    finally:
        conn.close()


@router.get("/bars/stats")
def stocks_db_bars_stats(
    symbol: str = Query(..., description="Stock symbol"),
) -> dict[str, Any]:
    """Row counts for stock_daily and stock_minute for a symbol."""
    sym = normalize_symbol(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required")
    conn = require_db()
    try:
        stats = query_bars_stats(conn, symbol=sym)
        return {"ok": True, "symbol": sym, **stats}
    finally:
        conn.close()


@router.get("/bars/coverage")
def stocks_db_bars_coverage(
    symbols: str = Query(..., description="Comma-separated symbols"),
) -> dict[str, Any]:
    """Per-symbol bar coverage for stock_daily and stock_minute."""
    parsed = _parse_symbols(symbols)
    if not parsed:
        raise HTTPException(status_code=400, detail="No valid symbols provided")
    conn = require_db()
    try:
        data = query_bars_coverage(conn, symbols=parsed)
        return {"ok": True, "symbols": data, "count": len(data)}
    finally:
        conn.close()


@router.get("/bars/caret-symbols")
def stocks_db_bars_caret_symbols() -> dict[str, Any]:
    """Distinct symbols starting with ^ in stock bars tables."""
    conn = require_db()
    try:
        syms = query_caret_symbols(conn)
        return {"ok": True, "symbols": syms, "count": len(syms)}
    finally:
        conn.close()
