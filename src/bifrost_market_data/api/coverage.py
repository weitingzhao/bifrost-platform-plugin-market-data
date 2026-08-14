"""DB-read coverage routes under ``/market/coverage/*`` (Wave 5-B)."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from bifrost_market_data.api.deps import (
    as_date,
    connect_db,
    iso_value,
    normalize_symbol,
    require_db,
    row_dict,
    safe_count,
    table_exists,
)
from bifrost_market_data.quality import run_all_checks

router = APIRouter(prefix="/coverage", tags=["coverage"])

_RECENT_SNAPSHOT_DAYS = 7
_RECENT_BAR_DAYS = 7


def _analytics_metric_summary(conn: Any, table: str) -> dict[str, Any] | None:
    """Aggregate symbol/day coverage for one market_analytics table."""
    if not table_exists(conn, "market_analytics", table):
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    COUNT(DISTINCT UPPER(TRIM(symbol)))::bigint AS symbols,
                    COUNT(DISTINCT trade_date)::bigint AS days,
                    MAX(trade_date) AS latest
                FROM market_analytics.{table}
                """
            )
            row = cur.fetchone()
        if not row:
            return {"symbols": 0, "days": 0, "latest": None}
        return {
            "symbols": int(row[0] or 0),
            "days": int(row[1] or 0),
            "latest": iso_value(row[2]),
        }
    except Exception:
        return None


def query_inventory(conn: Any) -> dict[str, Any]:
    """One-glance inventory: breadth × depth × analytics scope (watchlist-bound)."""
    watchlist_symbols = _fetch_watchlist_symbols(conn, limit=200)
    scope = (
        "watchlist"
        if table_exists(conn, "public", "watchlist") and watchlist_symbols
        else "option_contract_underlyings"
        if watchlist_symbols
        else "empty"
    )

    stock_daily: dict[str, Any] | None = None
    if table_exists(conn, "market", "stock_daily"):
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COUNT(DISTINCT UPPER(TRIM(symbol)))::bigint AS symbols,
                        COUNT(*)::bigint AS total_rows,
                        MIN(bar_date) AS min_date,
                        MAX(bar_date) AS max_date
                    FROM market.stock_daily
                    """
                )
                row = cur.fetchone()
            if row:
                stock_daily = {
                    "symbols": int(row[0] or 0),
                    "total_rows": int(row[1] or 0),
                    "min_date": iso_value(row[2]),
                    "max_date": iso_value(row[3]),
                }
        except Exception:
            stock_daily = None

    # Stock minute schema exists but is not in the active ingest policy.
    stock_min = None

    option: dict[str, Any] | None = None
    option_payload: dict[str, Any] = {
        "underlyings": 0,
        "total_contracts": 0,
        "total_expiries": 0,
        "snapshot_symbols": 0,
        "snapshot_latest": None,
        "oi_symbols": 0,
        "oi_latest": None,
    }
    if table_exists(conn, "market", "option_contract"):
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COUNT(DISTINCT UPPER(TRIM(underlying)))::bigint AS underlyings,
                        COUNT(*)::bigint AS total_contracts,
                        COUNT(DISTINCT expiry)::bigint AS total_expiries
                    FROM market.option_contract
                    """
                )
                row = cur.fetchone()
            if row:
                option_payload["underlyings"] = int(row[0] or 0)
                option_payload["total_contracts"] = int(row[1] or 0)
                option_payload["total_expiries"] = int(row[2] or 0)
        except Exception:
            pass
    if table_exists(conn, "market", "option_snapshot"):
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COUNT(DISTINCT UPPER(TRIM(underlying)))::bigint AS symbols,
                        MAX(snapshot_ts)::date AS latest
                    FROM market.option_snapshot
                    """
                )
                row = cur.fetchone()
            if row:
                option_payload["snapshot_symbols"] = int(row[0] or 0)
                option_payload["snapshot_latest"] = iso_value(row[1])
        except Exception:
            pass
    if table_exists(conn, "market", "option_open_interest"):
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COUNT(DISTINCT UPPER(TRIM(underlying)))::bigint AS symbols,
                        MAX(trade_date) AS latest
                    FROM market.option_open_interest
                    """
                )
                row = cur.fetchone()
            if row:
                option_payload["oi_symbols"] = int(row[0] or 0)
                option_payload["oi_latest"] = iso_value(row[1])
        except Exception:
            pass
    if any(
        option_payload[k]
        for k in (
            "underlyings",
            "total_contracts",
            "total_expiries",
            "snapshot_symbols",
            "oi_symbols",
        )
    ) or option_payload["snapshot_latest"] or option_payload["oi_latest"]:
        option = option_payload

    analytics = {
        "max_pain": _analytics_metric_summary(conn, "max_pain_daily"),
        "atm_iv": _analytics_metric_summary(conn, "atm_iv_daily"),
        "pcr": _analytics_metric_summary(conn, "pcr_daily"),
        "iv_percentile": _analytics_metric_summary(conn, "iv_percentile_daily"),
    }

    return {
        "ok": True,
        "scope": scope,
        "watchlist_symbols": watchlist_symbols,
        "stock_daily": stock_daily,
        "stock_min": stock_min,
        "option": option,
        "analytics": analytics,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def query_db_summary(conn: Any) -> dict[str, Any]:
    """Aggregate row counts and ingest freshness dimensions."""
    counts: dict[str, int | None] = {
        "tickers": safe_count(conn, "market.ticker"),
        "stock_daily": safe_count(conn, "market.stock_daily"),
        "option_contract": safe_count(conn, "market.option_contract"),
        "option_snapshot": safe_count(conn, "market.option_snapshot"),
        "option_open_interest": safe_count(conn, "market.option_open_interest"),
        "corporate_action": safe_count(conn, "market.corporate_action"),
    }
    freshness: list[dict[str, Any]] = []
    if table_exists(conn, "data_ops", "ingest_freshness"):
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT dimension, last_run_at, rows_written, status, updated_at
                    FROM data_ops.ingest_freshness
                    ORDER BY dimension ASC
                    """
                )
                rows = cur.fetchall() or []
            cols = ("dimension", "last_run_at", "rows_written", "status", "updated_at")
            freshness = [row_dict(r, cols) for r in rows]
        except Exception:
            freshness = []
    return {
        "ok": True,
        "source": "db",
        "counts": counts,
        "freshness": freshness,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _fetch_watchlist_symbols(conn: Any, *, limit: int = 80) -> list[str]:
    if table_exists(conn, "public", "watchlist"):
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT UPPER(TRIM(symbol)) AS sym
                    FROM public.watchlist
                    WHERE TRIM(COALESCE(symbol, '')) <> ''
                    ORDER BY sym
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall() or []
            out = []
            for row in rows:
                sym = row[0] if not isinstance(row, dict) else row.get("sym")
                if sym:
                    out.append(str(sym).strip().upper())
            if out:
                return out
        except Exception:
            pass

    if not table_exists(conn, "market", "option_contract"):
        return []
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT UPPER(TRIM(underlying)) AS sym, COUNT(*)::bigint AS n
                FROM market.option_contract
                GROUP BY UPPER(TRIM(underlying))
                ORDER BY n DESC, sym ASC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall() or []
        return [str(r[0]).strip().upper() for r in rows if r and r[0]]
    except Exception:
        return []


def query_watchlist_coverage(conn: Any, *, limit: int = 80) -> dict[str, Any]:
    syms = _fetch_watchlist_symbols(conn, limit=limit)
    source = "public.watchlist" if table_exists(conn, "public", "watchlist") else "option_contract_underlyings"
    symbols_out: list[dict[str, Any]] = []
    if syms and table_exists(conn, "market", "option_contract"):
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        UPPER(TRIM(underlying)) AS sym,
                        COUNT(*)::bigint AS contract_count,
                        COUNT(DISTINCT expiry)::bigint AS expiries,
                        MAX(updated_at) AS newest_contract_ts
                    FROM market.option_contract
                    WHERE UPPER(TRIM(underlying)) = ANY(%s)
                    GROUP BY UPPER(TRIM(underlying))
                    """,
                    (syms,),
                )
                by_sym = {}
                cols = ("symbol", "contract_count", "expiries", "newest_contract_ts")
                for row in cur.fetchall() or []:
                    d = row_dict(row, cols)
                    d["symbol"] = d.pop("sym", None) or (row[0] if row else None)
                    by_sym[str(d["symbol"]).upper()] = d
            for sym in syms:
                symbols_out.append(
                    by_sym.get(
                        sym,
                        {"symbol": sym, "contract_count": 0, "expiries": 0, "newest_contract_ts": None},
                    )
                )
        except Exception:
            symbols_out = [{"symbol": s} for s in syms]
    else:
        symbols_out = [{"symbol": s} for s in syms]

    return {
        "ok": True,
        "source": source,
        "symbols_count": len(symbols_out),
        "symbols": symbols_out,
    }


def query_greeks_coverage(
    conn: Any,
    *,
    symbol: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    if not table_exists(conn, "market", "option_snapshot"):
        return {"ok": True, "rows": [], "count": 0, "symbol": symbol}
    clauses: list[str] = []
    params: list[Any] = []
    sym = normalize_symbol(symbol) if symbol else None
    if sym:
        clauses.append("UPPER(TRIM(underlying)) = %s")
        params.append(sym)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f"""
        WITH latest AS (
            SELECT DISTINCT ON (option_ticker)
                UPPER(TRIM(underlying)) AS symbol,
                iv, delta, gamma, theta, vega, snapshot_ts
            FROM market.option_snapshot
            {where}
            ORDER BY option_ticker, snapshot_ts DESC
        )
        SELECT
            symbol,
            COUNT(*)::bigint AS total_contracts,
            COUNT(iv)::bigint AS with_iv,
            COUNT(delta)::bigint AS with_delta,
            COUNT(CASE WHEN delta IS NOT NULL AND gamma IS NOT NULL
                        AND theta IS NOT NULL AND vega IS NOT NULL THEN 1 END)::bigint AS with_full_greeks,
            MAX(snapshot_ts) AS newest_ts
        FROM latest
        GROUP BY symbol
        ORDER BY total_contracts DESC, symbol ASC
        LIMIT %s
    """
    params.append(limit)
    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        raw = cur.fetchall() or []
    cols = ("symbol", "total_contracts", "with_iv", "with_delta", "with_full_greeks", "newest_ts")
    rows = [row_dict(r, cols) for r in raw]
    return {"ok": True, "rows": rows, "count": len(rows), "symbol": sym}


def query_contracts_coverage(conn: Any, *, limit: int = 100) -> dict[str, Any]:
    if not table_exists(conn, "market", "option_contract"):
        return {"ok": True, "rows": [], "count": 0}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                UPPER(TRIM(underlying)) AS symbol,
                COUNT(*)::bigint AS contract_count,
                COUNT(DISTINCT expiry)::bigint AS expiries,
                COUNT(DISTINCT strike)::bigint AS strikes,
                MIN(expiry) AS min_expiry,
                MAX(expiry) AS max_expiry,
                MAX(updated_at) AS newest_updated_at
            FROM market.option_contract
            GROUP BY UPPER(TRIM(underlying))
            ORDER BY contract_count DESC, symbol ASC
            LIMIT %s
            """,
            (limit,),
        )
        raw = cur.fetchall() or []
    cols = (
        "symbol",
        "contract_count",
        "expiries",
        "strikes",
        "min_expiry",
        "max_expiry",
        "newest_updated_at",
    )
    rows = [row_dict(r, cols) for r in raw]
    return {"ok": True, "rows": rows, "count": len(rows)}


def query_option_contracts_reference_gap(
    conn: Any,
    *,
    symbol: str,
    limit: int = 500,
) -> dict[str, Any]:
    sym = normalize_symbol(symbol)
    if not sym:
        return {"ok": False, "error": "symbol is required"}
    if not table_exists(conn, "market", "option_contract"):
        return {"ok": True, "symbol": sym, "gap_count": 0, "gaps": []}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                option_ticker,
                underlying,
                expiry,
                strike,
                option_right,
                exercise_style,
                shares_per_contract
            FROM market.option_contract
            WHERE UPPER(TRIM(underlying)) = %s
              AND (
                    TRIM(COALESCE(option_ticker, '')) = ''
                 OR expiry IS NULL
                 OR strike IS NULL
                 OR TRIM(COALESCE(option_right::text, '')) = ''
                 OR exercise_style IS NULL
                 OR TRIM(exercise_style) = ''
                 OR shares_per_contract IS NULL
              )
            ORDER BY expiry NULLS LAST, strike NULLS LAST, option_ticker
            LIMIT %s
            """,
            (sym, limit),
        )
        raw = cur.fetchall() or []
    cols = (
        "option_ticker",
        "underlying",
        "expiry",
        "strike",
        "option_right",
        "exercise_style",
        "shares_per_contract",
    )
    gaps = [row_dict(r, cols) for r in raw]
    return {
        "ok": True,
        "symbol": sym,
        "gap_count": len(gaps),
        "gaps": gaps,
        "note": "Simplified local gap: incomplete contract identity or nullable reference fields.",
    }


def query_option_snapshots_contracts_gap(
    conn: Any,
    *,
    symbol: str,
    recent_days: int = _RECENT_SNAPSHOT_DAYS,
) -> dict[str, Any]:
    sym = normalize_symbol(symbol)
    if not sym:
        return {"ok": False, "error": "symbol is required"}
    if not table_exists(conn, "market", "option_contract"):
        return {"ok": True, "symbol": sym, "missing_snapshot_count": 0, "contracts": []}
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH recent_snaps AS (
                SELECT DISTINCT option_ticker
                FROM market.option_snapshot
                WHERE UPPER(TRIM(underlying)) = %s
                  AND snapshot_ts >= NOW() - (%s || ' days')::interval
            )
            SELECT
                c.option_ticker,
                c.expiry,
                c.strike,
                c.option_right,
                c.updated_at
            FROM market.option_contract c
            LEFT JOIN recent_snaps s ON s.option_ticker = c.option_ticker
            WHERE UPPER(TRIM(c.underlying)) = %s
              AND s.option_ticker IS NULL
            ORDER BY c.expiry, c.strike, c.option_ticker
            LIMIT 500
            """,
            (sym, recent_days, sym),
        )
        raw = cur.fetchall() or []
        cur.execute(
            """
            SELECT COUNT(*)::bigint FROM market.option_contract
            WHERE UPPER(TRIM(underlying)) = %s
            """,
            (sym,),
        )
        total_row = cur.fetchone()
        total = int(total_row[0] or 0) if total_row else 0
    cols = ("option_ticker", "expiry", "strike", "option_right", "updated_at")
    contracts = [row_dict(r, cols) for r in raw]
    return {
        "ok": True,
        "symbol": sym,
        "recent_days": recent_days,
        "contract_total": total,
        "missing_snapshot_count": len(contracts),
        "contracts": contracts,
        "note": "Simplified local gap: contracts with no snapshot in recent window.",
    }


def query_option_bars_contracts_gap(
    conn: Any,
    *,
    symbol: str,
    recent_days: int = _RECENT_BAR_DAYS,
) -> dict[str, Any]:
    sym = normalize_symbol(symbol)
    if not sym:
        return {"ok": False, "error": "symbol is required"}
    if not table_exists(conn, "market", "option_contract"):
        return {"ok": True, "symbol": sym, "missing_bar_count": 0, "contracts": []}
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH recent_bars AS (
                SELECT DISTINCT option_ticker
                FROM market.option_daily
                WHERE UPPER(TRIM(underlying)) = %s
                  AND bar_date >= CURRENT_DATE - (%s || ' days')::interval
            )
            SELECT
                c.option_ticker,
                c.expiry,
                c.strike,
                c.option_right
            FROM market.option_contract c
            LEFT JOIN recent_bars b ON b.option_ticker = c.option_ticker
            WHERE UPPER(TRIM(c.underlying)) = %s
              AND b.option_ticker IS NULL
            ORDER BY c.expiry, c.strike, c.option_ticker
            LIMIT 500
            """,
            (sym, recent_days, sym),
        )
        raw = cur.fetchall() or []
    cols = ("option_ticker", "expiry", "strike", "option_right")
    contracts = [row_dict(r, cols) for r in raw]
    return {
        "ok": True,
        "symbol": sym,
        "recent_days": recent_days,
        "missing_bar_count": len(contracts),
        "contracts": contracts,
        "note": "Simplified local gap: contracts with no option_daily bar in recent window.",
    }


def query_bar_quality_detail(
    conn: Any,
    *,
    symbol: str,
    days: int = 90,
) -> dict[str, Any]:
    sym = normalize_symbol(symbol)
    if not sym:
        return {"ok": False, "error": "symbol is required"}
    if not table_exists(conn, "market", "stock_daily"):
        return {"ok": True, "symbol": sym, "latest_date": None, "daily": [], "summary": {}}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                bar_date,
                open, high, low, close, volume, vwap
            FROM market.stock_daily
            WHERE UPPER(TRIM(symbol)) = %s
              AND bar_date >= CURRENT_DATE - (%s || ' days')::interval
            ORDER BY bar_date DESC
            """,
            (sym, days),
        )
        raw = cur.fetchall() or []
        cur.execute(
            """
            SELECT
                COUNT(*)::bigint,
                MIN(bar_date),
                MAX(bar_date)
            FROM market.stock_daily
            WHERE UPPER(TRIM(symbol)) = %s
            """,
            (sym,),
        )
        summary_row = cur.fetchone()
    cols = ("bar_date", "open", "high", "low", "close", "volume", "vwap")
    daily: list[dict[str, Any]] = []
    for row in raw:
        d = row_dict(row, cols)
        ohlc_ok = all(d.get(k) is not None for k in ("open", "high", "low", "close"))
        d["ohlc_complete"] = ohlc_ok
        daily.append(d)
    summary = {}
    if summary_row:
        summary = {
            "row_count": int(summary_row[0] or 0),
            "min_date": iso_value(summary_row[1]),
            "max_date": iso_value(summary_row[2]),
        }
    latest = daily[0]["bar_date"] if daily else None
    return {"ok": True, "symbol": sym, "table": "stock_daily", "latest_date": latest, "summary": summary, "daily": daily}


def query_snapshot_quality_detail(
    conn: Any,
    *,
    symbol: str,
    days: int = 30,
) -> dict[str, Any]:
    sym = normalize_symbol(symbol)
    if not sym:
        return {"ok": False, "error": "symbol is required"}
    if not table_exists(conn, "market", "option_snapshot"):
        return {"ok": True, "symbol": sym, "latest_date": None, "daily": [], "expiries": []}
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH daily_latest AS (
                SELECT DISTINCT ON (
                    DATE(timezone('America/New_York', snapshot_ts)),
                    option_ticker
                )
                    DATE(timezone('America/New_York', snapshot_ts)) AS snap_day,
                    iv, delta, gamma, theta, vega, open_interest, day_close
                FROM market.option_snapshot
                WHERE UPPER(TRIM(underlying)) = %s
                  AND snapshot_ts >= NOW() - (%s || ' days')::interval
                ORDER BY DATE(timezone('America/New_York', snapshot_ts)),
                         option_ticker,
                         snapshot_ts DESC
            )
            SELECT
                snap_day,
                COUNT(*)::int AS contract_count,
                ROUND(COUNT(iv)::numeric / NULLIF(COUNT(*), 0) * 100, 1) AS iv_pct,
                ROUND(COUNT(CASE WHEN delta IS NOT NULL AND gamma IS NOT NULL
                                 AND theta IS NOT NULL AND vega IS NOT NULL
                            THEN 1 END)::numeric / NULLIF(COUNT(*), 0) * 100, 1) AS full_greeks_pct,
                ROUND(COUNT(open_interest)::numeric / NULLIF(COUNT(*), 0) * 100, 1) AS oi_pct
            FROM daily_latest
            GROUP BY snap_day
            ORDER BY snap_day DESC
            """,
            (sym, days),
        )
        raw = cur.fetchall() or []
    daily = [
        {
            "snap_day": iso_value(r[0]),
            "contract_count": int(r[1] or 0),
            "iv_pct": float(r[2]) if r[2] is not None else None,
            "full_greeks_pct": float(r[3]) if r[3] is not None else None,
            "oi_pct": float(r[4]) if r[4] is not None else None,
        }
        for r in raw
    ]
    return {
        "ok": True,
        "symbol": sym,
        "source": "db",
        "latest_date": daily[0]["snap_day"] if daily else None,
        "daily": daily,
        "expiries": [],
    }


def query_stock_day_gap(
    conn: Any,
    *,
    symbol: str,
    years: int = 5,
) -> dict[str, Any]:
    sym = normalize_symbol(symbol)
    if not sym:
        return {"ok": False, "error": "symbol is required"}
    start = date.today() - timedelta(days=years * 365)
    expected_days: list[date] = []
    if table_exists(conn, "data_ops", "us_trading_calendar"):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT cal_date
                FROM data_ops.us_trading_calendar
                WHERE is_trading = true
                  AND cal_date >= %s
                  AND cal_date <= CURRENT_DATE
                ORDER BY cal_date
                """,
                (start,),
            )
            expected_days = [as_date(r[0]) for r in (cur.fetchall() or []) if as_date(r[0])]
    else:
        d = start
        while d <= date.today():
            if d.weekday() < 5:
                expected_days.append(d)
            d += timedelta(days=1)

    covered: set[date] = set()
    if table_exists(conn, "market", "stock_daily"):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT bar_date
                FROM market.stock_daily
                WHERE UPPER(TRIM(symbol)) = %s
                  AND bar_date >= %s
                """,
                (sym, start),
            )
            covered = {d for d in (as_date(r[0]) for r in (cur.fetchall() or [])) if d}

    missing = [d.isoformat() for d in expected_days if d not in covered]
    return {
        "ok": True,
        "symbol": sym,
        "lookback_years": years,
        "expected_trading_days": len(expected_days),
        "covered_days": len(covered),
        "missing_days": len(missing),
        "missing_dates": missing[:120],
        "note": "Simplified calendar gap using us_trading_calendar when available.",
    }


def query_stock_day_quality_detail(
    conn: Any,
    *,
    symbol: str,
    days: int = 90,
) -> dict[str, Any]:
    return query_bar_quality_detail(conn, symbol=symbol, days=days)


@router.get("/quality-score")
def coverage_quality_score() -> dict[str, Any]:
    """Run P7 data-quality checks (stock daily / option snapshot / OI / freshness)."""
    conn = require_db()
    try:
        return run_all_checks(conn)
    finally:
        conn.close()


@router.get("/inventory")
def coverage_inventory() -> dict[str, Any]:
    """Aggregate data scope — one-glance inventory of all tracked data."""
    conn = require_db()
    try:
        return query_inventory(conn)
    finally:
        conn.close()


@router.get("/db-summary")
def coverage_db_summary() -> dict[str, Any]:
    conn = require_db()
    try:
        return query_db_summary(conn)
    finally:
        conn.close()


@router.get("/watchlist")
def coverage_watchlist(limit: int = Query(80, ge=1, le=200)) -> dict[str, Any]:
    conn = require_db()
    try:
        return query_watchlist_coverage(conn, limit=limit)
    finally:
        conn.close()


@router.get("/greeks")
def coverage_greeks(
    symbol: str | None = Query(None, description="Optional underlying filter"),
    limit: int = Query(100, ge=1, le=500),
) -> dict[str, Any]:
    conn = require_db()
    try:
        return query_greeks_coverage(conn, symbol=symbol, limit=limit)
    finally:
        conn.close()


@router.get("/contracts")
def coverage_contracts(limit: int = Query(100, ge=1, le=500)) -> dict[str, Any]:
    conn = require_db()
    try:
        return query_contracts_coverage(conn, limit=limit)
    finally:
        conn.close()


@router.get("/option-contracts-reference-gap")
def coverage_option_contracts_reference_gap(
    symbol: str = Query(..., description="Underlying symbol"),
    limit: int = Query(500, ge=1, le=2000),
) -> dict[str, Any]:
    conn = require_db()
    try:
        result = query_option_contracts_reference_gap(conn, symbol=symbol, limit=limit)
        if not result.get("ok"):
            raise HTTPException(status_code=400, detail=result.get("error", "bad request"))
        return result
    finally:
        conn.close()


@router.get("/option-snapshots-contracts-gap")
def coverage_option_snapshots_contracts_gap(
    symbol: str = Query(..., description="Underlying symbol"),
    recent_days: int = Query(_RECENT_SNAPSHOT_DAYS, ge=1, le=90),
) -> dict[str, Any]:
    conn = require_db()
    try:
        result = query_option_snapshots_contracts_gap(conn, symbol=symbol, recent_days=recent_days)
        if not result.get("ok"):
            raise HTTPException(status_code=400, detail=result.get("error", "bad request"))
        return result
    finally:
        conn.close()


@router.get("/option-bars-contracts-gap")
def coverage_option_bars_contracts_gap(
    symbol: str = Query(..., description="Underlying symbol"),
    recent_days: int = Query(_RECENT_BAR_DAYS, ge=1, le=90),
) -> dict[str, Any]:
    conn = require_db()
    try:
        result = query_option_bars_contracts_gap(conn, symbol=symbol, recent_days=recent_days)
        if not result.get("ok"):
            raise HTTPException(status_code=400, detail=result.get("error", "bad request"))
        return result
    finally:
        conn.close()


@router.get("/bar-quality-detail")
def coverage_bar_quality_detail(
    symbol: str = Query(..., description="Stock symbol"),
    days: int = Query(90, ge=1, le=365),
) -> dict[str, Any]:
    conn = require_db()
    try:
        result = query_bar_quality_detail(conn, symbol=symbol, days=days)
        if not result.get("ok"):
            raise HTTPException(status_code=400, detail=result.get("error", "bad request"))
        return result
    finally:
        conn.close()


@router.get("/snapshot-quality-detail")
def coverage_snapshot_quality_detail(
    symbol: str = Query(..., description="Underlying symbol"),
    days: int = Query(30, ge=1, le=365),
) -> dict[str, Any]:
    conn = require_db()
    try:
        result = query_snapshot_quality_detail(conn, symbol=symbol, days=days)
        if not result.get("ok"):
            raise HTTPException(status_code=400, detail=result.get("error", "bad request"))
        return result
    finally:
        conn.close()


@router.get("/stock-day-gap")
def coverage_stock_day_gap(
    symbol: str = Query(..., description="Stock symbol"),
    years: int = Query(5, ge=1, le=20),
) -> dict[str, Any]:
    conn = require_db()
    try:
        result = query_stock_day_gap(conn, symbol=symbol, years=years)
        if not result.get("ok"):
            raise HTTPException(status_code=400, detail=result.get("error", "bad request"))
        return result
    finally:
        conn.close()


@router.get("/stock-day-quality-detail")
def coverage_stock_day_quality_detail(
    symbol: str = Query(..., description="Stock symbol"),
    days: int = Query(90, ge=1, le=365),
) -> dict[str, Any]:
    conn = require_db()
    try:
        result = query_stock_day_quality_detail(conn, symbol=symbol, days=days)
        if not result.get("ok"):
            raise HTTPException(status_code=400, detail=result.get("error", "bad request"))
        return result
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# SEPA / Data Readiness coverage (W2-P3)
# ---------------------------------------------------------------------------

_SEPA_COVERAGE_TABLES: list[tuple[str, str, str]] = [
    ("market", "stock_daily", "bar_date"),
    ("market", "stock_minute", "bar_time"),
    ("market", "option_contract", "updated_at"),
    ("market", "option_snapshot", "snapshot_ts"),
    ("market", "option_open_interest", "trade_date"),
    ("market", "option_daily", "bar_date"),
    ("market", "ticker", "updated_at"),
    ("market", "stock_financials", "updated_at"),
    ("market", "corporate_action", "updated_at"),
]


def query_sepa_stats(conn: Any) -> dict[str, Any]:
    """Row counts and latest date for each market.* table used by SEPA."""
    tables: list[dict[str, Any]] = []
    for schema, table, date_col in _SEPA_COVERAGE_TABLES:
        if not table_exists(conn, schema, table):
            tables.append({"table": f"{schema}.{table}", "row_count": None, "latest": None})
            continue
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*)::bigint, MAX({date_col}) FROM {schema}.{table}"
                )
                row = cur.fetchone()
            cnt = int(row[0] or 0) if row else 0
            latest = iso_value(row[1]) if row and row[1] else None
            tables.append({"table": f"{schema}.{table}", "row_count": cnt, "latest": latest})
        except Exception:
            tables.append({"table": f"{schema}.{table}", "row_count": None, "latest": None})
    return {"ok": True, "tables": tables}


def query_distributions(
    conn: Any,
    *,
    table: str,
    limit: int = 200,
) -> dict[str, Any]:
    """Per-symbol row count distribution for a given table."""
    valid_tables: dict[str, tuple[str, str]] = {
        "stock_daily": ("market", "symbol"),
        "stock_minute": ("market", "symbol"),
        "option_contract": ("market", "underlying"),
        "option_snapshot": ("market", "underlying"),
        "option_open_interest": ("market", "underlying"),
        "option_daily": ("market", "underlying"),
        "stock_financials": ("market", "symbol"),
    }
    if table not in valid_tables:
        return {"ok": False, "error": f"Invalid table; choose from: {list(valid_tables.keys())}"}

    schema, sym_col = valid_tables[table]
    qualified = f"{schema}.{table}"
    if not table_exists(conn, schema, table):
        return {"ok": True, "table": qualified, "distributions": [], "count": 0}

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT UPPER(TRIM({sym_col})) AS symbol, COUNT(*)::bigint AS row_count
            FROM {qualified}
            WHERE TRIM(COALESCE({sym_col}, '')) <> ''
            GROUP BY UPPER(TRIM({sym_col}))
            ORDER BY row_count DESC, symbol ASC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall() or []

    distributions = [
        {"symbol": str(r[0]).strip(), "row_count": int(r[1])} for r in rows if r[0]
    ]
    return {"ok": True, "table": qualified, "distributions": distributions, "count": len(distributions)}


@router.get("/sepa-stats")
def coverage_sepa_stats() -> dict[str, Any]:
    """Row counts and latest timestamps for all market.* tables used by SEPA."""
    conn = require_db()
    try:
        return query_sepa_stats(conn)
    finally:
        conn.close()


@router.get("/distributions")
def coverage_distributions(
    table: str = Query(..., description="Table name (e.g. stock_daily, option_contract)"),
    limit: int = Query(200, ge=1, le=2000),
) -> dict[str, Any]:
    """Per-symbol row count distribution for a given market table."""
    conn = require_db()
    try:
        return query_distributions(conn, table=table, limit=limit)
    finally:
        conn.close()


# Re-export for tests that monkeypatch connect
_connect = connect_db
