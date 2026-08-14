"""Readiness snapshot data endpoints (W2 residual cleanup).

Aggregated market data for Trade API's readiness_snapshot.py so it can
replace embedded market.* SQL with Plugin API HTTP calls.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query

from bifrost_market_data.api.deps import require_db, table_exists

router = APIRouter(prefix="/readiness", tags=["readiness-data"])


def query_bar_aggregate(
    conn: Any,
    *,
    window_days: int = 420,
) -> dict[str, Any]:
    """Per-symbol bar aggregate stats from market.stock_daily within a date window.

    Returns: bar_rows, first_bar_date, last_bar_date, null_close_rows, null_volume_rows
    per symbol. Used by readiness snapshot bars CTE.
    """
    if not table_exists(conn, "market", "stock_daily"):
        return {"ok": True, "symbols": {}}

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                UPPER(TRIM(symbol)) AS symbol,
                COUNT(*)::integer AS bar_rows,
                MIN(bar_date)::text AS first_bar_date,
                MAX(bar_date)::text AS last_bar_date,
                COUNT(*) FILTER (WHERE close IS NULL)::integer AS null_close_rows,
                COUNT(*) FILTER (WHERE volume IS NULL)::integer AS null_volume_rows
            FROM market.stock_daily
            WHERE bar_date >= (CURRENT_DATE - %s)::date
              AND bar_date <= CURRENT_DATE
            GROUP BY UPPER(TRIM(symbol))
            """,
            (window_days,),
        )
        raw = cur.fetchall() or []

    symbols: dict[str, dict[str, Any]] = {}
    for r in raw:
        if hasattr(r, "keys"):
            sym = str(r["symbol"])
            symbols[sym] = {
                "bar_rows": r["bar_rows"],
                "first_bar_date": r["first_bar_date"],
                "last_bar_date": r["last_bar_date"],
                "null_close_rows": r["null_close_rows"],
                "null_volume_rows": r["null_volume_rows"],
            }
        else:
            sym = str(r[0])
            symbols[sym] = {
                "bar_rows": int(r[1] or 0),
                "first_bar_date": str(r[2]) if r[2] else None,
                "last_bar_date": str(r[3]) if r[3] else None,
                "null_close_rows": int(r[4] or 0),
                "null_volume_rows": int(r[5] or 0),
            }
    return {"ok": True, "symbols": symbols}


def query_latest_bar_per_symbol(
    conn: Any,
    *,
    lookback_days: int = 90,
    symbols: list[str] | None = None,
) -> dict[str, Any]:
    """Latest bar_date + close per symbol from market.stock_daily.

    Used by readiness snapshot vendor gap CTE.
    """
    if not table_exists(conn, "market", "stock_daily"):
        return {"ok": True, "symbols": {}}

    if symbols:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (UPPER(TRIM(symbol)))
                    UPPER(TRIM(symbol)) AS symbol,
                    bar_date::text AS bar_date,
                    close
                FROM market.stock_daily
                WHERE UPPER(TRIM(symbol)) = ANY(%s)
                  AND bar_date >= (CURRENT_DATE - %s)::date
                ORDER BY UPPER(TRIM(symbol)), bar_date DESC NULLS LAST
                """,
                (symbols, lookback_days),
            )
            raw = cur.fetchall() or []
    else:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (UPPER(TRIM(symbol)))
                    UPPER(TRIM(symbol)) AS symbol,
                    bar_date::text AS bar_date,
                    close
                FROM market.stock_daily
                WHERE bar_date >= (CURRENT_DATE - %s)::date
                ORDER BY UPPER(TRIM(symbol)), bar_date DESC NULLS LAST
                """,
                (lookback_days,),
            )
            raw = cur.fetchall() or []

    out: dict[str, dict[str, Any]] = {}
    for r in raw:
        if hasattr(r, "keys"):
            sym = str(r["symbol"])
            out[sym] = {"bar_date": r["bar_date"], "close": r["close"]}
        else:
            sym = str(r[0])
            out[sym] = {"bar_date": str(r[1]) if r[1] else None, "close": r[2]}
    return {"ok": True, "symbols": out}


def query_latest_bar_full_history(
    conn: Any,
    *,
    symbols: list[str],
) -> dict[str, Any]:
    """Latest bar_date + close per symbol with NO lookback limit (cold path).

    Used by readiness snapshot vendor gap for symbols where the 90-day probe
    returned nothing.
    """
    if not symbols or not table_exists(conn, "market", "stock_daily"):
        return {"ok": True, "symbols": {}}

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (UPPER(TRIM(symbol)))
                UPPER(TRIM(symbol)) AS symbol,
                bar_date::text AS bar_date,
                close
            FROM market.stock_daily
            WHERE UPPER(TRIM(symbol)) = ANY(%s)
            ORDER BY UPPER(TRIM(symbol)), bar_date DESC NULLS LAST
            """,
            (symbols,),
        )
        raw = cur.fetchall() or []

    out: dict[str, dict[str, Any]] = {}
    for r in raw:
        if hasattr(r, "keys"):
            sym = str(r["symbol"])
            out[sym] = {"bar_date": r["bar_date"], "close": r["close"]}
        else:
            sym = str(r[0])
            out[sym] = {"bar_date": str(r[1]) if r[1] else None, "close": r[2]}
    return {"ok": True, "symbols": out}


def query_financials_coverage_symbols(conn: Any) -> dict[str, Any]:
    """Which symbols exist per report_type in market.stock_financials.

    Returns sets of symbols for income_statement (with quarterly/annual counts),
    balance_sheet, cash_flow_statement, ratios, short_interest, short_volume.
    """
    if not table_exists(conn, "market", "stock_financials"):
        return {
            "ok": True,
            "income_statement": {},
            "balance_sheet": [],
            "cash_flow_statement": [],
            "ratios": [],
            "short_interest": [],
            "short_volume": [],
        }

    result: dict[str, Any] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT UPPER(TRIM(symbol)) AS symbol,
                   COUNT(*) FILTER (WHERE LOWER(period_type) = 'quarterly')::integer AS q_count,
                   COUNT(*) FILTER (WHERE LOWER(period_type) = 'annual')::integer AS a_count
            FROM market.stock_financials
            WHERE report_type = 'income_statement'
            GROUP BY UPPER(TRIM(symbol))
            """
        )
        inc: dict[str, dict[str, int]] = {}
        for r in cur.fetchall() or []:
            sym = str(r[0]) if not hasattr(r, "keys") else str(r["symbol"])
            q = int(r[1] if not hasattr(r, "keys") else r["q_count"])
            a = int(r[2] if not hasattr(r, "keys") else r["a_count"])
            inc[sym] = {"q_count": q, "a_count": a}
        result["income_statement"] = inc

        for rtype in ("balance_sheet", "cash_flow_statement", "ratios", "short_interest", "short_volume"):
            cur.execute(
                """
                SELECT DISTINCT UPPER(TRIM(symbol)) AS symbol
                FROM market.stock_financials
                WHERE report_type = %s
                """,
                (rtype,),
            )
            syms = [str(r[0] if not hasattr(r, "keys") else r["symbol"]) for r in (cur.fetchall() or [])]
            result[rtype] = syms

    result["ok"] = True
    return result


def query_financials_fill_rate(
    conn: Any,
    *,
    universe_symbols: list[str] | None = None,
) -> dict[str, Any]:
    """Fill-rate counts for financial jsonb keys.

    Used by readiness_snapshot.compute_data_inventory_stats.
    """
    if not table_exists(conn, "market", "stock_financials"):
        return {"ok": True, "tables": {}}

    tables_spec: dict[str, tuple[str, list[str]]] = {
        "stock_ratios": (
            "ratios",
            [
                "return_on_equity", "price_to_earnings", "debt_to_equity",
                "price_to_book", "price_to_sales", "return_on_assets",
                "market_cap", "free_cash_flow", "price_to_free_cash_flow",
                "ev_to_ebitda", "ev_to_sales", "enterprise_value",
            ],
        ),
        "stock_balance_sheets": (
            "balance_sheet",
            [
                "total_equity", "long_term_debt_and_capital_lease_obligations",
                "cash_and_equivalents", "total_current_assets", "total_current_liabilities",
                "total_assets", "total_liabilities", "retained_earnings_deficit",
                "goodwill", "intangible_assets_net",
            ],
        ),
        "stock_cash_flows": (
            "cash_flow_statement",
            [
                "net_cash_from_operating_activities",
                "purchase_of_property_plant_and_equipment",
                "net_cash_from_investing_activities",
                "net_cash_from_financing_activities",
                "cash_from_operating_activities_continuing_operations",
            ],
        ),
        "stock_income_statements": (
            "income_statement",
            [
                "gross_profit", "operating_income", "ebitda",
                "cost_of_revenue", "research_development", "selling_general_administrative",
                "diluted_earnings_per_share",
            ],
        ),
        "stock_short_interest": (
            "short_interest",
            ["short_interest", "days_to_cover", "avg_daily_volume"],
        ),
        "stock_short_volume": (
            "short_volume",
            ["short_volume_ratio", "total_volume", "short_volume"],
        ),
    }

    result: dict[str, dict[str, int]] = {}
    with conn.cursor() as cur:
        for alias, (report_type, columns) in tables_spec.items():
            agg_parts = ", ".join(
                f"COUNT(DISTINCT t.symbol) FILTER ("
                f"WHERE (t.data ? '{col}') AND NULLIF(t.data->>'{col}', '') IS NOT NULL"
                f") AS {col}"
                for col in columns
            )
            universe_filter = ""
            params: list[Any] = [report_type]
            if universe_symbols:
                universe_filter = "AND UPPER(TRIM(t.symbol)) = ANY(%s)"
                params.append(universe_symbols)

            try:
                cur.execute(
                    f"""
                    SELECT {agg_parts}
                    FROM market.stock_financials t
                    WHERE t.report_type = %s {universe_filter}
                    """,
                    tuple(params),
                )
                row = cur.fetchone()
                if row and hasattr(row, "keys"):
                    result[alias] = {col: int(row.get(col) or 0) for col in columns}
                elif row:
                    result[alias] = {columns[i]: int(row[i] or 0) for i in range(len(columns))}
                else:
                    result[alias] = {col: 0 for col in columns}
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                result[alias] = {col: 0 for col in columns}

    return {"ok": True, "tables": result}


def query_date_coverage(
    conn: Any,
    *,
    days_back: int = 420,
    min_symbol_threshold: int = 1000,
) -> dict[str, Any]:
    """Dates with fewer than `min_symbol_threshold` distinct symbols in stock_daily.

    Used by readiness_snapshot.get_sepa_grouped_backfill_dates.
    """
    if not table_exists(conn, "market", "stock_daily"):
        return {"ok": True, "low_coverage_dates": [], "count": 0}

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                bar_date::text AS dt,
                COUNT(DISTINCT UPPER(TRIM(symbol)))::int AS symbol_count
            FROM market.stock_daily
            WHERE bar_date >= (CURRENT_DATE - %s)::date
              AND bar_date <= (CURRENT_DATE - 1)::date
            GROUP BY bar_date
            HAVING COUNT(DISTINCT UPPER(TRIM(symbol))) < %s
            ORDER BY bar_date
            """,
            (days_back, min_symbol_threshold),
        )
        raw = cur.fetchall() or []

    dates: list[dict[str, Any]] = []
    for r in raw:
        if hasattr(r, "keys"):
            dates.append({"date": r["dt"], "symbol_count": r["symbol_count"]})
        else:
            dates.append({"date": str(r[0]), "symbol_count": int(r[1] or 0)})

    return {"ok": True, "low_coverage_dates": dates, "count": len(dates)}


def query_financials_by_instrument_type(
    conn: Any,
    *,
    universe_symbols_by_type: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Distinct symbols per instrument type per report_type.

    Used by readiness_snapshot._fetch_fundamentals_symbol_counts_by_instrument_type.
    """
    if not table_exists(conn, "market", "stock_financials"):
        return {"ok": True, "by_type": []}

    specs = [
        ("income_statement_symbols", "income_statement"),
        ("balance_sheet_symbols", "balance_sheet"),
        ("cash_flow_symbols", "cash_flow_statement"),
        ("ratio_symbols", "ratios"),
    ]

    result: dict[str, int] = {}
    with conn.cursor() as cur:
        for col, rtype in specs:
            cur.execute(
                "SELECT COUNT(DISTINCT UPPER(TRIM(symbol)))::bigint FROM market.stock_financials WHERE report_type = %s",
                (rtype,),
            )
            row = cur.fetchone()
            result[col] = int(row[0] if row and not hasattr(row, "keys") else (row.get(col, 0) if row else 0))

    return {"ok": True, "counts": result}


# ---------------------------------------------------------------------------
# HTTP routes
# ---------------------------------------------------------------------------


@router.get("/bar-aggregate")
def readiness_bar_aggregate(
    window_days: int = Query(420, ge=1, le=800),
) -> dict[str, Any]:
    """Per-symbol stock_daily aggregate stats within a date window."""
    conn = require_db()
    try:
        return query_bar_aggregate(conn, window_days=window_days)
    finally:
        conn.close()


@router.get("/latest-bar-per-symbol")
def readiness_latest_bar(
    lookback_days: int = Query(90, ge=1, le=800),
    symbols: str | None = Query(None, description="Comma-separated symbols (optional)"),
) -> dict[str, Any]:
    """Latest bar_date + close per symbol from stock_daily."""
    parsed = [s.strip().upper() for s in symbols.split(",") if s.strip()] if symbols else None
    conn = require_db()
    try:
        return query_latest_bar_per_symbol(conn, lookback_days=lookback_days, symbols=parsed)
    finally:
        conn.close()


@router.get("/latest-bar-full-history")
def readiness_latest_bar_full(
    symbols: str = Query(..., description="Comma-separated symbols"),
) -> dict[str, Any]:
    """Latest bar_date + close per symbol (no lookback limit)."""
    parsed = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not parsed:
        return {"ok": True, "symbols": {}}
    conn = require_db()
    try:
        return query_latest_bar_full_history(conn, symbols=parsed)
    finally:
        conn.close()


@router.get("/financials-coverage-symbols")
def readiness_financials_coverage() -> dict[str, Any]:
    """Which symbols exist per report_type in stock_financials."""
    conn = require_db()
    try:
        return query_financials_coverage_symbols(conn)
    finally:
        conn.close()


@router.get("/financials-fill-rate")
def readiness_financials_fill_rate(
    universe_symbols: str | None = Query(None, description="Comma-separated universe symbols"),
) -> dict[str, Any]:
    """Fill-rate counts for financial jsonb keys."""
    parsed = [s.strip().upper() for s in universe_symbols.split(",") if s.strip()] if universe_symbols else None
    conn = require_db()
    try:
        return query_financials_fill_rate(conn, universe_symbols=parsed)
    finally:
        conn.close()


@router.get("/date-coverage")
def readiness_date_coverage(
    days_back: int = Query(420, ge=1, le=800),
    min_symbols: int = Query(1000, ge=1),
) -> dict[str, Any]:
    """Dates with low symbol coverage in stock_daily."""
    conn = require_db()
    try:
        return query_date_coverage(conn, days_back=days_back, min_symbol_threshold=min_symbols)
    finally:
        conn.close()


@router.get("/financials-by-instrument-type")
def readiness_financials_by_instrument_type() -> dict[str, Any]:
    """Distinct symbol counts per report_type in stock_financials."""
    conn = require_db()
    try:
        return query_financials_by_instrument_type(conn)
    finally:
        conn.close()
