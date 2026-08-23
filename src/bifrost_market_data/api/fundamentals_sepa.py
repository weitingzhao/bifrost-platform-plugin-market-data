"""SEPA financial aggregate endpoints (W2-P1).

Serves pre-structured financial data so Trade API ``financials_data.py``
can replace its ~33 direct SQL queries with HTTP calls.  The jsonb
``data`` column is returned verbatim; field unpacking stays in Trade.

All routes live under ``/stocks/fundamentals/sepa/`` (mounted with the
``/market`` prefix by ``app.py``).
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from bifrost_market_data.api.deps import (
    iso_value,
    normalize_symbol,
    require_db,
    table_exists,
)

router = APIRouter(prefix="/stocks/fundamentals/sepa", tags=["fundamentals-sepa"])

_VALID_REPORT_TYPES = frozenset({
    "income_statement",
    "balance_sheet",
    "cash_flow_statement",
    "ratios",
    "short_interest",
    "short_volume",
})

_VALID_PERIOD_TYPES = frozenset({"quarterly", "annual"})


def _parse_symbols(raw: str) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for part in raw.split(","):
        sym = normalize_symbol(part)
        if sym and sym not in seen:
            seen.add(sym)
            result.append(sym)
    return result


def _date_str(val: Any) -> str | None:
    if val is None:
        return None
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return str(val)


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


def query_financials_batch(
    conn: Any,
    *,
    symbols: list[str],
    report_type: str,
    period_type: str | None = None,
    limit_per_symbol: int = 20,
) -> dict[str, list[dict[str, Any]]]:
    """Batch-read financials rows grouped by symbol, returning raw jsonb data."""
    if not table_exists(conn, "market", "stock_financials"):
        return {}

    clauses = ["UPPER(TRIM(symbol)) = ANY(%s)", "report_type = %s"]
    params: list[Any] = [symbols, report_type]

    if period_type:
        clauses.append("lower(period_type) = %s")
        params.append(period_type.lower())

    params.append(limit_per_symbol)

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT * FROM (
                SELECT
                    UPPER(TRIM(symbol)) AS symbol,
                    report_type,
                    period_date,
                    period_type,
                    fiscal_year,
                    fiscal_quarter,
                    data,
                    fetched_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY UPPER(TRIM(symbol))
                        ORDER BY period_date DESC
                    ) AS rn
                FROM raw_market.stock_financials
                WHERE {" AND ".join(clauses)}
            ) ranked
            WHERE rn <= %s
            ORDER BY symbol, period_date ASC
            """,
            params,
        )
        rows = cur.fetchall() or []

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        rec = _financials_row(r)
        grouped[rec["symbol"]].append(rec)
    return dict(grouped)


def _financials_row(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return {
            "symbol": str(row.get("symbol") or ""),
            "report_type": str(row.get("report_type") or ""),
            "period_date": _date_str(row.get("period_date")),
            "period_type": str(row.get("period_type") or ""),
            "fiscal_year": row.get("fiscal_year"),
            "fiscal_quarter": row.get("fiscal_quarter"),
            "data": row.get("data"),
            "fetched_at": iso_value(row.get("fetched_at")),
        }
    return {
        "symbol": str(row[0] or ""),
        "report_type": str(row[1] or ""),
        "period_date": _date_str(row[2]),
        "period_type": str(row[3] or ""),
        "fiscal_year": row[4],
        "fiscal_quarter": row[5],
        "data": row[6],
        "fetched_at": iso_value(row[7]),
    }


def query_income_rows_for_sepa(
    conn: Any,
    *,
    symbol: str,
) -> dict[str, list[dict[str, Any]]]:
    """Quarterly + annual income statement rows for a single symbol.

    Mirrors ``fetch_income_rows_for_sepa_from_pg`` — returns both
    ``quarterly`` and ``annual`` lists with fiscal metadata + raw data.
    """
    if not table_exists(conn, "market", "stock_financials"):
        return {"quarterly": [], "annual": []}

    out: dict[str, list[dict[str, Any]]] = {"quarterly": [], "annual": []}
    with conn.cursor() as cur:
        for pt in ("quarterly", "annual"):
            order = (
                "fiscal_year ASC NULLS LAST, fiscal_quarter ASC NULLS LAST, period_date ASC"
                if pt == "quarterly"
                else "fiscal_year ASC NULLS LAST, period_date ASC"
            )
            cur.execute(
                f"""
                SELECT
                    period_type AS timeframe,
                    fiscal_year,
                    fiscal_quarter,
                    period_date AS period_end,
                    data
                FROM raw_market.stock_financials
                WHERE UPPER(TRIM(symbol)) = %s
                  AND report_type = 'income_statement'
                  AND lower(period_type) = %s
                ORDER BY {order}
                """,
                (symbol, pt),
            )
            for r in cur.fetchall() or []:
                rec = _income_sepa_row(r)
                out[pt].append(rec)
    return out


def _income_sepa_row(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return {
            "timeframe": str(row.get("timeframe") or ""),
            "fiscal_year": row.get("fiscal_year"),
            "fiscal_quarter": row.get("fiscal_quarter"),
            "period_end": _date_str(row.get("period_end")),
            "data": row.get("data"),
        }
    return {
        "timeframe": str(row[0] or ""),
        "fiscal_year": row[1],
        "fiscal_quarter": row[2],
        "period_end": _date_str(row[3]),
        "data": row[4],
    }


def query_financials_ext_batch(
    conn: Any,
    *,
    symbols: list[str],
    report_type: str,
    period_type: str = "quarterly",
    max_rows_per_symbol: int | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Batch-read financials rows for ext evaluators.

    Covers income-ext, balance-sheet-ext, cash-flow-ext patterns.
    Returns symbol -> list of dicts (ascending period_end) with raw data.
    """
    if not table_exists(conn, "market", "stock_financials"):
        return {}

    if max_rows_per_symbol is not None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM (
                    SELECT
                        UPPER(TRIM(symbol)) AS symbol,
                        fiscal_year,
                        fiscal_quarter,
                        period_date AS period_end,
                        data,
                        ROW_NUMBER() OVER (
                            PARTITION BY UPPER(TRIM(symbol))
                            ORDER BY period_date DESC
                        ) AS rn
                    FROM raw_market.stock_financials
                    WHERE UPPER(TRIM(symbol)) = ANY(%s)
                      AND report_type = %s
                      AND lower(period_type) = %s
                ) ranked
                WHERE rn <= %s
                ORDER BY symbol, period_end ASC
                """,
                (symbols, report_type, period_type.lower(), max_rows_per_symbol),
            )
            rows = cur.fetchall() or []
    else:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    UPPER(TRIM(symbol)) AS symbol,
                    fiscal_year,
                    fiscal_quarter,
                    period_date AS period_end,
                    data
                FROM raw_market.stock_financials
                WHERE UPPER(TRIM(symbol)) = ANY(%s)
                  AND report_type = %s
                  AND lower(period_type) = %s
                ORDER BY symbol, period_end ASC
                """,
                (symbols, report_type, period_type.lower()),
            )
            rows = cur.fetchall() or []

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        d = dict(r) if isinstance(r, dict) else {
            "symbol": r[0], "fiscal_year": r[1], "fiscal_quarter": r[2],
            "period_end": r[3], "data": r[4],
        }
        d.pop("rn", None)
        d["period_end"] = _date_str(d.get("period_end"))
        grouped[str(d["symbol"])].append(d)
    return dict(grouped)


def query_ratios_latest_batch(
    conn: Any,
    *,
    symbols: list[str],
) -> dict[str, dict[str, Any]]:
    """Latest ratios row per symbol (DISTINCT ON)."""
    if not table_exists(conn, "market", "stock_financials"):
        return {}

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (UPPER(TRIM(symbol)))
                UPPER(TRIM(symbol)) AS symbol,
                period_date AS date,
                data
            FROM raw_market.stock_financials
            WHERE UPPER(TRIM(symbol)) = ANY(%s)
              AND report_type = 'ratios'
            ORDER BY UPPER(TRIM(symbol)), period_date DESC
            """,
            (symbols,),
        )
        rows = cur.fetchall() or []

    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        if isinstance(r, dict):
            sym = str(r.get("symbol") or "")
            out[sym] = {"symbol": sym, "date": _date_str(r.get("date")), "data": r.get("data")}
        else:
            sym = str(r[0] or "")
            out[sym] = {"symbol": sym, "date": _date_str(r[1]), "data": r[2]}
    return out


def query_short_interest_latest_batch(
    conn: Any,
    *,
    symbols: list[str],
    max_rows: int = 2,
) -> dict[str, list[dict[str, Any]]]:
    """Latest N short-interest rows per symbol."""
    if not table_exists(conn, "market", "stock_financials"):
        return {}

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT * FROM (
                SELECT
                    UPPER(TRIM(symbol)) AS symbol,
                    period_date AS settlement_date,
                    data,
                    ROW_NUMBER() OVER (
                        PARTITION BY UPPER(TRIM(symbol))
                        ORDER BY period_date DESC
                    ) AS rn
                FROM raw_market.stock_financials
                WHERE UPPER(TRIM(symbol)) = ANY(%s)
                  AND report_type = 'short_interest'
            ) ranked
            WHERE rn <= %s
            ORDER BY symbol, settlement_date ASC
            """,
            (symbols, max_rows),
        )
        rows = cur.fetchall() or []

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        if isinstance(r, dict):
            sym = str(r.get("symbol") or "")
            grouped[sym].append({
                "symbol": sym,
                "settlement_date": _date_str(r.get("settlement_date")),
                "data": r.get("data"),
            })
        else:
            sym = str(r[0] or "")
            grouped[sym].append({
                "symbol": sym,
                "settlement_date": _date_str(r[1]),
                "data": r[2],
            })
    return dict(grouped)


def query_short_volume_recent_batch(
    conn: Any,
    *,
    symbols: list[str],
    max_days: int = 10,
) -> dict[str, list[dict[str, Any]]]:
    """Latest N short-volume rows per symbol."""
    if not table_exists(conn, "market", "stock_financials"):
        return {}

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT * FROM (
                SELECT
                    UPPER(TRIM(symbol)) AS symbol,
                    period_date AS trade_date,
                    data,
                    ROW_NUMBER() OVER (
                        PARTITION BY UPPER(TRIM(symbol))
                        ORDER BY period_date DESC
                    ) AS rn
                FROM raw_market.stock_financials
                WHERE UPPER(TRIM(symbol)) = ANY(%s)
                  AND report_type = 'short_volume'
            ) ranked
            WHERE rn <= %s
            ORDER BY symbol, trade_date ASC
            """,
            (symbols, max_days),
        )
        rows = cur.fetchall() or []

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        if isinstance(r, dict):
            sym = str(r.get("symbol") or "")
            grouped[sym].append({
                "symbol": sym,
                "trade_date": _date_str(r.get("trade_date")),
                "data": r.get("data"),
            })
        else:
            sym = str(r[0] or "")
            grouped[sym].append({
                "symbol": sym,
                "trade_date": _date_str(r[1]),
                "data": r[2],
            })
    return dict(grouped)


def query_gaps(
    conn: Any,
    *,
    report_type: str,
    limit: int = 2000,
) -> dict[str, Any]:
    """Coverage gap analysis for a given report type.

    Checks which universe symbols are missing or have insufficient data.
    Returns ``{"count": N, "symbols": [...]}``.
    """
    if not table_exists(conn, "market", "stock_financials"):
        return {"count": 0, "symbols": []}
    if not _view_or_table_exists(conn, "market", "v_us_equity_universe"):
        return {"count": 0, "symbols": [], "note": "v_us_equity_universe view not found"}

    gap_sql = _GAP_SQLS.get(report_type)
    if gap_sql is None:
        return {"count": 0, "symbols": [], "error": f"unsupported report_type: {report_type}"}

    with conn.cursor() as cur:
        cur.execute(gap_sql, (limit,))
        rows = cur.fetchall() or []

    syms: list[str] = []
    for r in rows:
        s = str(r["symbol"] if isinstance(r, dict) else r[0]).strip().upper()
        if s:
            syms.append(s)
    return {"count": len(syms), "symbols": syms}


def _view_or_table_exists(conn: Any, schema: str, name: str) -> bool:
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass(%s) IS NOT NULL AS ok",
                (f"{schema}.{name}",),
            )
            row = cur.fetchone()
            if row is None:
                return False
            return bool(row["ok"] if isinstance(row, dict) else row[0])
    except Exception:
        return False


_INSTRUMENT_TYPES = "('CS', 'ADRC', 'PFD')"

_GAP_SQLS: dict[str, str] = {
    "income_statement": f"""
        WITH u AS (
            SELECT symbol FROM raw_market.v_us_equity_universe
            WHERE upper(coalesce(instrument_type, '')) IN {_INSTRUMENT_TYPES}
        ),
        q AS (
            SELECT symbol, count(*)::int AS n
            FROM raw_market.stock_financials
            WHERE report_type = 'income_statement' AND lower(period_type) = 'quarterly'
            GROUP BY symbol
        ),
        a AS (
            SELECT symbol, count(*)::int AS n
            FROM raw_market.stock_financials
            WHERE report_type = 'income_statement' AND lower(period_type) = 'annual'
            GROUP BY symbol
        )
        SELECT u.symbol FROM u
        LEFT JOIN q ON q.symbol = u.symbol
        LEFT JOIN a ON a.symbol = u.symbol
        WHERE q.symbol IS NULL OR q.n < 5 OR a.symbol IS NULL OR a.n < 4
        ORDER BY u.symbol LIMIT %s
    """,
    "balance_sheet": f"""
        WITH u AS (
            SELECT symbol FROM raw_market.v_us_equity_universe
            WHERE upper(coalesce(instrument_type, '')) IN {_INSTRUMENT_TYPES}
        ),
        q AS (
            SELECT symbol, count(*)::int AS n
            FROM raw_market.stock_financials
            WHERE report_type = 'balance_sheet' AND lower(period_type) = 'quarterly'
            GROUP BY symbol
        )
        SELECT u.symbol FROM u
        LEFT JOIN q ON q.symbol = u.symbol
        WHERE q.symbol IS NULL OR q.n < 4
        ORDER BY u.symbol LIMIT %s
    """,
    "cash_flow_statement": f"""
        WITH u AS (
            SELECT symbol FROM raw_market.v_us_equity_universe
            WHERE upper(coalesce(instrument_type, '')) IN {_INSTRUMENT_TYPES}
        ),
        q AS (
            SELECT symbol, count(*)::int AS n
            FROM raw_market.stock_financials
            WHERE report_type = 'cash_flow_statement' AND lower(period_type) = 'quarterly'
            GROUP BY symbol
        )
        SELECT u.symbol FROM u
        LEFT JOIN q ON q.symbol = u.symbol
        WHERE q.symbol IS NULL OR q.n < 4
        ORDER BY u.symbol LIMIT %s
    """,
    "ratios": f"""
        WITH u AS (
            SELECT symbol FROM raw_market.v_us_equity_universe
            WHERE upper(coalesce(instrument_type, '')) IN {_INSTRUMENT_TYPES}
        ),
        q AS (
            SELECT symbol, count(*)::int AS n, max(period_date) AS mx
            FROM raw_market.stock_financials
            WHERE report_type = 'ratios'
            GROUP BY symbol
        )
        SELECT u.symbol FROM u
        LEFT JOIN q ON q.symbol = u.symbol
        WHERE q.symbol IS NULL OR q.n < 1 OR q.mx < (CURRENT_DATE - 45)
        ORDER BY u.symbol LIMIT %s
    """,
    "short_interest": """
        WITH u AS (SELECT symbol FROM raw_market.v_us_equity_universe),
        h AS (
            SELECT symbol, count(*)::int AS n, max(period_date) AS mx
            FROM raw_market.stock_financials
            WHERE report_type = 'short_interest'
            GROUP BY symbol
        )
        SELECT u.symbol FROM u
        LEFT JOIN h ON h.symbol = u.symbol
        WHERE h.symbol IS NULL OR h.n < 1 OR h.mx < (CURRENT_DATE - 45)
        ORDER BY u.symbol LIMIT %s
    """,
    "short_volume": """
        WITH u AS (SELECT symbol FROM raw_market.v_us_equity_universe),
        d AS (
            SELECT symbol, count(*)::int AS n, max(period_date) AS mx
            FROM raw_market.stock_financials
            WHERE report_type = 'short_volume'
            GROUP BY symbol
        )
        SELECT u.symbol FROM u
        LEFT JOIN d ON d.symbol = u.symbol
        WHERE d.symbol IS NULL OR d.n < 5 OR d.mx < (CURRENT_DATE - 14)
        ORDER BY u.symbol LIMIT %s
    """,
}


# ---------------------------------------------------------------------------
# HTTP routes
# ---------------------------------------------------------------------------


@router.get("/financials")
def sepa_financials_batch(
    symbols: str = Query(..., description="Comma-separated stock symbols"),
    report_type: str = Query(..., description="Report type (income_statement, balance_sheet, etc.)"),
    period_type: str | None = Query(None, description="Filter by period_type (quarterly/annual)"),
    limit: int = Query(20, ge=1, le=200, description="Max rows per symbol"),
) -> dict[str, Any]:
    """Batch financials with raw jsonb data for SEPA evaluation."""
    parsed = _parse_symbols(symbols)
    if not parsed:
        raise HTTPException(status_code=400, detail="No valid symbols provided")
    if report_type not in _VALID_REPORT_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid report_type: {report_type}. Valid: {sorted(_VALID_REPORT_TYPES)}",
        )
    if period_type and period_type not in _VALID_PERIOD_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period_type: {period_type}. Valid: {sorted(_VALID_PERIOD_TYPES)}",
        )
    conn = require_db()
    try:
        data = query_financials_batch(
            conn,
            symbols=parsed,
            report_type=report_type,
            period_type=period_type,
            limit_per_symbol=limit,
        )
        total = sum(len(v) for v in data.values())
        return {"ok": True, "data": data, "count": total}
    finally:
        conn.close()


@router.get("/income-rows")
def sepa_income_rows(
    symbol: str = Query(..., description="Single stock symbol"),
) -> dict[str, Any]:
    """Quarterly + annual income statement rows for SEPA ``evaluate_fundamentals``.

    Returns ``{"quarterly": [...], "annual": [...]}`` with raw jsonb data.
    """
    sym = normalize_symbol(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="No valid symbol provided")
    conn = require_db()
    try:
        result = query_income_rows_for_sepa(conn, symbol=sym)
        return {
            "ok": True,
            "symbol": sym,
            "quarterly_count": len(result["quarterly"]),
            "annual_count": len(result["annual"]),
            **result,
        }
    finally:
        conn.close()


@router.get("/income-ext")
def sepa_income_ext_batch(
    symbols: str = Query(..., description="Comma-separated stock symbols"),
) -> dict[str, Any]:
    """Batch quarterly income-statement rows for ext evaluators."""
    parsed = _parse_symbols(symbols)
    if not parsed:
        raise HTTPException(status_code=400, detail="No valid symbols provided")
    conn = require_db()
    try:
        data = query_financials_ext_batch(
            conn, symbols=parsed, report_type="income_statement",
        )
        total = sum(len(v) for v in data.values())
        return {"ok": True, "data": data, "count": total}
    finally:
        conn.close()


@router.get("/balance-sheet-ext")
def sepa_balance_sheet_ext_batch(
    symbols: str = Query(..., description="Comma-separated stock symbols"),
    max_quarters: int = Query(6, ge=1, le=40, description="Max quarterly rows per symbol"),
) -> dict[str, Any]:
    """Batch quarterly balance-sheet rows (latest N) for ext evaluators."""
    parsed = _parse_symbols(symbols)
    if not parsed:
        raise HTTPException(status_code=400, detail="No valid symbols provided")
    conn = require_db()
    try:
        data = query_financials_ext_batch(
            conn,
            symbols=parsed,
            report_type="balance_sheet",
            max_rows_per_symbol=max_quarters,
        )
        total = sum(len(v) for v in data.values())
        return {"ok": True, "data": data, "count": total}
    finally:
        conn.close()


@router.get("/cash-flow-ext")
def sepa_cash_flow_ext_batch(
    symbols: str = Query(..., description="Comma-separated stock symbols"),
    max_quarters: int = Query(6, ge=1, le=40, description="Max quarterly rows per symbol"),
) -> dict[str, Any]:
    """Batch quarterly cash-flow rows (latest N) for ext evaluators."""
    parsed = _parse_symbols(symbols)
    if not parsed:
        raise HTTPException(status_code=400, detail="No valid symbols provided")
    conn = require_db()
    try:
        data = query_financials_ext_batch(
            conn,
            symbols=parsed,
            report_type="cash_flow_statement",
            max_rows_per_symbol=max_quarters,
        )
        total = sum(len(v) for v in data.values())
        return {"ok": True, "data": data, "count": total}
    finally:
        conn.close()


@router.get("/ratios-latest")
def sepa_ratios_latest_batch(
    symbols: str = Query(..., description="Comma-separated stock symbols"),
) -> dict[str, Any]:
    """Latest ratios row per symbol (DISTINCT ON period_date DESC)."""
    parsed = _parse_symbols(symbols)
    if not parsed:
        raise HTTPException(status_code=400, detail="No valid symbols provided")
    conn = require_db()
    try:
        data = query_ratios_latest_batch(conn, symbols=parsed)
        return {"ok": True, "data": data, "count": len(data)}
    finally:
        conn.close()


@router.get("/short-interest-latest")
def sepa_short_interest_latest_batch(
    symbols: str = Query(..., description="Comma-separated stock symbols"),
    max_rows: int = Query(2, ge=1, le=50, description="Max rows per symbol"),
) -> dict[str, Any]:
    """Latest N short-interest rows per symbol with raw jsonb data."""
    parsed = _parse_symbols(symbols)
    if not parsed:
        raise HTTPException(status_code=400, detail="No valid symbols provided")
    conn = require_db()
    try:
        data = query_short_interest_latest_batch(conn, symbols=parsed, max_rows=max_rows)
        total = sum(len(v) for v in data.values())
        return {"ok": True, "data": data, "count": total}
    finally:
        conn.close()


@router.get("/short-volume-recent")
def sepa_short_volume_recent_batch(
    symbols: str = Query(..., description="Comma-separated stock symbols"),
    max_days: int = Query(10, ge=1, le=200, description="Max rows per symbol"),
) -> dict[str, Any]:
    """Latest N short-volume rows per symbol with raw jsonb data."""
    parsed = _parse_symbols(symbols)
    if not parsed:
        raise HTTPException(status_code=400, detail="No valid symbols provided")
    conn = require_db()
    try:
        data = query_short_volume_recent_batch(conn, symbols=parsed, max_days=max_days)
        total = sum(len(v) for v in data.values())
        return {"ok": True, "data": data, "count": total}
    finally:
        conn.close()


@router.get("/gaps")
def sepa_gaps(
    report_type: str = Query(..., description="Report type to check gaps for"),
    limit: int = Query(2000, ge=1, le=5000, description="Max symbols to return"),
) -> dict[str, Any]:
    """Coverage gap analysis: which universe symbols lack sufficient data."""
    if report_type not in _VALID_REPORT_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid report_type: {report_type}. Valid: {sorted(_VALID_REPORT_TYPES)}",
        )
    conn = require_db()
    try:
        result = query_gaps(conn, report_type=report_type, limit=limit)
        return {"ok": True, **result}
    finally:
        conn.close()
