"""Composite readiness summary — Trade ``SepaReadinessSummaryResponse`` shape.

Single Golden Source authority for Stock Data Readiness Steps 1–9 (+ Step 10
fund_cache counts from ``dw_stock.mart_sepa_fundamental_eval`` when present).
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from fastapi import APIRouter

from bifrost_market_data.api.deps import iso_value, require_db, table_exists
from bifrost_market_data.api.fundamentals_sepa import query_gaps
from bifrost_market_data.api.readiness_data import (
    query_bar_aggregate,
    query_financials_by_instrument_type,
    query_snapshot_coverage,
    query_vendor_gap,
)
from bifrost_market_data.api.source_void import VALID_DATA_TYPES, query_all_voids

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/readiness", tags=["readiness-summary"])

# FE / Trade data_type → Plugin report_type for gaps
_DATA_TYPE_TO_REPORT: dict[str, str] = {
    "income_statements": "income_statement",
    "balance_sheets": "balance_sheet",
    "cash_flows": "cash_flow_statement",
    "ratios": "ratios",
    "short_interest": "short_interest",
    "short_volume": "short_volume",
}

_MIN_BAR_ROWS = 240
_STALE_DAYS = 7


def _universe_count(conn: Any) -> int:
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*)::bigint AS n FROM raw_market.v_us_equity_universe"
            )
            row = cur.fetchone()
        if row is None:
            return 0
        return int(row["n"] if isinstance(row, dict) else row[0] or 0)
    except Exception as exc:
        logger.debug("universe count failed: %s", exc)
        return 0


def _holidays_summary(conn: Any) -> dict[str, Any] | None:
    if not table_exists(conn, "market", "us_market_holiday") and not table_exists(
        conn, "raw_market", "us_market_holiday"
    ):
        # table_exists checks information_schema; raw_market may still have the table
        pass
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    count(*)::bigint AS total,
                    count(*) FILTER (WHERE status = 'early-close')::bigint AS early_close_count,
                    min(holiday_date)::text AS earliest_date,
                    max(holiday_date)::text AS latest_date,
                    max(fetched_at) AS last_fetched_at
                FROM raw_market.us_market_holiday
                """
            )
            hr = cur.fetchone()
            cur.execute(
                """
                SELECT exchange, count(*)::bigint AS cnt
                FROM raw_market.us_market_holiday
                GROUP BY exchange
                ORDER BY exchange
                """
            )
            by_ex = cur.fetchall() or []
        if hr is None:
            return None
        if isinstance(hr, dict):
            total = int(hr.get("total") or 0)
            early = int(hr.get("early_close_count") or 0)
            earliest = hr.get("earliest_date")
            latest = hr.get("latest_date")
            last_fetched = hr.get("last_fetched_at")
        else:
            total = int(hr[0] or 0)
            early = int(hr[1] or 0)
            earliest = hr[2]
            latest = hr[3]
            last_fetched = hr[4]
        by_exchange = []
        for r in by_ex:
            if isinstance(r, dict):
                by_exchange.append(
                    {"exchange": r.get("exchange"), "count": int(r.get("cnt") or 0)}
                )
            else:
                by_exchange.append({"exchange": r[0], "count": int(r[1] or 0)})
        return {
            "total": total,
            "early_close_count": early,
            "earliest_date": earliest,
            "latest_date": latest,
            "last_fetched_at": iso_value(last_fetched) if last_fetched else None,
            "by_exchange": by_exchange,
        }
    except Exception as exc:
        logger.debug("holidays summary failed: %s", exc)
        return None


def _price_readiness(conn: Any) -> dict[str, int]:
    try:
        bar_agg = query_bar_aggregate(conn, window_days=420, summary=False)
        symbols = bar_agg.get("symbols") or {}
        stale_cutoff = date.today() - timedelta(days=_STALE_DAYS)
        ready = 0
        for stats in symbols.values():
            br = int(stats.get("bar_rows") or 0)
            lb = stats.get("last_bar_date")
            if lb and not isinstance(lb, date):
                try:
                    lb = date.fromisoformat(str(lb)[:10])
                except ValueError:
                    lb = None
            nc = int(stats.get("null_close_rows") or 0)
            nv = int(stats.get("null_volume_rows") or 0)
            if br >= _MIN_BAR_ROWS and lb and lb >= stale_cutoff and nc == 0 and nv == 0:
                ready += 1
        return {"total_symbols": len(symbols), "price_ready": ready}
    except Exception as exc:
        logger.warning("price readiness failed: %s", exc)
        return {"total_symbols": 0, "price_ready": 0}


def _fund_cache_counts(conn: Any) -> tuple[int | None, dict[str, Any] | None, bool]:
    """Read dw_stock.mart_sepa_fundamental_eval when present on Golden Source."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass('dw_stock.mart_sepa_fundamental_eval') IS NOT NULL AS ok"
            )
            row = cur.fetchone()
            exists = bool(row["ok"] if isinstance(row, dict) else (row[0] if row else False))
            if not exists:
                return None, None, False
            cur.execute(
                "SELECT count(*)::bigint AS n FROM dw_stock.mart_sepa_fundamental_eval"
            )
            total_row = cur.fetchone()
            total = int(
                (total_row["n"] if isinstance(total_row, dict) else total_row[0]) or 0
            )
            cur.execute(
                """
                SELECT count(*)::bigint AS n FROM dw_stock.mart_sepa_fundamental_eval
                WHERE insufficient_data = false
                """
            )
            ready_row = cur.fetchone()
            ready = int(
                (ready_row["n"] if isinstance(ready_row, dict) else ready_row[0]) or 0
            )
        snap = {
            "rows_total": total,
            "included_in_universe": total,
            "price_ready": ready,
        }
        return ready, snap, total > 0
    except Exception as exc:
        logger.debug("fund_cache / mart_sepa_fundamental_eval failed: %s", exc)
        return None, None, False


def _fundamentals_by_type_for_fe(conn: Any) -> list[dict[str, Any]] | None:
    try:
        raw = query_financials_by_instrument_type(conn)
        if not raw.get("ok"):
            return None
        counts = raw.get("counts") or {}
        if isinstance(counts, dict) and counts:
            return [
                {
                    "code": "ALL",
                    "description": "all",
                    "income_statement_symbols": int(
                        counts.get("income_statement_symbols") or 0
                    ),
                    "balance_sheet_symbols": int(
                        counts.get("balance_sheet_symbols") or 0
                    ),
                    "cash_flow_symbols": int(counts.get("cash_flow_symbols") or 0),
                    "ratio_symbols": int(counts.get("ratio_symbols") or 0),
                }
            ]
        rows = raw.get("by_type") or raw.get("rows")
        if isinstance(rows, list) and rows:
            return rows
        return None
    except Exception as exc:
        logger.debug("fundamentals by type failed: %s", exc)
        return None


def build_readiness_summary(conn: Any) -> dict[str, Any]:
    """Assemble Trade-compatible ``SepaReadinessSummaryResponse``."""
    out: dict[str, Any] = {"ok": True}

    universe = _universe_count(conn)
    out["universe_count"] = universe
    out["tickers_active_count"] = universe
    out["tickers_last_synced_at"] = None

    out["price_readiness_live"] = _price_readiness(conn)

    fund_valid, snap_today, snap_populated = _fund_cache_counts(conn)
    out["fund_cache_valid_count"] = fund_valid
    out["snapshot_populated"] = snap_populated
    out["snapshot_today"] = snap_today or {
        "rows_total": 0,
        "included_in_universe": 0,
        "price_ready": 0,
    }
    out["notes_breakdown"] = []

    try:
        snap_cov = query_snapshot_coverage(conn)
        out["stock_unified_snapshot_row_count"] = snap_cov.get("row_count")
        out["stock_unified_snapshot_last_fetched_at"] = snap_cov.get("last_fetched_at")
        by_type = snap_cov.get("by_instrument_type") or []
        out["stock_unified_snapshot_by_type"] = [
            {
                "code": r.get("code", ""),
                "description": r.get("code", ""),
                "snapshot_row_count": r.get("snapshot_row_count", 0),
                "universe_ticker_count": r.get("universe_ticker_count", 0),
            }
            for r in by_type
        ] or None
    except Exception as exc:
        logger.debug("snapshot coverage failed: %s", exc)
        out["stock_unified_snapshot_row_count"] = None
        out["stock_unified_snapshot_last_fetched_at"] = None
        out["stock_unified_snapshot_by_type"] = None

    out["fundamentals_symbol_count_by_type"] = _fundamentals_by_type_for_fe(conn)

    try:
        vg = query_vendor_gap(conn, detail=False)
        out["stock_day_vendor_fill_gap_count"] = vg.get("gap_count")
    except Exception as exc:
        logger.debug("vendor gap failed: %s", exc)
        out["stock_day_vendor_fill_gap_count"] = None

    for data_type, report_type in _DATA_TYPE_TO_REPORT.items():
        try:
            gap = query_gaps(conn, report_type=report_type, limit=10000)
            out[f"{data_type}_gap_count"] = int(gap.get("count") or 0)
        except Exception as exc:
            logger.debug("gap count %s failed: %s", data_type, exc)
            out[f"{data_type}_gap_count"] = None

    voids = query_all_voids(conn)
    for dt in VALID_DATA_TYPES:
        row = voids.get(dt) or {}
        is_void = bool(row.get("is_void"))
        acked_n = int(row.get("acked_gap_count") or 0) if is_void else 0
        total_n = out.get(f"{dt}_gap_count")
        if is_void and total_n is not None:
            actionable = max(0, int(total_n) - acked_n)
        else:
            actionable = total_n
        out[f"{dt}_source_void"] = is_void
        out[f"{dt}_acked_gap_count"] = acked_n if is_void else None
        out[f"{dt}_actionable_gap_count"] = actionable
        out[f"{dt}_void_reason"] = row.get("note") or row.get("void_reason")

    out["holidays_summary"] = _holidays_summary(conn)
    return out


@router.get("/summary")
def get_readiness_summary() -> dict[str, Any]:
    """Composite readiness summary matching Trade FE contract fields."""
    conn = require_db()
    try:
        return build_readiness_summary(conn)
    finally:
        conn.close()
