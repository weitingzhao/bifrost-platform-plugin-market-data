"""Shared upsert helpers for Wave 8 split financials entity tables."""

from __future__ import annotations

import json
import os
from typing import Any

_REPORT_TYPE_TO_TABLE: dict[str, str] = {
    "income_statement": "income_statement",
    "balance_sheet": "balance_sheet",
    "cash_flow_statement": "cash_flow",
    "ratios": "ratios",
    "short_interest": "short_interest",
    "short_volume": "short_volume",
}


def split_financials_writes_enabled() -> bool:
    """Kill-switch INGEST_DUAL_WRITE_FINANCIALS=0 skips entity-table writes."""
    return os.environ.get("INGEST_DUAL_WRITE_FINANCIALS", "1").strip().lower() not in (
        "0",
        "false",
        "no",
    )


def upsert_financials_rows(conn: Any, rows: list[tuple[Any, ...]]) -> int:
    """Upsert rows keyed by (symbol, report_type, period_date, period_type, ...).

    Row tuple: (symbol, report_type, period_date, period_type, fiscal_year,
    fiscal_quarter, data, filing_date). filing_date is last so the existing
    positional indexes stay put.
    """
    if not rows or not split_financials_writes_enabled():
        return 0

    by_table: dict[str, list[tuple[Any, ...]]] = {}
    for row in rows:
        report_type = str(row[1])
        table = _REPORT_TYPE_TO_TABLE.get(report_type)
        if not table:
            continue
        by_table.setdefault(table, []).append(row)

    total = 0
    for table, table_rows in by_table.items():
        total += _upsert_entity_table(conn, table, table_rows)
    return total


def replace_symbol_statement(
    conn: Any, symbol: str, report_type: str, rows: list[tuple[Any, ...]]
) -> tuple[int, int]:
    """Make one statement's rows for ``symbol`` exactly ``rows``, in one transaction.

    For a source that answers with the symbol's whole history: periods it no
    longer reports are deleted, so a series never holds rows from two sources
    or two companies side by side. Only the period types ``rows`` covers are
    touched: no answer is not a reason to drop history, so a symbol the source
    has annual rows for and no quarters (a BDC, 2026-09-29) keeps its
    quarterly rows. Returns (upserted, deleted).
    """
    table = _REPORT_TYPE_TO_TABLE[report_type]
    if not rows or not split_financials_writes_enabled():
        return 0, 0
    prepared = _prepare(rows)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                DELETE FROM raw_market.{table} AS t
                WHERE t.symbol = %s
                  AND t.period_type = ANY(%s::text[])
                  AND NOT EXISTS (
                      SELECT 1
                      FROM unnest(%s::date[], %s::text[]) AS k(period_date, period_type)
                      WHERE k.period_date = t.period_date AND k.period_type = t.period_type
                  )
                """,
                (
                    symbol,
                    sorted({r[2] for r in prepared}),
                    [r[1] for r in prepared],
                    [r[2] for r in prepared],
                ),
            )
            deleted = cur.rowcount
            cur.executemany(_upsert_sql(table), prepared)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return len(prepared), deleted


def _upsert_sql(table: str) -> str:
    return f"""
        INSERT INTO raw_market.{table}
            (symbol, period_date, period_type, fiscal_year, fiscal_quarter, data, filing_date)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)
        ON CONFLICT (symbol, period_date, period_type) DO UPDATE SET
            fiscal_year = EXCLUDED.fiscal_year,
            fiscal_quarter = EXCLUDED.fiscal_quarter,
            data = EXCLUDED.data,
            -- COALESCE: a later TTM-shaped fetch must not blank a filing_date
            -- an earlier quarterly row already established.
            filing_date = COALESCE(EXCLUDED.filing_date, raw_market.{table}.filing_date),
            fetched_at = now()
    """


def _prepare(rows: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    prepared: list[tuple[Any, ...]] = []
    for r in rows:
        data_val = r[6]
        if isinstance(data_val, (dict, list)):
            data_val = json.dumps(data_val)
        filing_date = r[7] if len(r) > 7 else None
        prepared.append((r[0], r[2], r[3], r[4], r[5], data_val, filing_date))
    return prepared


def _upsert_entity_table(conn: Any, table: str, rows: list[tuple[Any, ...]]) -> int:
    prepared = _prepare(rows)
    try:
        with conn.cursor() as cur:
            cur.executemany(_upsert_sql(table), prepared)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return len(prepared)
