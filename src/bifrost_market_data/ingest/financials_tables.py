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
    """Upsert rows keyed by (symbol, report_type, period_date, period_type, ...)."""
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


def _upsert_entity_table(conn: Any, table: str, rows: list[tuple[Any, ...]]) -> int:
    sql = f"""
        INSERT INTO raw_market.{table}
            (symbol, period_date, period_type, fiscal_year, fiscal_quarter, data)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (symbol, period_date, period_type) DO UPDATE SET
            fiscal_year = EXCLUDED.fiscal_year,
            fiscal_quarter = EXCLUDED.fiscal_quarter,
            data = EXCLUDED.data,
            fetched_at = now()
    """
    prepared: list[tuple[Any, ...]] = []
    for r in rows:
        data_val = r[6]
        if isinstance(data_val, (dict, list)):
            data_val = json.dumps(data_val)
        prepared.append((r[0], r[2], r[3], r[4], r[5], data_val))
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, prepared)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return len(prepared)
