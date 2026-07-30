"""kind=financials → market.stock_financials (jsonb per statement type)."""

from __future__ import annotations

import json
from typing import Any, Mapping

from bifrost_market_data.ingest._upsert import as_int, parse_date
from bifrost_market_data.worker.claim import JobRow

_STATEMENT_KEYS = (
    "income_statement",
    "balance_sheet",
    "cash_flow_statement",
    "comprehensive_income",
)


async def handle_financials(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    payload = job.payload or {}
    symbol = str(payload.get("symbol") or payload.get("ticker") or "").strip().upper()
    if not symbol:
        raise ValueError("financials payload requires symbol")
    timeframe = payload.get("timeframe")
    if timeframe is not None:
        timeframe = str(timeframe).strip() or None

    data = await client.fetch_financials(symbol, timeframe=timeframe)
    results = list(data.get("results") or [])
    rows: list[tuple[Any, ...]] = []

    for item in results:
        if not isinstance(item, dict):
            continue
        period_date = parse_date(
            item.get("end_date") or item.get("period_end") or item.get("filing_date")
        )
        if period_date is None:
            continue
        period_type = str(
            item.get("timeframe") or item.get("fiscal_period") or timeframe or ""
        ).strip()
        fiscal_year = as_int(item.get("fiscal_year"))
        fiscal_quarter = as_int(item.get("fiscal_quarter"))
        financials = item.get("financials") if isinstance(item.get("financials"), dict) else {}

        wrote_any = False
        for key in _STATEMENT_KEYS:
            stmt = financials.get(key)
            if not isinstance(stmt, dict) or not stmt:
                continue
            rows.append(
                (
                    symbol,
                    key,
                    period_date,
                    period_type,
                    fiscal_year,
                    fiscal_quarter,
                    stmt,
                )
            )
            wrote_any = True

        # Fallback: store whole item when no nested statements
        if not wrote_any:
            rows.append(
                (
                    symbol,
                    str(item.get("report_type") or "financials"),
                    period_date,
                    period_type,
                    fiscal_year,
                    fiscal_quarter,
                    dict(item),
                )
            )

    # batch_upsert JSON-encodes dicts; ensure jsonb cast works via ::jsonb in SQL?
    # Our _prepare_value dumps to JSON string — need cast in SQL for jsonb columns.
    # Override: write with explicit SQL for financials so data::jsonb is correct.
    n = _upsert_financials(conn, rows)
    return {
        "rows_written": n,
        "symbol": symbol,
        "truncated": bool(data.get("truncated")),
        "pages": data.get("pages"),
    }


def _upsert_financials(conn: Any, rows: list[tuple[Any, ...]]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO market.stock_financials
            (symbol, report_type, period_date, period_type, fiscal_year, fiscal_quarter, data)
        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (symbol, report_type, period_date, period_type) DO UPDATE SET
            fiscal_year = EXCLUDED.fiscal_year,
            fiscal_quarter = EXCLUDED.fiscal_quarter,
            data = EXCLUDED.data,
            fetched_at = now()
    """
    prepared = []
    for r in rows:
        data_val = r[6]
        if isinstance(data_val, (dict, list)):
            data_val = json.dumps(data_val)
        prepared.append((r[0], r[1], r[2], r[3], r[4], r[5], data_val))
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, prepared)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return len(prepared)
