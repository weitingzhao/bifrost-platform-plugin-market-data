"""kind=financials → raw_market split entity tables (Wave 8)."""

from __future__ import annotations

from typing import Any, Mapping

from bifrost_market_data.ingest._upsert import as_int, parse_date
from bifrost_market_data.ingest.financials_tables import upsert_financials_rows
from bifrost_market_data.worker.claim import JobRow

# Wave 1 hygiene: stop writing comprehensive_income (197k+ rows unused by
# SEPA / dbt). Historical rows removed on Wave 8 migration.
_STATEMENT_KEYS = (
    "income_statement",
    "balance_sheet",
    "cash_flow_statement",
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
        # When the 10-Q / 10-K was actually filed, as opposed to period_date,
        # which is the fiscal period end. Present on quarterly and annual rows;
        # null on TTM rows and on fiscal-Q4 quarterly rows, whose filing is
        # reported by the matching annual row.
        filing_date = parse_date(item.get("filing_date"))
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
                    filing_date,
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
                    filing_date,
                )
            )

    n = upsert_financials_rows(conn, rows)
    return {
        "rows_written": n,
        "symbol": symbol,
        "truncated": bool(data.get("truncated")),
        "pages": data.get("pages"),
    }
