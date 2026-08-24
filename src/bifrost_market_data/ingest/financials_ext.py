"""Extended financials handlers: ratios / short_interest / short_volume.

Wave 8: writes split entity tables (ratios, short_interest, short_volume).

Contract per report_type:

* ``ratios``          — one row per report period (``period_date``);
                        ``period_type`` = 'daily' (Polygon ratios v1 is daily).
* ``short_interest``  — one row per ``settlement_date``; ``period_type`` = 'biweekly'.
* ``short_volume``    — one row per calendar ``date``; ``period_type`` = 'daily'.

Handlers are safe to re-run: primary key
``(symbol, period_date, period_type)`` triggers ON CONFLICT DO
UPDATE with fresh jsonb + fetched_at.
"""

from __future__ import annotations

from typing import Any, Mapping

from bifrost_market_data.ingest._upsert import as_int, parse_date
from bifrost_market_data.ingest.financials_tables import upsert_financials_rows
from bifrost_market_data.worker.claim import JobRow


def _period_date_of(item: Mapping[str, Any], *fields: str) -> Any:
    for f in fields:
        v = item.get(f)
        if v is None or v == "":
            continue
        d = parse_date(v)
        if d is not None:
            return d
    return None


async def handle_ratios(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    payload = job.payload or {}
    symbol = str(payload.get("symbol") or payload.get("ticker") or "").strip().upper()
    if not symbol:
        raise ValueError("ratios payload requires symbol")

    data = await client.fetch_ratios(ticker=symbol)
    results = list(data.get("results") or [])
    rows: list[tuple[Any, ...]] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        period_date = _period_date_of(
            item, "period_end", "end_date", "date", "period_start", "as_of_date"
        )
        if period_date is None:
            continue
        rows.append(
            (
                symbol,
                "ratios",
                period_date,
                "daily",
                as_int(item.get("fiscal_year")),
                as_int(item.get("fiscal_quarter")),
                dict(item),
            )
        )
    n = upsert_financials_rows(conn, rows)
    return {"rows_written": n, "symbol": symbol}


async def handle_short_interest(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    payload = job.payload or {}
    symbol = str(payload.get("symbol") or payload.get("ticker") or "").strip().upper()
    if not symbol:
        raise ValueError("short_interest payload requires symbol")

    data = await client.fetch_short_interest(ticker=symbol)
    results = list(data.get("results") or [])
    rows: list[tuple[Any, ...]] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        period_date = _period_date_of(item, "settlement_date", "date", "period_end")
        if period_date is None:
            continue
        rows.append(
            (
                symbol,
                "short_interest",
                period_date,
                "biweekly",
                as_int(item.get("fiscal_year")),
                as_int(item.get("fiscal_quarter")),
                dict(item),
            )
        )
    n = upsert_financials_rows(conn, rows)
    return {"rows_written": n, "symbol": symbol}


async def handle_short_volume(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    payload = job.payload or {}
    symbol = str(payload.get("symbol") or payload.get("ticker") or "").strip().upper()
    if not symbol:
        raise ValueError("short_volume payload requires symbol")

    data = await client.fetch_short_volume(ticker=symbol)
    results = list(data.get("results") or [])
    rows: list[tuple[Any, ...]] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        period_date = _period_date_of(item, "date", "trade_date", "period_end")
        if period_date is None:
            continue
        rows.append(
            (
                symbol,
                "short_volume",
                period_date,
                "daily",
                as_int(item.get("fiscal_year")),
                as_int(item.get("fiscal_quarter")),
                dict(item),
            )
        )
    n = upsert_financials_rows(conn, rows)
    return {"rows_written": n, "symbol": symbol}
