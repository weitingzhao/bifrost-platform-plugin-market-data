"""Full-market ratios / short data by date — one paginated pull per session.

Financials & Ratios answers ``ratios?date=D``, ``short-volume?date=D`` and
``short-interest?settlement_date.gte=D`` for every ticker at 1,000 rows a
page, so a whole session lands in a handful of requests instead of one call
per symbol. Rows go into the same entity tables the per-symbol handlers use.
"""

from __future__ import annotations

from typing import Any, Mapping

from bifrost_market_data.ingest._upsert import parse_date
from bifrost_market_data.ingest.financials_tables import upsert_financials_rows
from bifrost_market_data.worker.claim import JobRow


def _rows_from(results: list[Any], *, report_type: str, period_type: str, date_keys: tuple[str, ...]) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("ticker") or item.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        period_date = None
        for key in date_keys:
            period_date = parse_date(item.get(key))
            if period_date is not None:
                break
        if period_date is None:
            continue
        rows.append((symbol, report_type, period_date, period_type, None, None, dict(item)))
    return rows


async def handle_ratios_market(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    payload = job.payload or {}
    day = str(payload.get("date") or "").strip()
    if not day:
        raise ValueError("ratios_market payload requires date")
    data = await client.fetch_ratios_market(day)
    rows = _rows_from(list(data.get("results") or []), report_type="ratios", period_type="daily", date_keys=("date",))
    n = upsert_financials_rows(conn, rows)
    return {"rows_written": n, "date": day, "pages": data.get("pages"), "truncated": bool(data.get("truncated"))}


async def handle_short_volume_market(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    payload = job.payload or {}
    day = str(payload.get("date") or "").strip()
    if not day:
        raise ValueError("short_volume_market payload requires date")
    data = await client.fetch_short_volume_market(day)
    rows = _rows_from(list(data.get("results") or []), report_type="short_volume", period_type="daily", date_keys=("date",))
    n = upsert_financials_rows(conn, rows)
    return {"rows_written": n, "date": day, "pages": data.get("pages"), "truncated": bool(data.get("truncated"))}


async def handle_short_interest_market(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    payload = job.payload or {}
    since = str(payload.get("settlement_date_gte") or payload.get("from") or "").strip()
    if not since:
        raise ValueError("short_interest_market payload requires settlement_date_gte")
    data = await client.fetch_short_interest_market(since)
    rows = _rows_from(
        list(data.get("results") or []),
        report_type="short_interest",
        period_type="biweekly",
        date_keys=("settlement_date",),
    )
    n = upsert_financials_rows(conn, rows)
    return {
        "rows_written": n,
        "settlement_date_gte": since,
        "pages": data.get("pages"),
        "truncated": bool(data.get("truncated")),
    }
