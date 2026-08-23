"""Extended financials handlers: ratios / short_interest / short_volume.

All three write into ``market.stock_financials`` (jsonb ``data``) with the
appropriate ``report_type`` so downstream SEPA/analytics queries can join.

Contract per report_type:

* ``ratios``          — one row per report period (``period_date``);
                        ``period_type`` = 'daily' (Polygon ratios v1 is daily).
* ``short_interest``  — one row per ``settlement_date``; ``period_type`` = 'biweekly'.
* ``short_volume``    — one row per calendar ``date``; ``period_type`` = 'daily'.

Handlers are safe to re-run: primary key
``(symbol, report_type, period_date, period_type)`` triggers ON CONFLICT DO
UPDATE with fresh jsonb + fetched_at.
"""

from __future__ import annotations

import json
from typing import Any, Mapping

from bifrost_market_data.ingest._upsert import as_int, parse_date
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


def _upsert_rows(conn: Any, rows: list[tuple[Any, ...]]) -> int:
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
    n = _upsert_rows(conn, rows)
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
    n = _upsert_rows(conn, rows)
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
    n = _upsert_rows(conn, rows)
    return {"rows_written": n, "symbol": symbol}
