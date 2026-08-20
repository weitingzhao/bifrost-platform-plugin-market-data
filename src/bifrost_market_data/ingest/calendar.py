"""kind=calendar → market.us_market_holiday (canonical US exchange calendar)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from bifrost_market_data.ingest._upsert import batch_upsert, parse_date
from bifrost_market_data.worker.claim import JobRow

_HOLIDAY_COLS = ("exchange", "holiday_date", "name", "status", "open_time", "close_time")


def _parse_timestamptz(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    s = str(value).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


async def handle_calendar(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    _ = job  # payload unused; upcoming market status is global
    data = await client.fetch_market_status_upcoming()
    results = list(data.get("results") or [])
    holiday_rows: list[tuple[Any, ...]] = []
    seen_holiday: set[tuple[str, Any]] = set()

    for item in results:
        if not isinstance(item, dict):
            continue
        cal_date = parse_date(item.get("date"))
        if cal_date is None:
            continue
        status_raw = str(item.get("status") or "").strip().lower()
        # Persist closed + early-close detail for Trade / Plugin consumers.
        if status_raw not in ("closed", "holiday", "early-close"):
            continue
        name = item.get("name") or item.get("exchange") or status_raw or "market status"
        exchange = str(item.get("exchange") or "NYSE").strip() or "NYSE"
        status = "closed" if status_raw in ("closed", "holiday") else "early-close"
        key = (exchange, cal_date)
        if key in seen_holiday:
            continue
        seen_holiday.add(key)
        holiday_rows.append(
            (
                exchange,
                cal_date,
                str(name),
                status,
                _parse_timestamptz(item.get("open")),
                _parse_timestamptz(item.get("close")),
            )
        )

    n_hol = batch_upsert(
        conn,
        "market.us_market_holiday",
        _HOLIDAY_COLS,
        holiday_rows,
        conflict_keys=("exchange", "holiday_date"),
        update_cols=("name", "status", "open_time", "close_time"),
        set_fetched_at=True,
    )
    return {
        "rows_written": n_hol,
        "holiday_rows_written": n_hol,
        "pages": data.get("pages") or 1,
    }
