"""kind=calendar → data_ops.us_trading_calendar."""

from __future__ import annotations

from typing import Any, Mapping

from bifrost_market_data.ingest._upsert import batch_upsert, parse_date
from bifrost_market_data.worker.claim import JobRow

_COLS = ("cal_date", "is_trading", "note")


async def handle_calendar(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    _ = job  # payload unused; upcoming market status is global
    data = await client.fetch_market_status_upcoming()
    results = list(data.get("results") or [])
    rows: list[tuple[Any, ...]] = []
    seen: set[Any] = set()

    for item in results:
        if not isinstance(item, dict):
            continue
        cal_date = parse_date(item.get("date"))
        if cal_date is None or cal_date in seen:
            continue
        status = str(item.get("status") or "").strip().lower()
        # early-close is still a trading day (shortened hours)
        is_trading = status not in ("closed", "holiday")
        name = item.get("name") or item.get("exchange") or status or "market status"
        note = str(name)
        seen.add(cal_date)
        rows.append((cal_date, is_trading, note))

    n = batch_upsert(
        conn,
        "data_ops.us_trading_calendar",
        _COLS,
        rows,
        conflict_keys=("cal_date",),
        update_cols=("is_trading", "note"),
        set_fetched_at=False,
    )
    return {"rows_written": n, "pages": data.get("pages") or 1}
