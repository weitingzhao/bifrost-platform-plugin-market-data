"""kind=treasury_yields → raw_market.treasury_yield.

Constant-maturity Treasury yields are free with every Massive plan and are the
risk-free leg every option model in Research needs; without them the solvers
fall back to a hard-coded rate.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Mapping

from bifrost_market_data.ingest._upsert import as_float, batch_upsert, parse_date
from bifrost_market_data.worker.claim import JobRow

_COLS = (
    "yield_date",
    "yield_1_month",
    "yield_3_month",
    "yield_1_year",
    "yield_2_year",
    "yield_5_year",
    "yield_10_year",
    "yield_30_year",
)

DEFAULT_LOOKBACK_DAYS = 30


async def handle_treasury_yields(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    """Payload: ``{"from": "2026-08-01", "to": "2026-09-08"}`` (both optional)."""
    payload = job.payload or {}
    to_date = parse_date(payload.get("to")) or date.today()
    from_date = parse_date(payload.get("from")) or to_date - timedelta(
        days=int(payload.get("lookback_days") or DEFAULT_LOOKBACK_DAYS)
    )

    data = await client.fetch_treasury_yields(
        date_gte=from_date.isoformat(), date_lte=to_date.isoformat()
    )
    rows: list[tuple[Any, ...]] = []
    for item in data.get("results") or []:
        if not isinstance(item, dict):
            continue
        d = parse_date(item.get("date"))
        if d is None:
            continue
        rows.append(
            (
                d,
                as_float(item.get("yield_1_month")),
                as_float(item.get("yield_3_month")),
                as_float(item.get("yield_1_year")),
                as_float(item.get("yield_2_year")),
                as_float(item.get("yield_5_year")),
                as_float(item.get("yield_10_year")),
                as_float(item.get("yield_30_year")),
            )
        )

    n = batch_upsert(
        conn,
        "market.treasury_yield",
        _COLS,
        rows,
        conflict_keys=("yield_date",),
        update_cols=tuple(c for c in _COLS if c != "yield_date"),
        set_fetched_at=True,
    )
    return {
        "rows_written": n,
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "pages": data.get("pages"),
    }
