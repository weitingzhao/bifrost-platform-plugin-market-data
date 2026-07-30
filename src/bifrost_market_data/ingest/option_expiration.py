"""kind=option_expiration → market.option_expiration."""

from __future__ import annotations

from typing import Any, Mapping

from bifrost_market_data.ingest._upsert import batch_upsert, parse_date
from bifrost_market_data.worker.claim import JobRow

_COLS = ("underlying", "expiry")


async def handle_option_expiration(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    payload = job.payload or {}
    underlying = str(payload.get("underlying") or payload.get("underlying_ticker") or "").strip().upper()
    if not underlying:
        raise ValueError("option_expiration payload requires underlying")

    data = await client.fetch_options_contracts(
        underlying_ticker=underlying,
        expired=False,
        max_pages=int(payload.get("max_pages") or 20),
    )
    results = list(data.get("results") or [])
    seen: set[Any] = set()
    for item in results:
        if not isinstance(item, dict):
            continue
        expiry = parse_date(item.get("expiration_date"))
        if expiry is None:
            continue
        und = str(item.get("underlying_ticker") or underlying).strip().upper()
        seen.add((und, expiry))

    rows = sorted(seen, key=lambda x: (x[0], x[1]))
    try:
        n = batch_upsert(
            conn,
            "market.option_expiration",
            _COLS,
            rows,
            conflict_keys=("underlying", "expiry"),
            update_cols=(),
            set_fetched_at=False,
            auto_commit=False,
        )
        if rows:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE market.option_expiration
                    SET updated_at = now()
                    WHERE underlying = %s
                    """,
                    (underlying,),
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {
        "rows_written": n,
        "underlying": underlying,
        "truncated": bool(data.get("truncated")),
        "pages": data.get("pages"),
    }
