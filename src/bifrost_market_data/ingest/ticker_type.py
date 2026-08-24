"""kind=ticker_type → market.ticker_type (Polygon ticker types dictionary)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from bifrost_market_data.ingest._upsert import batch_upsert, physical_table_name
from bifrost_market_data.worker.claim import JobRow

_COLS = ("code", "description", "asset_class", "locale", "fetched_at")


async def handle_ticker_type(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    """Full-replace Polygon instrument type dictionary (small, ~25 rows)."""
    _ = job  # payload unused — always full sync
    data = await client.fetch_ticker_types()
    results = list(data.get("results") or [])
    fetched_at = datetime.now(timezone.utc)
    rows: list[tuple[Any, ...]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in results:
        if not isinstance(item, dict):
            continue
        code = str(item.get("code") or item.get("type") or "").strip()
        if not code:
            continue
        desc = (str(item.get("description") or "").strip() or None)
        ac = str(item.get("asset_class") or "").strip() or ""
        loc = str(item.get("locale") or "").strip() or ""
        key = (code, ac, loc)
        if key in seen:
            continue
        seen.add(key)
        rows.append((code, desc, ac, loc, fetched_at))

    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE {physical_table_name('market.ticker_type')}")
        n = batch_upsert(
            conn,
            "market.ticker_type",
            _COLS,
            rows,
            conflict_keys=("code", "asset_class", "locale"),
            update_cols=("description", "fetched_at"),
            set_fetched_at=False,
            auto_commit=False,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return {"rows_written": n}
