"""kind=ticker_related → market.ticker_related (Polygon related-companies)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from bifrost_market_data.ingest._upsert import batch_upsert
from bifrost_market_data.worker.claim import JobRow

_COLS = ("from_symbol", "to_symbol", "rank", "fetched_at")


async def handle_ticker_related(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    payload = job.payload or {}
    symbol = str(payload.get("symbol") or payload.get("ticker") or "").strip().upper()
    if not symbol:
        raise ValueError("ticker_related requires symbol")

    data = await client.fetch_related_companies(symbol)
    results = list(data.get("results") or [])
    fetched_at = datetime.now(timezone.utc)
    rows: list[tuple[Any, ...]] = []
    seen: set[str] = set()
    for idx, item in enumerate(results):
        if not isinstance(item, dict):
            continue
        peer = str(item.get("ticker") or item.get("symbol") or "").strip().upper()
        if not peer or peer in seen:
            continue
        seen.add(peer)
        rows.append((symbol, peer, idx, fetched_at))

    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM raw_market.ticker_related WHERE from_symbol = %s",
                (symbol,),
            )
        n = batch_upsert(
            conn,
            "market.ticker_related",
            _COLS,
            rows,
            conflict_keys=("from_symbol", "to_symbol"),
            update_cols=("rank", "fetched_at"),
            set_fetched_at=False,
            auto_commit=False,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return {"rows_written": n, "symbol": symbol, "peers": len(rows)}
