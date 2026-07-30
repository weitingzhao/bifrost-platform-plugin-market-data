"""Historical backfill enqueue helpers (used by scripts/backfill.py)."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from bifrost_market_data.scheduler.enqueue import insert_job


def date_chunks(from_date: date, to_date: date, chunk_days: int) -> list[tuple[date, date]]:
    """Split [from_date, to_date] into inclusive chunks of at most ``chunk_days`` days."""
    if to_date < from_date:
        raise ValueError(f"to_date {to_date} < from_date {from_date}")
    if chunk_days < 1:
        raise ValueError("chunk_days must be >= 1")
    chunks: list[tuple[date, date]] = []
    cur = from_date
    while cur <= to_date:
        end = min(cur + timedelta(days=chunk_days - 1), to_date)
        chunks.append((cur, end))
        cur = end + timedelta(days=1)
    return chunks


def enqueue_backfill(
    conn: Any,
    *,
    kind: str,
    symbols: list[str],
    from_date: date,
    to_date: date,
    chunk_days: int = 365,
    priority: int = 0,
) -> dict[str, Any]:
    """Enqueue chunked backfill jobs. Returns summary."""
    kind_s = str(kind).strip()
    if kind_s not in ("stock_daily", "option_daily"):
        raise ValueError(f"unsupported kind for backfill: {kind_s!r}")
    if not symbols:
        raise ValueError("symbols required")

    chunks = date_chunks(from_date, to_date, chunk_days)
    enqueued = 0
    deduped = 0
    jobs: list[dict[str, Any]] = []

    for sym in symbols:
        for start, end in chunks:
            if kind_s == "stock_daily":
                payload = {"symbol": sym, "from": start.isoformat(), "to": end.isoformat()}
            else:
                payload = {
                    "option_ticker": sym,
                    "from": start.isoformat(),
                    "to": end.isoformat(),
                }
            job_id = insert_job(conn, kind=kind_s, payload=payload, priority=priority)
            if job_id is None:
                deduped += 1
                jobs.append({"kind": kind_s, "payload": payload, "deduped": True})
            else:
                enqueued += 1
                jobs.append({"kind": kind_s, "payload": payload, "id": job_id, "deduped": False})

    return {
        "kind": kind_s,
        "symbols": len(symbols),
        "chunks": len(chunks),
        "enqueued": enqueued,
        "deduped": deduped,
        "jobs": jobs,
    }
