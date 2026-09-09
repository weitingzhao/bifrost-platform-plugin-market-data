"""GET /market/ingest/queue-history — what the queue did, over time.

The queue dashboard answers "right now". That was the only answer available,
and it is a thin one: a rate that fell from 1,700 jobs a minute to 666 looked
identical to a healthy queue in every single reading. This serves the recorded
series instead, and runs the sampler that writes it.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

from fastapi import APIRouter, Depends, Query

from bifrost_market_data.api.deps import connect_db, require_db, require_write_token
from bifrost_market_data.queue_history import (
    SAMPLE_INTERVAL_SEC,
    backfill_from_jobs,
    read_series,
    take_sample,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/ingest", tags=["market-ingest"])

_sampler_started = False
_sampler_lock = threading.Lock()


def _sample_once() -> int:
    conn = connect_db(statement_timeout="60s")
    try:
        return take_sample(conn)
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001 — a close that fails must not lose the sample
            pass


def start_sampler(interval_sec: int = SAMPLE_INTERVAL_SEC) -> bool:
    """Start the background sampler once per process. True if this call started it.

    A thread rather than a scheduled slot: the series must not depend on Dagster
    being healthy, since one of the things worth seeing in it is Dagster missing
    a fire. The cost is that samples pause while this pod restarts, which shows
    in the series as a gap — the honest rendering of "nobody was watching".
    """
    global _sampler_started
    with _sampler_lock:
        if _sampler_started:
            return False
        _sampler_started = True

    def run() -> None:
        while True:
            try:
                _sample_once()
            except Exception:  # noqa: BLE001 — the sampler outlives any one failure
                logger.exception("queue sampler tick failed")
            time.sleep(max(30, int(interval_sec)))

    threading.Thread(target=run, name="queue-sampler", daemon=True).start()
    logger.info("queue sampler started, every %ss", interval_sec)
    return True


@router.get("/queue-history")
def queue_history(
    hours: float = Query(48.0, gt=0, le=24 * 90, description="how far back to read"),
    kind: str | None = Query(None, description="one job kind, or omit for the whole queue"),
) -> dict[str, Any]:
    """Queue depth and throughput over time, from ops_jobs.queue_sample."""
    conn = require_db()
    try:
        series = read_series(conn, hours=hours, kind=kind)
    finally:
        conn.close()
    return {
        "ok": True,
        "data": {
            "interval_sec": SAMPLE_INTERVAL_SEC,
            "hours": hours,
            "kind": kind,
            "points": series,
        },
    }


@router.post("/queue-history/backfill", dependencies=[Depends(require_write_token)])
def queue_history_backfill() -> dict[str, Any]:
    """Reconstruct what the still-present job rows can tell us, once.

    Only the deltas come back; a past queue depth cannot be recovered from jobs
    that have already finished. Run before letting the trim catch up.
    """
    conn = require_db()
    try:
        written = backfill_from_jobs(conn)
    finally:
        conn.close()
    return {"ok": True, "data": {"rows_written": written}}


__all__ = ["router", "start_sampler", "queue_history"]
