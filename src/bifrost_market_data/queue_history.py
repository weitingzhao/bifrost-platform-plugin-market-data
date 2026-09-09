"""A record of what the queue did, not just what a probe caught.

``ops_jobs.job_ingest`` is a work queue. Its trim caps finished rows at a few
tens of thousands, which at 600 jobs a minute is about an hour, and
``ops_jobs.ingest_freshness`` is keyed by dimension and overwritten on every
run. So no series existed anywhere, and every question about throughput had to
be answered from whatever an instantaneous read happened to see. That is how a
rate falling from 1,700 a minute to 666 went unnoticed for a day.

One row per kind per sample. ``pending`` and ``running`` are null on rows
reconstructed from finished jobs, because a past queue depth cannot be
recovered once the jobs that were waiting have been deleted; the deltas can.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

logger = logging.getLogger(__name__)

#: How often the sampler writes a row. Fine enough to show a drain changing
#: slope within the hour, coarse enough that 90 days of ten kinds is 250k rows.
SAMPLE_INTERVAL_SEC = 300
#: How long samples are kept. Trimmed alongside the job rows.
KEEP_DAYS = 90

_STATUS_SQL = """
SELECT kind,
       count(*) FILTER (WHERE status = 'pending')::bigint AS pending,
       count(*) FILTER (WHERE status = 'running')::bigint AS running,
       extract(epoch FROM (now() - min(created_at) FILTER (WHERE status = 'pending')))
         AS oldest_pending_age_sec
FROM ops_jobs.job_ingest
WHERE status IN ('pending', 'running')
GROUP BY 1
"""

_DELTA_SQL = """
SELECT kind,
       count(*) FILTER (WHERE created_at >= %(since)s)::bigint AS created_delta,
       count(*) FILTER (WHERE status = 'done' AND finished_at >= %(since)s)::bigint AS done_delta,
       count(*) FILTER (WHERE status = 'failed' AND finished_at >= %(since)s)::bigint AS failed_delta,
       percentile_cont(0.5) WITHIN GROUP (
           ORDER BY extract(epoch FROM (finished_at - started_at))
       ) FILTER (WHERE finished_at >= %(since)s AND started_at IS NOT NULL) AS p50_sec,
       percentile_cont(0.95) WITHIN GROUP (
           ORDER BY extract(epoch FROM (finished_at - started_at))
       ) FILTER (WHERE finished_at >= %(since)s AND started_at IS NOT NULL) AS p95_sec
FROM ops_jobs.job_ingest
WHERE created_at >= %(since)s OR finished_at >= %(since)s
GROUP BY 1
"""


def _rows(cur: Any) -> list[tuple[Any, ...]]:
    out = cur.fetchall() if hasattr(cur, "fetchall") else []
    return [tuple(r.values()) if isinstance(r, Mapping) else tuple(r) for r in out or []]


def take_sample(
    conn: Any,
    *,
    now: datetime | None = None,
    interval_sec: int = SAMPLE_INTERVAL_SEC,
    statement_timeout: str = "30s",
) -> int:
    """Write one row per kind for this instant. Returns the number of rows written.

    Deltas are counted over the interval that just ended rather than differenced
    against the previous row, so a missed sample leaves a gap in the series
    instead of a spike in the next point.
    """
    at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    since = at - timedelta(seconds=int(interval_sec))
    depth: dict[str, tuple[Any, Any, Any]] = {}
    deltas: dict[str, tuple[Any, ...]] = {}
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL statement_timeout = '{statement_timeout}'")
            cur.execute(_STATUS_SQL)
            for kind, pending, running, oldest in _rows(cur):
                depth[str(kind)] = (pending, running, oldest)
            cur.execute(_DELTA_SQL, {"since": since})
            for row in _rows(cur):
                deltas[str(row[0])] = row[1:]
        conn.commit()
    except Exception as exc:  # noqa: BLE001 — a missed sample is a gap, not an outage
        logger.warning("queue sample read failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return 0

    kinds = sorted(set(depth) | set(deltas))
    if not kinds:
        return 0
    values = []
    for kind in kinds:
        pending, running, oldest = depth.get(kind, (0, 0, None))
        created, done, failed, p50, p95 = deltas.get(kind, (0, 0, 0, None, None))
        values.append((at, kind, pending, running, created, done, failed, oldest, p50, p95))
    return _write(conn, values)


def _write(conn: Any, values: Sequence[tuple[Any, ...]]) -> int:
    try:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO ops_jobs.queue_sample (
                    sample_ts, kind, pending, running,
                    created_delta, done_delta, failed_delta,
                    oldest_pending_age_sec, p50_sec, p95_sec
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sample_ts, kind) DO UPDATE SET
                    pending = EXCLUDED.pending,
                    running = EXCLUDED.running,
                    created_delta = EXCLUDED.created_delta,
                    done_delta = EXCLUDED.done_delta,
                    failed_delta = EXCLUDED.failed_delta,
                    oldest_pending_age_sec = EXCLUDED.oldest_pending_age_sec,
                    p50_sec = EXCLUDED.p50_sec,
                    p95_sec = EXCLUDED.p95_sec
                """,
                list(values),
            )
        conn.commit()
        return len(values)
    except Exception as exc:  # noqa: BLE001
        logger.warning("queue sample write failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return 0


def backfill_from_jobs(
    conn: Any,
    *,
    interval_sec: int = SAMPLE_INTERVAL_SEC,
    statement_timeout: str = "300s",
) -> int:
    """Reconstruct the deltas still recoverable from ``job_ingest``.

    Run once, before the trim is allowed to catch up: the finished rows it is
    about to delete are the only record of the hours before the sampler existed.
    Depth is left null — what was waiting at a past instant cannot be recovered
    from what finished.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL statement_timeout = '{statement_timeout}'")
            cur.execute(
                """
                WITH bucket AS (
                    SELECT
                        to_timestamp(floor(extract(epoch FROM created_at) / %(sec)s) * %(sec)s) AS ts,
                        kind, 1 AS created, 0 AS done, 0 AS failed, NULL::double precision AS dur
                    FROM ops_jobs.job_ingest WHERE created_at IS NOT NULL
                    UNION ALL
                    SELECT
                        to_timestamp(floor(extract(epoch FROM finished_at) / %(sec)s) * %(sec)s) AS ts,
                        kind, 0,
                        CASE WHEN status = 'done' THEN 1 ELSE 0 END,
                        CASE WHEN status = 'failed' THEN 1 ELSE 0 END,
                        extract(epoch FROM (finished_at - started_at))
                    FROM ops_jobs.job_ingest WHERE finished_at IS NOT NULL
                )
                INSERT INTO ops_jobs.queue_sample (
                    sample_ts, kind, created_delta, done_delta, failed_delta, p50_sec, p95_sec
                )
                SELECT ts, kind,
                       sum(created)::bigint, sum(done)::bigint, sum(failed)::bigint,
                       percentile_cont(0.5) WITHIN GROUP (ORDER BY dur),
                       percentile_cont(0.95) WITHIN GROUP (ORDER BY dur)
                FROM bucket
                GROUP BY 1, 2
                ON CONFLICT (sample_ts, kind) DO NOTHING
                """,
                {"sec": int(interval_sec)},
            )
            n = int(getattr(cur, "rowcount", 0) or 0)
        conn.commit()
        return n
    except Exception as exc:  # noqa: BLE001
        logger.warning("queue sample backfill failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return 0


def read_series(
    conn: Any,
    *,
    hours: float = 48.0,
    kind: str | None = None,
    statement_timeout: str = "30s",
) -> list[dict[str, Any]]:
    """The series, newest last, one entry per sample (summed across kinds by default)."""
    cols = (
        "sample_ts, kind, pending, running, created_delta, done_delta, "
        "failed_delta, oldest_pending_age_sec, p50_sec, p95_sec"
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL statement_timeout = '{statement_timeout}'")
            if kind:
                cur.execute(
                    f"SELECT {cols} FROM ops_jobs.queue_sample "
                    "WHERE kind = %s AND sample_ts >= now() - make_interval(secs => %s) "
                    "ORDER BY sample_ts",
                    (kind, float(hours) * 3600.0),
                )
                rows = _rows(cur)
                out = [
                    {
                        "sample_ts": r[0].isoformat() if hasattr(r[0], "isoformat") else r[0],
                        "kind": r[1],
                        "pending": r[2],
                        "running": r[3],
                        "created_delta": r[4],
                        "done_delta": r[5],
                        "failed_delta": r[6],
                        "oldest_pending_age_sec": r[7],
                        "p50_sec": r[8],
                        "p95_sec": r[9],
                    }
                    for r in rows
                ]
            else:
                cur.execute(
                    """
                    SELECT sample_ts,
                           sum(pending)::bigint, sum(running)::bigint,
                           sum(created_delta)::bigint, sum(done_delta)::bigint,
                           sum(failed_delta)::bigint,
                           max(oldest_pending_age_sec),
                           max(p95_sec)
                    FROM ops_jobs.queue_sample
                    WHERE sample_ts >= now() - make_interval(secs => %s)
                    GROUP BY 1 ORDER BY 1
                    """,
                    (float(hours) * 3600.0,),
                )
                out = [
                    {
                        "sample_ts": r[0].isoformat() if hasattr(r[0], "isoformat") else r[0],
                        "kind": None,
                        "pending": r[1],
                        "running": r[2],
                        "created_delta": r[3],
                        "done_delta": r[4],
                        "failed_delta": r[5],
                        "oldest_pending_age_sec": r[6],
                        "p95_sec": r[7],
                    }
                    for r in _rows(cur)
                ]
        conn.commit()
        return out
    except Exception as exc:  # noqa: BLE001
        logger.warning("queue history read failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return []


def trim_samples(conn: Any, *, keep_days: int = KEEP_DAYS) -> int:
    """Drop samples past the retention window."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM ops_jobs.queue_sample "
                "WHERE sample_ts < now() - make_interval(days => %s)",
                (int(keep_days),),
            )
            n = int(getattr(cur, "rowcount", 0) or 0)
        conn.commit()
        return n
    except Exception as exc:  # noqa: BLE001
        logger.warning("queue sample trim failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return 0


__all__ = [
    "SAMPLE_INTERVAL_SEC",
    "KEEP_DAYS",
    "take_sample",
    "backfill_from_jobs",
    "read_series",
    "trim_samples",
]
