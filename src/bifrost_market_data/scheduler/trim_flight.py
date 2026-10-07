"""Single-flight nightly trim (TD-106).

The lock is a session advisory lock. It lives as long as the connection that
took it, so the background task keeps that connection until the trim has
written its result. A second caller does not start another trim.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any

from bifrost_market_data.scheduler.enqueue import payload_hash

TRIM_LOCK_NAME = "bifrost.market.trim"
TRIM_JOB_KIND = "slot-trim"
#: Longer than the sum of the trim budgets. The Dagster poller waits this long
#: too (bifrost_research.orchestration.plugin_http.TRIM_CLIENT_TIMEOUT_SEC).
TRIM_CLIENT_TIMEOUT_SEC = 1200.0
TRIM_STATEMENT_TIMEOUT = "1500s"


def try_acquire(conn: Any) -> bool:
    """Take the trim lock, or report that this session already holds it.

    A second ``pg_try_advisory_lock`` in the same session would succeed and
    bump the lock count, so a later unlock would drop the lock early.
    """
    if getattr(conn, "_bifrost_trim_lock", False):
        return True
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (TRIM_LOCK_NAME,))
        row = cur.fetchone()
    ok = bool(row[0] if not isinstance(row, Mapping) else next(iter(row.values())))
    if ok:
        conn._bifrost_trim_lock = True
    return ok


def release(conn: Any) -> None:
    if not getattr(conn, "_bifrost_trim_lock", False):
        return
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (TRIM_LOCK_NAME,))
        if hasattr(conn, "commit"):
            conn.commit()
    finally:
        conn._bifrost_trim_lock = False


def latest_running_id(conn: Any) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id FROM ops_jobs.job_ingest
            WHERE kind = %s AND status = 'running'
            ORDER BY id DESC
            LIMIT 1
            """,
            (TRIM_JOB_KIND,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return int(row["id"] if isinstance(row, Mapping) else row[0])


def retire_stale_and_insert(conn: Any, payload: Mapping[str, Any]) -> int:
    """The lock is held, so a ``running`` row belongs to a holder that died."""
    body = dict(payload)
    ph = payload_hash(body)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ops_jobs.job_ingest
            SET status = 'failed',
                result = %s::jsonb,
                finished_at = now(),
                updated_at = now()
            WHERE kind = %s AND status = 'running'
            """,
            (
                json.dumps({"error": "previous trim holder exited without finishing"}),
                TRIM_JOB_KIND,
            ),
        )
        cur.execute(
            """
            INSERT INTO ops_jobs.job_ingest
                (kind, payload, payload_hash, priority, status, max_attempts,
                 attempts, started_at)
            VALUES (%s, %s::jsonb, %s, 0, 'running', 1, 1, now())
            RETURNING id
            """,
            (TRIM_JOB_KIND, json.dumps(body), ph),
        )
        row = cur.fetchone()
    conn.commit()
    if row is None:
        raise RuntimeError("trim job insert returned no id")
    return int(row["id"] if isinstance(row, Mapping) else row[0])


def write_result(conn: Any, job_id: int, status: str, result: Mapping[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ops_jobs.job_ingest
            SET status = %s,
                result = %s::jsonb,
                finished_at = now(),
                updated_at = now()
            WHERE id = %s
            """,
            (status, json.dumps(dict(result), default=str), int(job_id)),
        )
    conn.commit()


def start_single_flight(
    *,
    try_lock: Callable[[], bool],
    running_job: Callable[[], Any],
    begin: Callable[[], Any],
    run: Callable[[], Mapping[str, Any]],
    finish: Callable[[Any, str, Mapping[str, Any]], None],
    unlock: Callable[[], None],
    spawn: Callable[[Callable[[], None]], None],
) -> dict[str, Any]:
    """Start ``run`` once. A caller that loses the lock is told who is running."""
    if not try_lock():
        return {"ok": True, "status": "already_running", "job_id": running_job()}
    job_id = begin()

    def _bg() -> None:
        try:
            result = run()
            body = result if isinstance(result, Mapping) else {"result": result}
            finish(job_id, "done", body)
        except Exception as exc:  # noqa: BLE001 — the job row records whatever stopped the trim
            finish(job_id, "failed", {"ok": False, "error": f"{type(exc).__name__}: {exc}"})
        finally:
            unlock()

    try:
        spawn(_bg)
    except Exception:
        unlock()
        raise
    return {"ok": True, "status": "accepted", "job_id": job_id}
