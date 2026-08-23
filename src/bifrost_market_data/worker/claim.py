"""SELECT FOR UPDATE SKIP LOCKED job claim against data_ops.job_ingest."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Protocol, Sequence


class _Cursor(Protocol):
    def execute(self, query: str, params: Any = None) -> Any: ...

    def fetchone(self) -> Any: ...

    def __enter__(self) -> _Cursor: ...

    def __exit__(self, *args: object) -> None: ...


class _Connection(Protocol):
    def cursor(self) -> Any: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...


@dataclass(frozen=True)
class JobRow:
    """One row from data_ops.job_ingest after claim."""

    id: int
    kind: str
    payload: dict[str, Any]
    payload_hash: str | None
    priority: int
    status: str
    result: dict[str, Any] | None
    attempts: int
    max_attempts: int
    created_at: datetime | None
    updated_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None


_JOB_COLUMNS = (
    "id, kind, payload, payload_hash, priority, status, result, "
    "attempts, max_attempts, created_at, updated_at, started_at, finished_at"
)


def _as_dict(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode()
    if isinstance(value, str):
        return json.loads(value) if value else {}
    if isinstance(value, Mapping):
        return dict(value)
    raise TypeError(f"unsupported jsonb payload type: {type(value)!r}")


def job_row_from_record(row: Sequence[Any] | Mapping[str, Any]) -> JobRow:
    """Build JobRow from a positional tuple (column order = _JOB_COLUMNS) or mapping."""
    if isinstance(row, Mapping):
        data = dict(row)
        return JobRow(
            id=int(data["id"]),
            kind=str(data["kind"]),
            payload=_as_dict(data.get("payload")) or {},
            payload_hash=data.get("payload_hash"),
            priority=int(data.get("priority") or 0),
            status=str(data["status"]),
            result=_as_dict(data.get("result")),
            attempts=int(data.get("attempts") or 0),
            max_attempts=int(data.get("max_attempts") or 3),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            started_at=data.get("started_at"),
            finished_at=data.get("finished_at"),
        )
    return JobRow(
        id=int(row[0]),
        kind=str(row[1]),
        payload=_as_dict(row[2]) or {},
        payload_hash=row[3],
        priority=int(row[4] or 0),
        status=str(row[5]),
        result=_as_dict(row[6]),
        attempts=int(row[7] or 0),
        max_attempts=int(row[8] or 3),
        created_at=row[9],
        updated_at=row[10],
        started_at=row[11],
        finished_at=row[12],
    )


def claim_job(conn: _Connection, pool_kinds: Sequence[str]) -> JobRow | None:
    """Atomically claim one pending job whose kind is in ``pool_kinds``.

    Uses ``SELECT … FOR UPDATE SKIP LOCKED`` then marks the row ``running``
    and increments ``attempts`` in the same transaction.
    """
    kinds = [str(k) for k in pool_kinds if str(k).strip()]
    if not kinds:
        return None

    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM data_ops.job_ingest
                WHERE status = 'pending'
                  AND kind = ANY(%s)
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """,
                (list(kinds),),
            )
            row = cur.fetchone()
            if row is None:
                conn.commit()
                return None

            job_id = int(row[0] if not isinstance(row, Mapping) else row["id"])
            cur.execute(
                f"""
                UPDATE data_ops.job_ingest
                SET status = 'running',
                    attempts = attempts + 1,
                    started_at = now(),
                    updated_at = now(),
                    finished_at = NULL
                WHERE id = %s
                RETURNING {_JOB_COLUMNS}
                """,
                (job_id,),
            )
            updated = cur.fetchone()
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    if updated is None:
        return None
    return job_row_from_record(updated)


def mark_done(conn: _Connection, job_id: int, result: Mapping[str, Any] | None = None) -> None:
    """Mark a running job as successfully completed."""
    payload = dict(result) if result is not None else {}
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE data_ops.job_ingest
                SET status = 'done',
                    result = %s::jsonb,
                    finished_at = now(),
                    updated_at = now()
                WHERE id = %s
                """,
                (json.dumps(payload), int(job_id)),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def reclaim_stale_running(
    conn: _Connection,
    *,
    stale_after_sec: int,
    kinds: Sequence[str] | None = None,
) -> dict[str, int]:
    """Re-queue abandoned ``running`` rows so retry can happen after a crash.

    Claim already increments ``attempts``. If the worker dies before
    ``mark_done`` / ``mark_failed``, the row stays ``running`` forever.
    Jobs still under ``max_attempts`` go back to ``pending``; exhausted
    jobs become ``failed``.
    """
    stale_after_sec = max(30, int(stale_after_sec))
    kinds_list = [str(k) for k in (kinds or []) if str(k).strip()]
    kind_sql = "AND kind = ANY(%s)" if kinds_list else ""
    params: list[Any] = [stale_after_sec]
    if kinds_list:
        params.append(list(kinds_list))
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE data_ops.job_ingest
                SET status = CASE
                        WHEN attempts >= max_attempts THEN 'failed'
                        ELSE 'pending'
                    END,
                    result = jsonb_build_object(
                        'error', 'stale_running: abandoned after started_at',
                        'attempts', attempts,
                        'reclaimed', true
                    ),
                    finished_at = CASE
                        WHEN attempts >= max_attempts THEN now()
                        ELSE NULL
                    END,
                    started_at = CASE
                        WHEN attempts >= max_attempts THEN started_at
                        ELSE NULL
                    END,
                    updated_at = now()
                WHERE status = 'running'
                  AND started_at IS NOT NULL
                  AND started_at < now() - make_interval(secs => %s)
                  {kind_sql}
                RETURNING id, status
                """,
                tuple(params),
            )
            rows = cur.fetchall() or []
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    pending = 0
    failed = 0
    for row in rows:
        status = row[1] if not isinstance(row, Mapping) else row.get("status")
        if status == "failed":
            failed += 1
        else:
            pending += 1
    return {"reclaimed": pending + failed, "pending": pending, "failed": failed}


def mark_failed(
    conn: _Connection,
    job_id: int,
    error_msg: str,
    *,
    attempts: int,
    max_attempts: int,
) -> str:
    """Mark job failed permanently or re-queue as pending for retry.

    Returns the new status: ``failed`` or ``pending``.
    """
    new_status = "failed" if int(attempts) >= int(max_attempts) else "pending"
    error_body = {"error": str(error_msg), "attempts": int(attempts)}
    finished_sql = "now()" if new_status == "failed" else "NULL"
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE data_ops.job_ingest
                SET status = %s,
                    result = %s::jsonb,
                    finished_at = {finished_sql},
                    updated_at = now()
                WHERE id = %s
                """,
                (new_status, json.dumps(error_body), int(job_id)),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return new_status
