"""Job enqueue helpers: payload_hash, insert with dedup, trim old jobs."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping, Protocol


class _Connection(Protocol):
    def cursor(self) -> Any: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...


def payload_hash(payload: Mapping[str, Any] | None) -> str:
    """Deterministic SHA-256 prefix of canonical JSON (16 hex chars)."""
    data = dict(payload or {})
    canonical = json.dumps(data, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def insert_job(
    conn: _Connection,
    *,
    kind: str,
    payload: Mapping[str, Any] | None = None,
    priority: int = 0,
    max_attempts: int = 3,
) -> int | None:
    """Insert a pending job; dedupe via partial unique index on (kind, payload_hash).

    Returns the new job id, or ``None`` if a pending/running duplicate already exists.
    """
    kind_s = str(kind).strip()
    if not kind_s:
        raise ValueError("kind is required")
    body = dict(payload or {})
    ph = payload_hash(body)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO data_ops.job_ingest
                    (kind, payload, payload_hash, priority, status, max_attempts)
                VALUES
                    (%s, %s::jsonb, %s, %s, 'pending', %s)
                ON CONFLICT (kind, payload_hash)
                    WHERE status IN ('pending', 'running') AND payload_hash IS NOT NULL
                DO NOTHING
                RETURNING id
                """,
                (kind_s, json.dumps(body), ph, int(priority), int(max_attempts)),
            )
            row = cur.fetchone()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    if row is None:
        return None
    if isinstance(row, Mapping):
        return int(row["id"])
    return int(row[0])


def trim_old_jobs(
    conn: _Connection,
    *,
    keep_days: int = 7,
    keep_max: int = 5000,
) -> int:
    """Delete finished jobs older than ``keep_days``, then cap total finished rows.

    Returns number of rows deleted.
    """
    deleted = 0
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM data_ops.job_ingest
                WHERE status IN ('done', 'failed')
                  AND finished_at IS NOT NULL
                  AND finished_at < now() - (%s || ' days')::interval
                """,
                (int(keep_days),),
            )
            deleted += int(getattr(cur, "rowcount", 0) or 0)

            cur.execute(
                """
                DELETE FROM data_ops.job_ingest
                WHERE id IN (
                    SELECT id FROM data_ops.job_ingest
                    WHERE status IN ('done', 'failed')
                    ORDER BY finished_at DESC NULLS LAST, id DESC
                    OFFSET %s
                )
                """,
                (int(keep_max),),
            )
            deleted += int(getattr(cur, "rowcount", 0) or 0)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return deleted
