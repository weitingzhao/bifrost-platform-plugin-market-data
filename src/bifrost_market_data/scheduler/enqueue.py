"""Job enqueue helpers: payload_hash, insert with dedup, trim old jobs."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping, Protocol, Sequence


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
                INSERT INTO ops_jobs.job_ingest
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


def insert_jobs_bulk(
    conn: _Connection,
    specs: Sequence[tuple[str, Mapping[str, Any] | None, int, int]],
) -> list[int | None]:
    """Insert many pending jobs in one statement; ``None`` where a pending/running duplicate exists.

    One round trip and one commit for a whole slot (the fundamentals rotate is
    5,000 rows) instead of one commit per row inside an HTTP request.
    Duplicate keys inside the batch are folded onto the first occurrence.
    """
    if not specs:
        return []
    kinds: list[str] = []
    payloads: list[str] = []
    hashes: list[str] = []
    priorities: list[int] = []
    max_attempts: list[int] = []
    key_index: dict[tuple[str, str], int] = {}
    positions: list[tuple[str, str] | None] = []
    for kind, payload, priority, attempts in specs:
        kind_s = str(kind).strip()
        if not kind_s:
            raise ValueError("kind is required")
        body = dict(payload or {})
        ph = payload_hash(body)
        key = (kind_s, ph)
        if key in key_index:
            positions.append(None)  # in-batch duplicate → deduped
            continue
        key_index[key] = len(kinds)
        positions.append(key)
        kinds.append(kind_s)
        payloads.append(json.dumps(body))
        hashes.append(ph)
        priorities.append(int(priority))
        max_attempts.append(int(attempts))
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops_jobs.job_ingest
                    (kind, payload, payload_hash, priority, status, max_attempts)
                SELECT r.kind, r.payload::jsonb, r.payload_hash, r.priority, 'pending', r.max_attempts
                FROM unnest(%s::text[], %s::text[], %s::text[], %s::smallint[], %s::smallint[])
                     AS r(kind, payload, payload_hash, priority, max_attempts)
                ON CONFLICT (kind, payload_hash)
                    WHERE status IN ('pending', 'running') AND payload_hash IS NOT NULL
                DO NOTHING
                RETURNING id, kind, payload_hash
                """,
                (kinds, payloads, hashes, priorities, max_attempts),
            )
            rows = cur.fetchall() or []
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    id_by_key: dict[tuple[str, str], int] = {}
    for row in rows:
        if isinstance(row, Mapping):
            id_by_key[(str(row["kind"]), str(row["payload_hash"]))] = int(row["id"])
        else:
            id_by_key[(str(row[1]), str(row[2]))] = int(row[0])
    return [id_by_key.get(key) if key is not None else None for key in positions]


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
                DELETE FROM ops_jobs.job_ingest
                WHERE status IN ('done', 'failed')
                  AND finished_at IS NOT NULL
                  AND finished_at < now() - (%s || ' days')::interval
                """,
                (int(keep_days),),
            )
            deleted += int(getattr(cur, "rowcount", 0) or 0)

            cur.execute(
                """
                DELETE FROM ops_jobs.job_ingest
                WHERE id IN (
                    SELECT id FROM ops_jobs.job_ingest
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
