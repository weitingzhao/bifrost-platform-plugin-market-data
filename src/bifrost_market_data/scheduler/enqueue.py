"""Job enqueue helpers: payload_hash, insert with dedup, trim old jobs."""

from __future__ import annotations

import hashlib
import json
from time import monotonic
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


#: Rows per INSERT statement. Sized so a statement stays well inside a few
#: hundred milliseconds on the job table at its current size.
INSERT_CHUNK_ROWS = 2000


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
    # One transaction, one commit — but not one statement. The whole option
    # universe backfill is 575 underlyings × 24 months of planner rows, and a
    # single 14,000-row INSERT with ON CONFLICT over an 860,000-row table ran
    # past the role's 2s statement_timeout and was cancelled with nothing
    # written (2026-09-08). Chunks keep each statement short; SET LOCAL gives
    # the transaction room the role's default does not.
    rows: list[Any] = []
    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '60s'")
            for start in range(0, len(kinds), INSERT_CHUNK_ROWS):
                end = start + INSERT_CHUNK_ROWS
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
                    (kinds[start:end], payloads[start:end], hashes[start:end], priorities[start:end], max_attempts[start:end]),
                )
                rows.extend(cur.fetchall() or [])
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


#: Rows per delete statement. Measured 2026-09-09 against ops_jobs.job_ingest
#: with its nine indexes: 20,000 rows in 0.51s, which is the knee — 10,000 costs
#: nearly as much per statement and 50,000 costs more per row.
TRIM_BATCH_ROWS = 20000
#: Wall-clock budget for one trim. The API connection allows 60s; a trim that
#: cannot finish must stop having made progress, not be cancelled having made
#: none. The scheduler CLI has no gateway in front of it and passes a larger
#: one, since a backfill day can leave millions of rows to clear.
TRIM_BUDGET_SEC = 45.0
#: How long finished rows are kept. A window, not a row count: the count was
#: 40,000, which held about seven days of a normal day's jobs and about fifteen
#: minutes once throughput reached 2,700 a minute — so every check that said
#: "in the last day" was reading a quarter of an hour. Slot adherence needs the
#: evidence to outlive roughly thirty hours, hence two days.
TRIM_KEEP_HOURS = 48.0
#: Absolute backstop, not a retention policy. It exists only so a runaway
#: producer cannot grow the table without bound between two trims; at the
#: measured 39,000 deletes a second it is a few minutes of clearing.
TRIM_MAX_ROWS = 5_000_000
#: Per-statement budget, set inside each batch's transaction. The function must
#: not depend on how its caller opened the connection: the scheduler CLI takes
#: the `bifrost` role's 2s default, under which a 20,000-row delete competing
#: with the workers' writes is cancelled every time.
TRIM_STATEMENT_TIMEOUT = "30s"


def _finished_cutoff(conn: _Connection, keep_max: int) -> Any:
    """``finished_at`` of the keep_max-th newest finished job, or None.

    Written to match ``job_ingest_finished_at`` exactly — one column, DESC, and
    no NULLS clause, since DESC already means NULLS FIRST. The previous form
    ordered by ``finished_at DESC NULLS LAST, id DESC``, which no index can
    serve: it seq-scanned 1.27M rows and spilled a 32MB external sort, 14
    seconds before deleting a single row. This answers in 60ms.
    """
    with conn.cursor() as cur:
        cur.execute(f"SET LOCAL statement_timeout = '{TRIM_STATEMENT_TIMEOUT}'")
        cur.execute(
            """
            SELECT finished_at
            FROM ops_jobs.job_ingest
            WHERE status IN ('done', 'failed')
            ORDER BY finished_at DESC
            OFFSET %s LIMIT 1
            """,
            (int(keep_max),),
        )
        row = cur.fetchone()
    conn.commit()
    if row is None:
        return None
    return row[0] if not isinstance(row, Mapping) else next(iter(row.values()), None)


#: Two fixed statements rather than one assembled at call time: the difference
#: is a whole predicate, and a query that reads the same in the source as it does
#: in the log is worth two constants.
_SNAPSHOT_DELETE_ALL = """
DELETE FROM raw_market.option_snapshot t
WHERE (t.option_ticker, t.snapshot_ts) IN (
    SELECT option_ticker, snapshot_ts
    FROM raw_market.option_snapshot
    WHERE snapshot_ts < now() - make_interval(days => %s)
    LIMIT %s
)
"""

_SNAPSHOT_DELETE_INTRADAY = """
DELETE FROM raw_market.option_snapshot t
WHERE (t.option_ticker, t.snapshot_ts) IN (
    SELECT option_ticker, snapshot_ts
    FROM raw_market.option_snapshot
    WHERE snapshot_ts < now() - make_interval(days => %s)
      AND (snapshot_ts AT TIME ZONE 'America/New_York')::time <> time '16:00'
    LIMIT %s
)
"""

#: Rows per intraday-snapshot delete statement. Smaller than the job batch: each
#: row is wider and the predicate has to read the whole candidate range.
SNAPSHOT_BATCH_ROWS = 5000


def trim_option_snapshots(
    conn: _Connection,
    *,
    keep_days: int,
    intraday_only: bool = False,
    batch_size: int = SNAPSHOT_BATCH_ROWS,
    budget_sec: float = 60.0,
    statement_timeout: str = TRIM_STATEMENT_TIMEOUT,
) -> int:
    """Delete option snapshots past ``keep_days``; ``intraday_only`` spares the EOD anchor.

    Two windows use this. Intraday rows are the current regime's shape, not
    history — about three times the EOD volume — so they leave on a shorter
    clock; EOD rows sit exactly at 16:00 New York, which is what tells them
    apart, and evaluating that on every candidate row is why one unbounded
    statement took 22 seconds across eighteen partitions and deleted nothing,
    failing under the scheduler CLI's two-second default every night since it
    was written. The longer window then takes everything past retention.

    Deleting rows is not how a partitioned table would prefer to be trimmed;
    dropping the month is. The plugin's role cannot: all eighteen partitions are
    owned by `postgres`. Deleting needs only the DML grant it has, so retention
    happens either way, and the empty partitions left behind are catalogue
    clutter rather than data.

    Bounded and committed per batch, like the job trim, so it makes progress
    within whatever budget it is given. Batches are keyed on the primary key,
    not on ``ctid``: this table is partitioned by month and a ctid identifies a
    row only within one partition — 462,252 of them are shared by more than one
    row here, so a ctid batch would delete rows nobody selected. ``job_ingest``
    is an ordinary table, which is why its trim can use ctid.
    """
    deleted = 0
    started = monotonic()
    try:
        while monotonic() - started < budget_sec:
            with conn.cursor() as cur:
                cur.execute(f"SET LOCAL statement_timeout = '{statement_timeout}'")
                cur.execute(
                    _SNAPSHOT_DELETE_INTRADAY if intraday_only else _SNAPSHOT_DELETE_ALL,
                    (int(keep_days), int(batch_size)),
                )
                n = int(getattr(cur, "rowcount", 0) or 0)
            conn.commit()
            deleted += n
            if n < batch_size:
                break
    except Exception:
        conn.rollback()
        raise
    return deleted


def trim_old_jobs(
    conn: _Connection,
    *,
    keep_hours: float = TRIM_KEEP_HOURS,
    keep_max: int = TRIM_MAX_ROWS,
    batch_size: int = TRIM_BATCH_ROWS,
    budget_sec: float = TRIM_BUDGET_SEC,
) -> int:
    """Delete finished jobs older than ``keep_hours``; ``keep_max`` is a backstop.

    Retention is a window. It used to be a window *and* a row count, and the row
    count did all the work: 40,000 finished rows is about seven days at a normal
    day's volume and about fifteen minutes at 2,700 jobs a minute, so how far
    back the queue could be questioned depended on how busy it had been. Now the
    answer is the same on a quiet day and a backfill day, and ``keep_max`` fires
    only if a producer outruns two trims.

    Both passes delete in bounded, committed batches. The single-statement form
    was fine while the queue was small and stopped working once it was not: the
    row cap had to delete 1.23M rows in one statement, which the API's
    60-second budget cancelled, so trim last completed on 2026-09-08 and 1.26M
    finished rows stayed on the table. Batching means a backlog cannot outgrow
    one statement, and a run that hits its budget still leaves the table
    smaller than it found it.

    Returns the number of rows deleted.
    """
    deleted = 0
    started = monotonic()

    def _delete(sql: str, params: tuple[Any, ...]) -> int:
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL statement_timeout = '{TRIM_STATEMENT_TIMEOUT}'")
            cur.execute(sql, params)
            n = int(getattr(cur, "rowcount", 0) or 0)
        conn.commit()
        return n

    try:
        while monotonic() - started < budget_sec:
            n = _delete(
                """
                DELETE FROM ops_jobs.job_ingest
                WHERE ctid IN (
                    SELECT ctid FROM ops_jobs.job_ingest
                    WHERE status IN ('done', 'failed')
                      AND finished_at IS NOT NULL
                      AND finished_at < now() - make_interval(secs => %s)
                    LIMIT %s
                )
                """,
                (float(keep_hours) * 3600.0, int(batch_size)),
            )
            deleted += n
            if n < batch_size:
                break

        cutoff = _finished_cutoff(conn, keep_max)
        while cutoff is not None and monotonic() - started < budget_sec:
            n = _delete(
                """
                DELETE FROM ops_jobs.job_ingest
                WHERE ctid IN (
                    SELECT ctid FROM ops_jobs.job_ingest
                    WHERE status IN ('done', 'failed')
                      AND finished_at < %s
                    LIMIT %s
                )
                """,
                (cutoff, int(batch_size)),
            )
            deleted += n
            if n < batch_size:
                break
    except Exception:
        conn.rollback()
        raise
    return deleted
