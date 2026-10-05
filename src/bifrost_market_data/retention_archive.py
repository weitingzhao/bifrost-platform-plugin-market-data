"""Archive before delete: rows past the window go to Parquet on the NAS, then leave.

Three tables cannot be fetched again once the trim expires them (phase 0 W3,
Owner 2026-10-05): ``option_snapshot`` is the vendor's IV / Greeks / OI as they
were observed, and ``option_daily`` / ``short_volume`` drop out of the vendor's
own two-year window on the same night they drop out of ours. Deleting them is
fine — the database should not grow without bound — but only once a copy exists.

One range of whole days at a time (never across a month), in one REPEATABLE
READ transaction:

1. read the range's rows past the window through a server-side cursor and write
   them to ``<name>.partial`` as Parquet;
2. read the file back: its row count must equal what was read;
3. ``DELETE`` with the same predicate — under the same snapshot, so it can only
   remove rows that were exported, and its rowcount must match too (a row
   inserted into that range after the snapshot is invisible, stays, and is picked
   up by a later night);
4. rename the file into place, commit, then write the manifest and a ledger line.

Any mismatch or I/O error rolls back: the rows stay in the database and the
partial file is removed. A crash between the rename and the commit leaves a file
whose rows were not deleted; the next night exports them again into a new file,
so the archive can hold a row twice but never miss one. Readers dedupe on the
table's primary key.

Every export is a new file (``<first>_<last>__<UTC stamp>.parquet``): a late row
for a day that was already archived must not overwrite the earlier file.

The archive is refused, and nothing is deleted, unless the directory is a mount
point (the ``nfs-cold`` PVC) — an empty directory in the container's own layer
would accept the files and lose them with the pod.

Layout under the archive root::

    raw_market/<table>/<kind>/<YYYY>/<first>_<last>__<YYYYMMDDTHHMMSSZ>.parquet
    raw_market/<table>/<kind>/<YYYY>/<first>_<last>__<YYYYMMDDTHHMMSSZ>.json
    raw_market/_ledger.jsonl
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from time import monotonic
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_ARCHIVE_DIR = "/archive"
#: Rows per fetch from the server-side cursor and per Parquet row group.
#: The API pod has 512 MiB; a snapshot session is ~270,000 rows.
FETCH_ROWS = 50_000
ARCHIVE_STATEMENT_TIMEOUT = "900s"


@dataclass(frozen=True)
class TableSpec:
    """How one table is cut into days. All names are constants, never input."""

    table: str
    date_column: str
    #: ``date`` columns compare to a day directly; ``timestamptz`` columns are cut
    #: into UTC days.
    timestamp: bool = False


TABLES: Mapping[str, TableSpec] = {
    "raw_market.option_daily": TableSpec("raw_market.option_daily", "bar_date"),
    "raw_market.short_volume": TableSpec("raw_market.short_volume", "period_date"),
    "raw_market.option_snapshot": TableSpec(
        "raw_market.option_snapshot", "snapshot_ts", timestamp=True
    ),
}

#: Snapshot rows that are not the 16:00 New York EOD anchor. Same rule as the
#: intraday trim in ``scheduler.enqueue``.
INTRADAY_ONLY = "(snapshot_ts AT TIME ZONE 'America/New_York')::time <> time '16:00'"


@dataclass(frozen=True)
class RetentionArchive:
    root: Path
    tables: frozenset[str]
    require_mount: bool = True

    def unavailable_reason(self) -> str | None:
        """Why nothing may be archived (and so nothing deleted) right now, or None."""
        root = self.root
        if not root.is_dir():
            return f"archive dir {root} does not exist"
        if self.require_mount and not os.path.ismount(root):
            return f"archive dir {root} is not a mount point (PVC not mounted?)"
        if not os.access(root, os.W_OK | os.X_OK):
            return f"archive dir {root} is not writable"
        return None


def resolve_archive(scfg: Mapping[str, Any]) -> RetentionArchive | None:
    """``retention_archive`` from the trim slot, or None when archiving is off.

    ``{dir: /archive, tables: [raw_market.option_daily, ...], require_mount: true}``.
    Unknown table names are logged and ignored, as ``retention_hold`` does.
    """
    raw = scfg.get("retention_archive")
    if not isinstance(raw, Mapping):
        return None
    names = raw.get("tables") or ()
    if isinstance(names, str):
        names = (names,)
    wanted = {str(x).strip() for x in names if str(x).strip()}
    unknown = wanted - set(TABLES)
    if unknown:
        logger.warning("retention_archive names tables it cannot archive: %s", sorted(unknown))
    wanted &= set(TABLES)
    if not wanted:
        return None
    return RetentionArchive(
        root=Path(str(raw.get("dir") or DEFAULT_ARCHIVE_DIR)),
        tables=frozenset(wanted),
        require_mount=bool(raw.get("require_mount", True)),
    )


@dataclass
class ArchiveRun:
    """What one table's pass did; returned in the trim result."""

    days: int = 0
    rows: int = 0
    files: list[str] = field(default_factory=list)
    cutoff: str | None = None
    error: str | None = None
    budget_exhausted: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "days": self.days,
            "rows": self.rows,
            "files": self.files,
            "cutoff": self.cutoff,
            "error": self.error,
            "budget_exhausted": self.budget_exhausted,
        }


class ArchiveMismatch(RuntimeError):
    """The file, the read and the delete disagree on how many rows there were."""


# ── Postgres type → Arrow type ──────────────────────────────────────────────


def _arrow_type(data_type: str) -> Any | None:
    import pyarrow as pa

    return {
        "text": pa.string(),
        "character varying": pa.string(),
        "character": pa.string(),
        "smallint": pa.int16(),
        "integer": pa.int32(),
        "bigint": pa.int64(),
        "real": pa.float32(),
        "double precision": pa.float64(),
        "boolean": pa.bool_(),
        "date": pa.date32(),
        "timestamp with time zone": pa.timestamp("us", tz="UTC"),
        "timestamp without time zone": pa.timestamp("us"),
    }.get(data_type)


def table_columns(conn: Any, table: str) -> tuple[list[str], Any]:
    """The SELECT list and the Arrow schema for ``table``.

    Types without an exact Arrow match (numeric, json, jsonb, arrays) are cast
    to text in the SELECT so nothing is lost or guessed: numeric keeps every
    digit, jsonb keeps its canonical text.
    """
    import pyarrow as pa

    schema_name, table_name = table.split(".", 1)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            (schema_name, table_name),
        )
        rows = cur.fetchall()
    if not rows:
        raise RuntimeError(f"{table} has no columns visible to this role")
    select: list[str] = []
    fields: list[Any] = []
    for row in rows:
        name, data_type = (
            (row["column_name"], row["data_type"]) if isinstance(row, Mapping) else row
        )
        arrow = _arrow_type(str(data_type))
        quoted = '"' + str(name).replace('"', '""') + '"'
        if arrow is None:
            select.append(f"{quoted}::text AS {quoted}")
            arrow = pa.string()
        else:
            select.append(quoted)
        fields.append(pa.field(str(name), arrow, metadata={"pg_type": str(data_type)}))
    return select, pa.schema(fields)


# ── The archive pass ────────────────────────────────────────────────────────


def _cutoff_day(conn: Any, spec: TableSpec, keep_days: int, *, month_floor: bool) -> date:
    """First day that is kept. Every day before it is expired in full.

    Date tables use the trim's own cutoff — ``date_trunc('month', CURRENT_DATE -
    keep_days)`` — so archiving changes where rows go, not when. Timestamp tables
    use ``now() - keep_days`` floored to the UTC day, so a day is never split
    between two nights (which would put one day in two files for no reason).
    """
    if spec.timestamp:
        sql = "SELECT ((now() - make_interval(days => %s)) AT TIME ZONE 'UTC')::date"
    elif month_floor:
        sql = "SELECT date_trunc('month', CURRENT_DATE - make_interval(days => %s))::date"
    else:
        sql = "SELECT (CURRENT_DATE - make_interval(days => %s))::date"
    with conn.cursor() as cur:
        cur.execute(sql, (int(keep_days),))
        row = cur.fetchone()
    value = next(iter(row.values())) if isinstance(row, Mapping) else row[0]
    conn.commit()
    return value if isinstance(value, date) else date.fromisoformat(str(value)[:10])


def _oldest_day(conn: Any, spec: TableSpec, before: date, extra: str) -> date | None:
    col = spec.date_column
    if spec.timestamp:
        sql = (
            f"SELECT (min({col}) AT TIME ZONE 'UTC')::date FROM {spec.table} "
            f"WHERE {col} < %s::date::timestamp AT TIME ZONE 'UTC' {extra}"
        )
    else:
        sql = f"SELECT min({col}) FROM {spec.table} WHERE {col} < %s {extra}"
    with conn.cursor() as cur:
        cur.execute(f"SET LOCAL statement_timeout = '{ARCHIVE_STATEMENT_TIMEOUT}'")
        cur.execute(sql, (before,))
        row = cur.fetchone()
    conn.commit()
    value = (
        None if row is None else (next(iter(row.values())) if isinstance(row, Mapping) else row[0])
    )
    if value is None:
        return None
    return value if isinstance(value, date) else date.fromisoformat(str(value)[:10])


def _range_predicate(spec: TableSpec, extra: str) -> str:
    """``[start, end)`` on the date column; UTC midnight bounds for a timestamp."""
    col = spec.date_column
    if spec.timestamp:
        base = (
            f"{col} >= %(start)s::date::timestamp AT TIME ZONE 'UTC' "
            f"AND {col} < %(end)s::date::timestamp AT TIME ZONE 'UTC'"
        )
    else:
        base = f"{col} >= %(start)s AND {col} < %(end)s"
    return f"{base} {extra}"


def _chunk_end(start: date, cutoff: date, span_days: int) -> date:
    """End (exclusive) of the range archived in one transaction.

    Never crosses the cutoff or a calendar month, so one file never spans two
    partitions and a month-floored cutoff moves exactly one chunk at a time.
    """
    next_month = (start.replace(day=1) + timedelta(days=32)).replace(day=1)
    return min(cutoff, next_month, start + timedelta(days=max(1, span_days)))


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def archive_range(
    conn: Any,
    archive: RetentionArchive,
    spec: TableSpec,
    start: date,
    end: date,
    *,
    kind: str,
    extra: str = "",
    select: Sequence[str],
    schema: Any,
    reason: str,
) -> tuple[int, Path | None]:
    """Export the rows in ``[start, end)``, delete exactly those, commit. Rows, file.

    One scan per range, not per day: ``option_daily_default`` — the partition
    that holds the oldest year — has no index that leads with ``bar_date``, so a
    per-day query would read the whole partition once for every day.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    table_name = spec.table.split(".", 1)[1]
    folder = archive.root / "raw_market" / table_name / kind / f"{start:%Y}"
    last = end - timedelta(days=1)
    final = folder / f"{start.isoformat()}_{last.isoformat()}__{stamp}.parquet"
    partial = final.with_name(final.name + ".partial")
    where = _range_predicate(spec, extra)
    params = {"start": start, "end": end}
    names = [f.name for f in schema]
    date_index = names.index(spec.date_column)
    per_day: dict[str, int] = {}
    rows = 0
    writer = None
    try:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cur.execute(f"SET LOCAL statement_timeout = '{ARCHIVE_STATEMENT_TIMEOUT}'")
        with conn.cursor(name=f"archive_{table_name}_{start:%Y%m%d}") as scur:
            scur.execute(f"SELECT {', '.join(select)} FROM {spec.table} WHERE {where}", params)
            while True:
                batch = scur.fetchmany(FETCH_ROWS)
                if not batch:
                    break
                if isinstance(batch[0], Mapping):
                    columns = [[r[n] for r in batch] for n in names]
                else:
                    columns = [list(c) for c in zip(*batch, strict=True)]
                for v in columns[date_index]:
                    d = v.astimezone(UTC).date() if isinstance(v, datetime) else v
                    key = d.isoformat() if isinstance(d, date) else str(d)
                    per_day[key] = per_day.get(key, 0) + 1
                if writer is None:
                    folder.mkdir(parents=True, exist_ok=True)
                    writer = pq.ParquetWriter(str(partial), schema, compression="zstd")
                writer.write_table(
                    pa.Table.from_arrays(
                        [pa.array(c, type=f.type) for c, f in zip(columns, schema, strict=True)],
                        schema=schema,
                    )
                )
                rows += len(batch)
        if rows == 0:
            conn.rollback()
            return 0, None
        assert writer is not None
        writer.close()
        writer = None
        with partial.open("rb") as f:
            os.fsync(f.fileno())
        written = pq.ParquetFile(str(partial)).metadata.num_rows
        if written != rows:
            raise ArchiveMismatch(f"{spec.table} {start}..{last}: read {rows}, file has {written}")
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {spec.table} WHERE {where}", params)
            deleted = int(getattr(cur, "rowcount", -1))
        if deleted != rows:
            raise ArchiveMismatch(
                f"{spec.table} {start}..{last}: archived {rows}, delete matched {deleted}"
            )
        os.replace(partial, final)
        _fsync_dir(folder)
        try:
            conn.commit()
        except Exception:
            # The rows are still in the database; the file would be a second copy.
            final.unlink(missing_ok=True)
            raise
    except Exception:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                logger.debug("closing a failed archive writer", exc_info=True)
        partial.unlink(missing_ok=True)
        try:
            conn.rollback()
        except Exception:
            logger.debug("rollback after a failed archive", exc_info=True)
        raise

    manifest = {
        "table": spec.table,
        "kind": kind,
        "start": start.isoformat(),
        "end_exclusive": end.isoformat(),
        "rows": rows,
        "rows_per_day": dict(sorted(per_day.items())),
        "sha256": _sha256(final),
        "bytes": final.stat().st_size,
        "file": final.name,
        "columns": [
            {"name": f.name, "pg_type": (f.metadata or {}).get(b"pg_type", b"").decode()}
            for f in schema
        ],
        "predicate": where,
        "params": {"start": start.isoformat(), "end": end.isoformat()},
        "reason": reason,
        "archived_at": datetime.now(UTC).isoformat(),
    }
    final.with_name(final.name.removesuffix(".parquet") + ".json").write_text(
        json.dumps(manifest, indent=2) + "\n"
    )
    ledger = archive.root / "raw_market" / "_ledger.jsonl"
    keys = ("table", "kind", "start", "end_exclusive", "rows", "sha256", "file", "archived_at")
    with ledger.open("a", encoding="utf-8") as f:
        f.write(json.dumps({k: manifest[k] for k in keys}) + "\n")
    return rows, final


def archive_past_window(
    conn: Any,
    archive: RetentionArchive,
    table: str,
    *,
    keep_days: int,
    kind: str = "all",
    extra: str = "",
    month_floor: bool = True,
    span_days: int = 31,
    budget_sec: float = 60.0,
) -> ArchiveRun:
    """Archive then delete every whole day of ``table`` past ``keep_days``.

    ``span_days`` bounds one transaction: a calendar month for the dated tables,
    whose cutoff moves a month at a time; a few days for snapshots, a quarter of a
    million rows a session, so a backlog is worked off in pieces.

    Stops at the first failure: the remaining days stay in the database and are
    tried again on the next run. Never raises; the error is in the result.
    """
    run = ArchiveRun()
    spec = TABLES[table]
    reason = f"past {keep_days}d window ({kind})"
    started = monotonic()
    try:
        unavailable = archive.unavailable_reason()
        if unavailable:
            run.error = unavailable
            return run
        cutoff = _cutoff_day(conn, spec, keep_days, month_floor=month_floor)
        run.cutoff = cutoff.isoformat()
        day = _oldest_day(conn, spec, cutoff, extra)
        if day is None:
            return run
        select, schema = table_columns(conn, spec.table)
        conn.commit()
        while day < cutoff:
            if monotonic() - started >= budget_sec:
                run.budget_exhausted = True
                break
            end = _chunk_end(day, cutoff, span_days)
            rows, path = archive_range(
                conn,
                archive,
                spec,
                day,
                end,
                kind=kind,
                extra=extra,
                select=select,
                schema=schema,
                reason=reason,
            )
            if rows:
                run.days += (end - day).days
                run.rows += rows
                if path is not None:
                    run.files.append(str(path.relative_to(archive.root)))
            day = end
    except Exception as exc:  # noqa: BLE001 — a failed night keeps the rows
        run.error = f"{type(exc).__name__}: {str(exc).splitlines()[0] if str(exc) else ''}"
        logger.warning("retention archive %s (%s) stopped: %s", table, kind, run.error)
        try:
            conn.rollback()
        except Exception:
            logger.debug("rollback after a stopped archive pass", exc_info=True)
    if run.rows:
        logger.info(
            "archived and deleted %s rows over %s days from %s (%s)",
            run.rows,
            run.days,
            table,
            kind,
        )
    return run
