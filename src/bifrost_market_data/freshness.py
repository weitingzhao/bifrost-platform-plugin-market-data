"""Best-effort UPSERT into ops_jobs.ingest_freshness after successful ingest jobs."""

from __future__ import annotations

from typing import Any, Mapping

# Jobs that write the same logical table map to one freshness dimension.
_DIMENSION_ALIASES: dict[str, str] = {
    "stock_daily_grouped": "stock_daily",
}


def dimension_for_kind(kind: str) -> str:
    """Map job kind → freshness dimension (PK of ops_jobs.ingest_freshness)."""
    key = str(kind or "").strip()
    return _DIMENSION_ALIASES.get(key, key)


def rows_written_from_result(result: Mapping[str, Any] | None) -> int:
    """Extract rows_written from a handler result dict (default 0)."""
    if not result:
        return 0
    raw = result.get("rows_written")
    if raw is None:
        return 0
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return 0


def update_freshness(
    conn: Any,
    dimension: str,
    rows_written: int,
    *,
    status: str = "ok",
) -> None:
    """UPSERT ``ops_jobs.ingest_freshness`` after a successful job.

    Caller should catch exceptions — freshness must not fail the job.
    """
    dim = str(dimension or "").strip()
    if not dim:
        raise ValueError("dimension is required")
    rows = max(0, int(rows_written))
    status_s = str(status or "ok").strip() or "ok"

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops_jobs.ingest_freshness
                (dimension, last_run_at, rows_written, status, updated_at)
            VALUES (%s, now(), %s, %s, now())
            ON CONFLICT (dimension) DO UPDATE SET
                last_run_at = now(),
                rows_written = EXCLUDED.rows_written,
                status = EXCLUDED.status,
                updated_at = now()
            """,
            (dim, rows, status_s),
        )
    if hasattr(conn, "commit"):
        conn.commit()
