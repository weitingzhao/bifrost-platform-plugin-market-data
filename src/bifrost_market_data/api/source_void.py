"""Source-void (gap acknowledgment) CRUD — Golden Source ops_jobs.data_source_void.

Migrated from Trade ``public.preference_data_gap_ack``. Operator marks a
fundamentals data_type as permanently unavailable from the vendor ("Source N/A").
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException

from bifrost_market_data.api.deps import iso_value, require_db, require_write_token, table_exists

router = APIRouter(prefix="/readiness", tags=["readiness-source-void"])

VALID_DATA_TYPES = frozenset(
    (
        "income_statements",
        "balance_sheets",
        "cash_flows",
        "ratios",
        "short_interest",
        "short_volume",
    )
)


def _row_to_entry(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        data_type = str(row.get("data_type") or "")
        is_void = bool(row.get("is_void"))
        acked = row.get("acked_gap_count")
        note = row.get("note")
        updated = row.get("updated_at")
    else:
        data_type = str(row[0] or "")
        is_void = bool(row[1])
        acked = row[2]
        note = row[3]
        updated = row[4]
    return {
        "data_type": data_type,
        "is_void": is_void,
        "acked_gap_count": int(acked) if acked is not None else None,
        "note": note,
        "void_reason": note,  # Trade FE / API alias
        "updated_at": iso_value(updated) if updated is not None else None,
        "acked_at": iso_value(updated) if updated is not None else None,
    }


def query_all_voids(conn: Any) -> dict[str, dict[str, Any]]:
    """Return ``{ data_type: { is_void, acked_gap_count, note, updated_at } }``."""
    if not table_exists(conn, "ops_jobs", "data_source_void"):
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT data_type, is_void, acked_gap_count, note, updated_at
            FROM ops_jobs.data_source_void
            ORDER BY data_type
            """
        )
        rows = cur.fetchall() or []
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        entry = _row_to_entry(row)
        dt = entry["data_type"]
        if dt:
            out[dt] = {
                "is_void": entry["is_void"],
                "acked_gap_count": entry["acked_gap_count"],
                "note": entry["note"],
                "void_reason": entry["void_reason"],
                "updated_at": entry["updated_at"],
                "acked_at": entry["acked_at"],
            }
    return out


def upsert_void(
    conn: Any,
    *,
    data_type: str,
    is_void: bool,
    gap_count: int | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    """Upsert one data_type row; ``gap_count`` maps to ``acked_gap_count``."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops_jobs.data_source_void
                (data_type, is_void, acked_gap_count, note, updated_at)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (data_type) DO UPDATE SET
                is_void = EXCLUDED.is_void,
                acked_gap_count = EXCLUDED.acked_gap_count,
                note = EXCLUDED.note,
                updated_at = now()
            RETURNING data_type, is_void, acked_gap_count, note, updated_at
            """,
            (data_type, is_void, gap_count, note),
        )
        row = cur.fetchone()
    conn.commit()
    return _row_to_entry(row)


@router.get("/source-void")
def get_source_void() -> dict[str, Any]:
    """List all source-void acknowledgments as a dict keyed by data_type."""
    conn = require_db()
    try:
        voids = query_all_voids(conn)
        acks = [
            {"data_type": dt, **vals}
            for dt, vals in sorted(voids.items())
        ]
        return {"ok": True, "voids": voids, "acks": acks}
    finally:
        conn.close()


@router.post("/source-void", dependencies=[Depends(require_write_token)])
def post_source_void(body: dict[str, Any] = Body(...)) -> dict[str, Any]:
    """Upsert source-void ack. Body: ``{ data_type, is_void, gap_count?, note? }``.

    ``gap_count`` is stored as ``acked_gap_count`` (Trade ``postSepaGapAck`` compat).
    ``void_reason`` is accepted as an alias for ``note``.
    """
    data_type = str(body.get("data_type") or "").strip()
    if data_type not in VALID_DATA_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid data_type: {data_type!r}; allowed: {sorted(VALID_DATA_TYPES)}",
        )
    is_void = bool(body.get("is_void", False))
    try:
        gap_count = max(0, int(body.get("gap_count") or 0))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="gap_count must be an integer") from exc
    note = body.get("note") or body.get("void_reason") or None
    if note is not None:
        note = str(note)

    conn = require_db()
    try:
        if not table_exists(conn, "ops_jobs", "data_source_void"):
            raise HTTPException(
                status_code=503,
                detail="ops_jobs.data_source_void not applied — run db-init",
            )
        entry = upsert_void(
            conn,
            data_type=data_type,
            is_void=is_void,
            gap_count=gap_count,
            note=note,
        )
        return {
            "ok": True,
            "data_type": entry["data_type"],
            "is_void": entry["is_void"],
            "acked_gap_count": entry["acked_gap_count"],
            "note": entry["note"],
            "void_reason": entry["void_reason"],
            "updated_at": entry["updated_at"],
        }
    finally:
        conn.close()
