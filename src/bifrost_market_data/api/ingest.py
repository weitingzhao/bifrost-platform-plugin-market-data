"""Ingest job enqueue + status routes (D15=A — write ops_jobs.job_ingest directly)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query

from bifrost_market_data.api.deps import iso_value, require_db, require_write_token, row_dict
from bifrost_market_data.ingest import raw_handler_kinds
from bifrost_market_data.scheduler.enqueue import insert_job

router = APIRouter(prefix="/ingest", tags=["ingest"])

ALLOWED_KINDS = frozenset(raw_handler_kinds())


def _job_row_to_api(row: Mapping[str, Any] | dict[str, Any]) -> dict[str, Any]:
    cols = (
        "id",
        "kind",
        "payload",
        "payload_hash",
        "priority",
        "status",
        "result",
        "attempts",
        "max_attempts",
        "created_at",
        "updated_at",
        "started_at",
        "finished_at",
    )
    out = row_dict(row, cols)
    if "id" in out and out["id"] is not None:
        out["id"] = int(out["id"])
        out["job_id"] = str(out["id"])
    for key in ("created_at", "updated_at", "started_at", "finished_at"):
        if key in out and out[key] is not None and not isinstance(out[key], str):
            out[key] = iso_value(out[key])
    return out


def get_job(conn: Any, job_id: int) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, kind, payload, payload_hash, priority, status, result,
                   attempts, max_attempts, created_at, updated_at,
                   started_at, finished_at
            FROM ops_jobs.job_ingest
            WHERE id = %s
            """,
            (int(job_id),),
        )
        row = cur.fetchone()
    if row is None:
        return None
    cols = (
        "id",
        "kind",
        "payload",
        "payload_hash",
        "priority",
        "status",
        "result",
        "attempts",
        "max_attempts",
        "created_at",
        "updated_at",
        "started_at",
        "finished_at",
    )
    if isinstance(row, Mapping):
        return _job_row_to_api(dict(row))
    return _job_row_to_api({cols[i]: row[i] for i in range(len(cols))})


@router.post("/enqueue", dependencies=[Depends(require_write_token)])
def enqueue_job(
    body: dict[str, Any] = Body(...),
) -> dict[str, Any]:
    """Insert a pending job into ``ops_jobs.job_ingest`` (no Celery — D15=A).

    Body: ``{ "kind": "<handler kind>", "payload": {...}, "priority": 0 }``
    """
    kind = str(body.get("kind") or "").strip()
    if not kind:
        raise HTTPException(status_code=400, detail="kind is required")
    if kind not in ALLOWED_KINDS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid kind; allowed: {sorted(ALLOWED_KINDS)}",
        )
    payload = body.get("payload") if isinstance(body.get("payload"), dict) else {}
    try:
        priority = int(body.get("priority") or 0)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="priority must be an integer") from exc
    try:
        max_attempts = int(body.get("max_attempts") or 3)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="max_attempts must be an integer") from exc

    conn = require_db()
    try:
        job_id = insert_job(
            conn,
            kind=kind,
            payload=payload,
            priority=priority,
            max_attempts=max_attempts,
        )
        if job_id is None:
            # Deduped — find existing pending/running job
            from bifrost_market_data.scheduler.enqueue import payload_hash

            ph = payload_hash(payload)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id FROM ops_jobs.job_ingest
                    WHERE kind = %s AND payload_hash = %s
                      AND status IN ('pending', 'running')
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (kind, ph),
                )
                existing = cur.fetchone()
            if existing is not None:
                eid = int(existing[0] if not isinstance(existing, Mapping) else existing["id"])
                return {
                    "ok": True,
                    "job_id": str(eid),
                    "deduplicated": True,
                    "kind": kind,
                }
            return {"ok": False, "error": "enqueue failed (dedup, job not found)"}
        return {
            "ok": True,
            "job_id": str(job_id),
            "deduplicated": False,
            "kind": kind,
        }
    finally:
        conn.close()


@router.get("/jobs/{job_id}")
def get_ingest_job(
    job_id: str = Path(..., description="Job id (numeric)"),
) -> dict[str, Any]:
    """Return a single ``ops_jobs.job_ingest`` row."""
    try:
        jid = int(str(job_id).strip())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="job_id must be numeric") from exc

    conn = require_db()
    try:
        job = get_job(conn, jid)
    finally:
        conn.close()
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return {"ok": True, "job": job}


@router.get("/jobs")
def list_ingest_jobs(
    status: str | None = Query(None, description="Filter by status"),
    kind: str | None = Query(None, description="Filter by kind"),
    limit: int = Query(50, ge=1, le=500),
) -> dict[str, Any]:
    """List recent ingest jobs (newest first)."""
    clauses: list[str] = []
    params: list[Any] = []
    if status:
        clauses.append("status = %s")
        params.append(str(status).strip().lower())
    if kind:
        clauses.append("kind = %s")
        params.append(str(kind).strip())
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    conn = require_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, kind, payload, payload_hash, priority, status, result,
                       attempts, max_attempts, created_at, updated_at,
                       started_at, finished_at
                FROM ops_jobs.job_ingest
                {where}
                ORDER BY id DESC
                LIMIT %s
                """,
                (*params, int(limit)),
            )
            raw = cur.fetchall() or []
        cols = (
            "id",
            "kind",
            "payload",
            "payload_hash",
            "priority",
            "status",
            "result",
            "attempts",
            "max_attempts",
            "created_at",
            "updated_at",
            "started_at",
            "finished_at",
        )
        jobs = []
        for r in raw:
            if isinstance(r, Mapping):
                jobs.append(_job_row_to_api(dict(r)))
            else:
                jobs.append(_job_row_to_api({cols[i]: r[i] for i in range(len(cols))}))
    finally:
        conn.close()
    return {
        "ok": True,
        "jobs": jobs,
        "count": len(jobs),
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


@router.get("/queue-summary")
def ingest_queue_summary() -> dict[str, Any]:
    """Pending + running counts by kind (full queue, not latest-N jobs)."""
    conn = require_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT kind, status, COUNT(*)::bigint AS n
                FROM ops_jobs.job_ingest
                WHERE status IN ('pending', 'running')
                GROUP BY kind, status
                ORDER BY kind, status
                """
            )
            raw = cur.fetchall() or []
    finally:
        conn.close()
    by_kind: dict[str, dict[str, int]] = {}
    pending_total = 0
    running_total = 0
    for r in raw:
        if isinstance(r, Mapping):
            kind = str(r.get("kind") or "")
            status = str(r.get("status") or "")
            n = int(r.get("n") or 0)
        else:
            kind = str(r[0] or "")
            status = str(r[1] or "")
            n = int(r[2] or 0)
        bucket = by_kind.setdefault(kind, {"pending": 0, "running": 0})
        if status == "pending":
            bucket["pending"] += n
            pending_total += n
        elif status == "running":
            bucket["running"] += n
            running_total += n
    kinds = [
        {
            "kind": kind,
            "pending": vals["pending"],
            "running": vals["running"],
            "active": vals["pending"] + vals["running"],
        }
        for kind, vals in sorted(by_kind.items())
    ]
    return {
        "ok": True,
        "pending": pending_total,
        "running": running_total,
        "active": pending_total + running_total,
        "kinds": kinds,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


@router.get("/queue-dashboard")
def ingest_queue_dashboard(
    grace_minutes: int = Query(45, ge=5, le=240),
) -> dict[str, Any]:
    """Queue composition + Cron schedule plan + plan-vs-actual adherence."""
    from bifrost_market_data.api.ingest_dashboard import build_queue_dashboard

    conn = require_db()
    try:
        return build_queue_dashboard(conn, grace_minutes=int(grace_minutes))
    finally:
        conn.close()



@router.get("/kinds")
def list_ingest_kinds() -> dict[str, Any]:
    """List worker handler kinds accepted by POST /market/ingest/enqueue."""
    return {"ok": True, "kinds": sorted(ALLOWED_KINDS)}
