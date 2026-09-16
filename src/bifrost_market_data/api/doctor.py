"""GET /market/doctor · POST /market/doctor/heal — check now, fix now."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query

from bifrost_market_data.api.deps import connect_db, require_write_token, resolve_polygon_api_key
from bifrost_market_data.api.slow_cache import BackgroundCache
from bifrost_market_data.config import load_config
from bifrost_market_data.doctor import heal, probe_vendor, probe_worker_health, run_doctor
from bifrost_market_data.scheduler.daily import resolve_scheduler_cfg

router = APIRouter(prefix="/doctor", tags=["doctor"])

#: The report is a few kB per key and there are two keys, so this costs nothing
#: against the pod's 512Mi — unlike recomputing it on every Console poll.
DOCTOR_CACHE = BackgroundCache("doctor")

#: Same shape, no findings: a caller can tell "still computing" from "computed,
#: and nothing is wrong".
_DOCTOR_EMPTY: dict[str, Any] = {"ok": True, "findings": [], "generated_at": None}


def _scheduler_cfg() -> dict[str, Any]:
    scheduler_cfg = resolve_scheduler_cfg()
    scheduler_cfg["worker"] = dict((load_config() or {}).get("worker") or {})
    return scheduler_cfg


def _db() -> Any:
    try:
        return connect_db(statement_timeout="120s")
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc


def _doctor_payload(probes: bool) -> dict[str, Any]:
    conn = _db()
    try:
        worker_health = probe_worker_health() if probes else None
        vendor = None
        if probes:
            cfg = load_config()
            poly = dict(cfg.get("polygon") or {})
            try:
                key = resolve_polygon_api_key()
            except Exception:  # noqa: BLE001 — no key is itself a finding
                key = ""
            vendor = probe_vendor(key, rest_base=str(poly.get("rest_base") or "https://api.polygon.io"))
        report = run_doctor(
            conn, scheduler_cfg=_scheduler_cfg(), worker_health=worker_health, vendor=vendor
        )
    finally:
        conn.close()
    # The age matters as much as the verdict: a cached report has to say when it
    # was produced, or a reader cannot tell a healed system from a stale answer.
    report["generated_at"] = datetime.now(timezone.utc).isoformat()
    report["probes"] = probes
    return report


@router.get("")
def doctor_report(
    probes: bool = True,
    refresh: bool = Query(False, description="recompute instead of reading the cached report"),
) -> dict[str, Any]:
    """Session completeness, staleness, failed jobs, workers and vendor — with prescriptions.

    The report runs a session-completeness pass per dataset plus worker and vendor
    probes, so it costs seconds, and every Console poll used to pay for all of it.
    The cached answer comes back at once with ``age_sec`` and ``generated_at``, a
    recompute starts behind it, and ``?refresh=true`` waits for a fresh one.

    Keyed by ``probes``: a report that skipped the probes is not an answer to the
    question that asked for them.
    """
    key = f"probes={bool(probes)}"
    compute = lambda: _doctor_payload(bool(probes))  # noqa: E731 — one expression, named by key
    if refresh:
        return DOCTOR_CACHE.compute_now(key, compute)
    return DOCTOR_CACHE.read(key, compute, empty=dict(_DOCTOR_EMPTY))


@router.post("/heal", dependencies=[Depends(require_write_token)])
def doctor_heal(body: dict[str, Any] | None = Body(None)) -> dict[str, Any]:
    """Execute the prescriptions. Body: ``{ "dry_run": false, "finding_ids": [...] }``."""
    body = body or {}
    ids = body.get("finding_ids")
    if ids is not None and not isinstance(ids, list):
        raise HTTPException(status_code=400, detail="finding_ids must be a list")
    conn = _db()
    try:
        return heal(
            conn,
            scheduler_cfg=_scheduler_cfg(),
            finding_ids=[str(i) for i in ids] if ids else None,
            dry_run=bool(body.get("dry_run", False)),
        )
    finally:
        conn.close()
