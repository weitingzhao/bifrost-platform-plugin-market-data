"""GET /market/doctor · POST /market/doctor/heal — check now, fix now."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException

from bifrost_market_data.api.deps import connect_db, require_write_token, resolve_polygon_api_key
from bifrost_market_data.config import load_config
from bifrost_market_data.doctor import heal, probe_vendor, probe_worker_health, run_doctor
from bifrost_market_data.scheduler.daily import load_schedule

router = APIRouter(prefix="/doctor", tags=["doctor"])


def _scheduler_cfg() -> dict[str, Any]:
    cfg = load_config()
    schedule = load_schedule()
    scheduler_cfg = dict(schedule.get("scheduler") or {})
    if isinstance(cfg.get("scheduler"), dict):
        scheduler_cfg.update(cfg["scheduler"])
    scheduler_cfg["worker"] = dict(cfg.get("worker") or {})
    return scheduler_cfg


def _db() -> Any:
    try:
        return connect_db(statement_timeout="120s")
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc


@router.get("")
def doctor_report(probes: bool = True) -> dict[str, Any]:
    """Session completeness, staleness, failed jobs, workers and vendor — with prescriptions."""
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
        return run_doctor(conn, scheduler_cfg=_scheduler_cfg(), worker_health=worker_health, vendor=vendor)
    finally:
        conn.close()


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
