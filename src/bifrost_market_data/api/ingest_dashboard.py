"""Ingest queue + schedule plan / adherence dashboard (ops UI)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from bifrost_market_data.scheduler.cronutil import iso_z, iter_cron_fires, next_fires, previous_fire
from bifrost_market_data.scheduler.daily import load_schedule

# Swimlane horizon (UTC). Drain lookback is longer so weekend catch-up bars clip in.
SWIMLANE_PAST = timedelta(hours=24)
SWIMLANE_FUTURE = timedelta(hours=6)
DRAIN_LOOKBACK = timedelta(hours=48)

# Slot → job kinds created by enqueue_slot (for plan-vs-actual).
# Empty list → slot is inline / analytics; use freshness_dimension when set.
SLOT_EVIDENCE: dict[str, dict[str, Any]] = {
    "stock-eod": {"kinds": ["stock_daily"], "freshness": "stock_daily"},
    "eod-pipeline": {
        "kinds": ["option_snapshot", "option_open_interest"],
        "freshness": "option_snapshot",
    },
    "universe-daily": {"kinds": ["stock_daily_grouped"], "freshness": "stock_daily"},
    "corporate": {"kinds": ["splits", "dividends"], "freshness": None},
    "option-refresh": {"kinds": ["option_contract", "option_expiration"], "freshness": None},
    "option-bars": {"kinds": ["option_daily"], "freshness": None},
    "option-trades": {"kinds": ["option_trades"], "freshness": "option_trades"},
    "minute-bars": {"kinds": ["stock_minute", "option_minute"], "freshness": None},
    "calendar": {"kinds": ["calendar"], "freshness": "calendar"},
    "reference": {"kinds": ["ticker_sync"], "freshness": None},
    "fundamentals-rotate": {"kinds": ["financials"], "freshness": None},
    "related-rotate": {"kinds": ["ticker_related"], "freshness": None},
    "stock-snapshot": {"kinds": ["stock_snapshot"], "freshness": "stock_snapshot"},
    "stock-movers": {"kinds": ["stock_movers"], "freshness": None},
    "oi-gap-heal": {"kinds": [], "freshness": "option_open_interest", "inline": True},
    "max-pain": {
        "kinds": [],
        "freshness": None,
        "inline": True,
        "migrated": True,
    },
    "atm-iv-pcr": {
        "kinds": [],
        "freshness": None,
        "inline": True,
        "migrated": True,
    },
    "iv-percentile": {
        "kinds": [],
        "freshness": None,
        "inline": True,
        "migrated": True,
    },
    "readiness-refresh": {"kinds": [], "freshness": None, "inline": True},
    "trim": {"kinds": [], "freshness": None, "inline": True},
}

SLOT_NOTES: dict[str, str] = {
    "stock-eod": "Stock EOD bars",
    "eod-pipeline": "Option snapshot + OI",
    "universe-daily": "Grouped stock daily",
    "corporate": "Splits / dividends",
    "option-refresh": "Option contracts / expirations",
    "option-bars": "Option daily bars",
    "option-trades": "Option trades tape (REST)",
    "minute-bars": "Stock/option minutes",
    "calendar": "US trading calendar",
    "reference": "Ticker sync",
    "fundamentals-rotate": "Financials rotate",
    "related-rotate": "Related-companies rotate",
    "stock-snapshot": "Stock snapshots",
    "stock-movers": "Stock movers",
    "oi-gap-heal": "OI extract from snapshots (inline)",
    "max-pain": "moved to Research (bifrost_research.scheduler.volatility)",
    "atm-iv-pcr": "moved to Research (bifrost_research.scheduler.volatility)",
    "iv-percentile": "moved to Research (bifrost_research.scheduler.volatility)",
    "readiness-refresh": "Readiness rollup — RETIRED (inline no-op)",
    "trim": "Trim old jobs (inline)",
}

MIGRATED_SLOT_IDS = frozenset(
    sid for sid, ev in SLOT_EVIDENCE.items() if ev.get("migrated")
)


def _count_jobs_in_window(
    conn: Any,
    *,
    kinds: list[str],
    start: datetime,
    end: datetime,
) -> dict[str, int]:
    if not kinds:
        return {"created": 0, "done": 0, "failed": 0, "pending": 0, "running": 0}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, COUNT(*)::bigint AS n
            FROM ops_jobs.job_ingest
            WHERE kind = ANY(%s)
              AND created_at >= %s
              AND created_at < %s
            GROUP BY status
            """,
            (kinds, start, end),
        )
        rows = cur.fetchall() or []
    out = {"created": 0, "done": 0, "failed": 0, "pending": 0, "running": 0}
    for row in rows:
        if isinstance(row, Mapping):
            status = str(row.get("status") or "")
            n = int(row.get("n") or 0)
        else:
            status = str(row[0] or "")
            n = int(row[1] or 0)
        out["created"] += n
        if status in out:
            out[status] = n
        elif status == "success":
            out["done"] += n
    return out


def _freshness_map(conn: Any) -> dict[str, dict[str, Any]]:
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT dimension, last_run_at, rows_written, status
                FROM ops_jobs.ingest_freshness
                """
            )
            rows = cur.fetchall() or []
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return {}
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if isinstance(row, Mapping):
            dim = str(row.get("dimension") or "")
            last = row.get("last_run_at")
            status = str(row.get("status") or "")
            rows_w = row.get("rows_written")
        else:
            dim = str(row[0] or "")
            last = row[1]
            rows_w = row[2]
            status = str(row[3] or "")
        if not dim:
            continue
        out[dim] = {
            "last_run_at": iso_z(last) if isinstance(last, datetime) else None,
            "rows_written": int(rows_w or 0) if rows_w is not None else None,
            "status": status,
            "_last": last if isinstance(last, datetime) else None,
        }
    return out


def _queue_composition(conn: Any) -> dict[str, Any]:
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
        for kind, vals in sorted(by_kind.items(), key=lambda kv: -(kv[1]["pending"] + kv[1]["running"]))
    ]
    return {
        "pending": pending_total,
        "running": running_total,
        "active": pending_total + running_total,
        "kinds": kinds,
    }


def _throughput(conn: Any, now: datetime) -> dict[str, Any]:
    windows = {"5m": 5, "15m": 15, "60m": 60}
    done: dict[str, int] = {}
    failed: dict[str, int] = {}
    for label, minutes in windows.items():
        start = now - timedelta(minutes=minutes)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, COUNT(*)::bigint AS n
                FROM ops_jobs.job_ingest
                WHERE finished_at >= %s
                  AND status IN ('done', 'failed')
                GROUP BY status
                """,
                (start,),
            )
            rows = cur.fetchall() or []
        d = f = 0
        for row in rows:
            if isinstance(row, Mapping):
                status = str(row.get("status") or "")
                n = int(row.get("n") or 0)
            else:
                status = str(row[0] or "")
                n = int(row[1] or 0)
            if status == "done":
                d = n
            elif status == "failed":
                f = n
        done[label] = d
        failed[label] = f
    per_min_15 = round(done["15m"] / 15.0, 2) if done["15m"] else 0.0
    return {
        "done_last_5m": done["5m"],
        "done_last_15m": done["15m"],
        "done_last_60m": done["60m"],
        "failed_last_15m": failed["15m"],
        "jobs_per_min_15m": per_min_15,
    }


def _kind_activity(conn: Any, since: datetime) -> dict[str, dict[str, Any]]:
    """Per-kind first enqueue / last finish / still-active — for swimlane drain bars."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT kind,
                   MIN(created_at) AS first_created,
                   MAX(finished_at) AS last_finished,
                   COUNT(*) FILTER (
                       WHERE status IN ('pending', 'running')
                   )::bigint AS active
            FROM ops_jobs.job_ingest
            WHERE created_at >= %s
               OR status IN ('pending', 'running')
            GROUP BY kind
            """,
            (since,),
        )
        raw = cur.fetchall() or []
    out: dict[str, dict[str, Any]] = {}
    for row in raw:
        if isinstance(row, Mapping):
            kind = str(row.get("kind") or "")
            first = row.get("first_created")
            last = row.get("last_finished")
            active = int(row.get("active") or 0)
        else:
            kind = str(row[0] or "")
            first = row[1]
            last = row[2]
            active = int(row[3] or 0)
        if not kind:
            continue
        out[kind] = {
            "first_created": first if isinstance(first, datetime) else None,
            "last_finished": last if isinstance(last, datetime) else None,
            "active": active,
        }
    return out


def _slot_drain(
    kinds: list[str],
    activity: Mapping[str, dict[str, Any]],
) -> dict[str, Any] | None:
    if not kinds:
        return None
    firsts: list[datetime] = []
    lasts: list[datetime] = []
    active = 0
    for kind in kinds:
        row = activity.get(kind)
        if row is None:
            continue
        first = row.get("first_created")
        last = row.get("last_finished")
        if isinstance(first, datetime):
            firsts.append(first)
        if isinstance(last, datetime):
            lasts.append(last)
        active += int(row.get("active") or 0)
    if not firsts and active <= 0:
        return None
    started = min(firsts) if firsts else None
    ended = max(lasts) if lasts and active <= 0 else None
    return {
        "started_at": iso_z(started),
        "ended_at": iso_z(ended),
        "active": active > 0,
    }


def _oldest_pending_age_sec(conn: Any, now: datetime) -> float | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MIN(created_at)
            FROM ops_jobs.job_ingest
            WHERE status = 'pending'
            """
        )
        row = cur.fetchone()
    if row is None:
        return None
    ts = row[0] if not isinstance(row, Mapping) else next(iter(row.values()), None)
    if not isinstance(ts, datetime):
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return max(0.0, (now - ts).total_seconds())


def _slot_adherence(
    conn: Any,
    *,
    slot_id: str,
    cron: str,
    now: datetime,
    grace_minutes: int,
    freshness: Mapping[str, dict[str, Any]],
    activity: Mapping[str, dict[str, Any]] | None = None,
    horizon_start: datetime | None = None,
    horizon_end: datetime | None = None,
) -> dict[str, Any]:
    evidence = SLOT_EVIDENCE.get(slot_id, {"kinds": [], "freshness": None})
    kinds = list(evidence.get("kinds") or [])
    fresh_dim = evidence.get("freshness")
    inline = bool(evidence.get("inline"))
    migrated = bool(evidence.get("migrated"))
    fires_iso: list[str] = []
    if cron and horizon_start is not None and horizon_end is not None:
        try:
            fires_iso = [iso_z(t) for t in iter_cron_fires(cron, start=horizon_start, end=horizon_end)]
        except ValueError:
            fires_iso = []
    drain = _slot_drain(kinds, activity or {})
    if migrated:
        return {
            "slot": slot_id,
            "cron": cron or None,
            "note": SLOT_NOTES.get(slot_id, "moved to Research"),
            "ok": True,
            "adherence": "migrated",
            "detail": "moved to Research (bifrost_research.scheduler.volatility)",
            "last_fire": None,
            "next_fires": [],
            "inline": inline,
            "migrated": True,
            "evidence_kinds": kinds,
            "fires_in_window": [],
            "drain": None,
        }
    try:
        last = previous_fire(cron, before=now)
        nxt = next_fires(cron, after=now, count=3)
    except ValueError as exc:
        return {
            "slot": slot_id,
            "cron": cron,
            "note": SLOT_NOTES.get(slot_id, ""),
            "ok": False,
            "adherence": "unsupported_cron",
            "detail": str(exc),
            "next_fires": [],
            "last_fire": None,
            "fires_in_window": fires_iso,
            "drain": drain,
        }

    last_iso = iso_z(last)
    next_iso = [iso_z(t) for t in nxt]
    if last is None:
        return {
            "slot": slot_id,
            "cron": cron,
            "note": SLOT_NOTES.get(slot_id, ""),
            "ok": True,
            "adherence": "unknown",
            "detail": "no prior fire in lookback",
            "last_fire": None,
            "next_fires": next_iso,
            "inline": inline,
            "evidence_kinds": kinds,
            "fires_in_window": fires_iso,
            "drain": drain,
        }

    grace_end = last + timedelta(minutes=grace_minutes)
    window_end = min(now, grace_end + timedelta(hours=2))
    counts = _count_jobs_in_window(conn, kinds=kinds, start=last, end=window_end)

    fresh_hit = False
    fresh_last = None
    if fresh_dim and fresh_dim in freshness:
        fresh_last = freshness[fresh_dim].get("last_run_at")
        raw_last = freshness[fresh_dim].get("_last")
        if isinstance(raw_last, datetime):
            if raw_last.tzinfo is None:
                raw_last = raw_last.replace(tzinfo=timezone.utc)
            fresh_hit = raw_last >= last

    evidence_ok = (counts["created"] > 0) or fresh_hit
    if evidence_ok:
        adherence = "on_plan"
        detail = (
            f"jobs_created={counts['created']} "
            f"(done={counts['done']} failed={counts['failed']})"
        )
        if fresh_hit:
            detail += f"; freshness.{fresh_dim}={fresh_last}"
    elif now < grace_end:
        adherence = "due"
        detail = f"within grace ({grace_minutes}m) after last fire; waiting for evidence"
    else:
        adherence = "missed"
        detail = (
            f"no jobs/freshness evidence since last fire "
            f"(kinds={kinds or '—'}, freshness={fresh_dim or '—'})"
        )

    return {
        "slot": slot_id,
        "cron": cron,
        "note": SLOT_NOTES.get(slot_id, ""),
        "ok": adherence in ("on_plan", "due", "unknown"),
        "adherence": adherence,
        "detail": detail,
        "last_fire": last_iso,
        "next_fires": next_iso,
        "grace_ends_at": iso_z(grace_end),
        "inline": inline,
        "evidence_kinds": kinds,
        "jobs_in_window": counts,
        "freshness_dimension": fresh_dim,
        "freshness_last_run_at": fresh_last,
        "fires_in_window": fires_iso,
        "drain": drain,
    }


def build_queue_dashboard(
    conn: Any,
    *,
    now: datetime | None = None,
    grace_minutes: int = 45,
) -> dict[str, Any]:
    """Compose queue + schedule plan + adherence report."""
    now_utc = now or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)

    queue = _queue_composition(conn)
    throughput = _throughput(conn, now_utc)
    oldest = _oldest_pending_age_sec(conn, now_utc)
    freshness = _freshness_map(conn)
    horizon_start = now_utc - SWIMLANE_PAST
    horizon_end = now_utc + SWIMLANE_FUTURE
    activity = _kind_activity(conn, now_utc - DRAIN_LOOKBACK)

    eta_min: float | None = None
    if throughput["jobs_per_min_15m"] > 0 and queue["pending"] > 0:
        eta_min = round(queue["pending"] / throughput["jobs_per_min_15m"], 1)

    raw = load_schedule()
    sched = raw.get("scheduler") if isinstance(raw, dict) else {}
    slots_cfg = sched.get("slots") if isinstance(sched, dict) else {}
    if not isinstance(slots_cfg, dict):
        slots_cfg = {}

    plan: list[dict[str, Any]] = []
    active_slot_ids: set[str] = set()
    for slot_id, scfg in sorted(slots_cfg.items()):
        if not isinstance(scfg, dict):
            continue
        cron = str(scfg.get("cron") or "").strip()
        if not cron:
            continue
        active_slot_ids.add(str(slot_id))
        plan.append(
            _slot_adherence(
                conn,
                slot_id=str(slot_id),
                cron=cron,
                now=now_utc,
                grace_minutes=grace_minutes,
                freshness=freshness,
                activity=activity,
                horizon_start=horizon_start,
                horizon_end=horizon_end,
            )
        )

    # Surface migrated analytics slots even when removed from schedule.yaml.
    for slot_id in sorted(MIGRATED_SLOT_IDS):
        if slot_id in active_slot_ids:
            continue
        plan.append(
            _slot_adherence(
                conn,
                slot_id=slot_id,
                cron="",
                now=now_utc,
                grace_minutes=grace_minutes,
                freshness=freshness,
                activity=activity,
                horizon_start=horizon_start,
                horizon_end=horizon_end,
            )
        )

    on_plan = sum(1 for s in plan if s.get("adherence") == "on_plan")
    missed = sum(1 for s in plan if s.get("adherence") == "missed")
    due = sum(1 for s in plan if s.get("adherence") == "due")

    if queue["active"] > 0:
        queue_verdict = "draining"
    else:
        queue_verdict = "idle"
    if missed > 0:
        schedule_verdict = "missed"
    elif due > 0:
        schedule_verdict = "due"
    else:
        schedule_verdict = "on_plan"

    return {
        "ok": True,
        "generated_at": iso_z(now_utc),
        "model": {
            "ready_now": "pending jobs waiting for worker claim (SKIP LOCKED)",
            "running": "jobs claimed by workers",
            "scheduled_future_jobs": (
                "always 0 in job_ingest — CronJobs enqueue at fire time; "
                "future plan lives in schedule.yaml / K8s CronJobs"
            ),
        },
        "queue": {
            **queue,
            "ready_now": queue["pending"],
            "scheduled_future": 0,
            "oldest_pending_age_sec": oldest,
            "verdict": queue_verdict,
        },
        "throughput": {
            **throughput,
            "eta_minutes_at_current_rate": eta_min,
        },
        "schedule": {
            "verdict": schedule_verdict,
            "on_plan": on_plan,
            "due": due,
            "missed": missed,
            "grace_minutes": grace_minutes,
            "horizon": {
                "start": iso_z(horizon_start),
                "end": iso_z(horizon_end),
            },
            "slots": plan,
        },
    }
