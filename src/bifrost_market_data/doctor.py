"""Doctor — what the last session should have produced, what it did, and the
exact call that fills each gap.

The schedule tells you when things were *supposed* to run. This tells you
what is actually in the tables for the session, names what is missing, and
hands back a prescription the heal endpoint, the Console button, an agent's
MCP tool and the nightly self-heal all execute the same way. Read-only here;
``heal()`` is the only writer.

Every check is one bounded query against the session's rows; the report is
meant to answer in seconds, not to be a data-quality audit.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

from bifrost_market_data.ingest.index_options import storage_underlying
from bifrost_market_data.quality import fetch_completed_trading_days, filter_optionable_underlyings
from bifrost_market_data.scheduler.daily import (
    enqueue_slot,
    load_watchlist_symbols,
    union_iv_radar_benchmarks,
)
from bifrost_market_data.scheduler.enqueue import insert_jobs_bulk
from bifrost_market_data.subscription import SLOT_REQUIREMENTS
from bifrost_market_data.trading_calendar import chain_session, is_trading_day

logger = logging.getLogger(__name__)

_NY = ZoneInfo("America/New_York")

# The EOD chain fires at 22:00 UTC (18:00 EDT / 17:00 EST) and drains in
# minutes; by this wall-clock time in New York the session's rows must exist.
EOD_EXPECTED_BY_NY = time(19, 30)

# Whole-market floors: below these the pull did not happen, whatever the count.
# A normal session lands ~12.5k stock_daily and ~13k stock_snapshot rows.
STOCK_DAILY_MIN_ROWS = 12000
STOCK_SNAPSHOT_MIN_ROWS = 12000

# A session's chain must cover this share of the underlying's live contracts.
# Measured, not aspirational: the vendor's snapshot endpoint returns fewer
# contracts than the reference catalogue enumerates (SPY 11,966 of 12,576 on a
# live probe), so 95% of the catalogue is unreachable by construction. Healthy
# sessions measure 94-100%; the sessions the old model broke measured 33-72%.
SNAPSHOT_COVERAGE_MIN = 0.90

# The checks whose failure means the session's EOD data is not fit for dbt.
# Everything else (rotates, reference refreshes, maintenance) can lag a day
# without making the warehouse wrong, so it must not block the Research batch.
EOD_CRITICAL_CHECKS = (
    "option_snapshot",
    "option_open_interest",
    "stock_daily",
    "stock_daily_watchlist",
)
RATIOS_MIN_ROWS = 2000
SHORT_VOLUME_MIN_ROWS = 4000

# Slot → freshness dimension and the age (hours) after which it is stale.
STALENESS: dict[str, tuple[str, float]] = {
    "calendar": ("calendar", 48.0),
    "reference": ("ticker_sync", 48.0),
    "option-refresh": ("option_contract", 12.0),
    "corporate": ("dividends", 7 * 24.0),
    "fundamentals-rotate": ("financials", 48.0),
}

# Above this the worker loop is wedged behind synchronous batch writes.
WORKER_LOOP_LAG_WARN_SEC = 60.0

DEFAULT_WORKER_HEALTH_URLS = {
    "stocks": "http://market-data-health-stocks:8080/health",
    "options": "http://market-data-health-options:8080/health",
}


@dataclass
class Finding:
    id: str
    slot: str
    severity: str  # ok | warn | crit
    title: str
    expected: Any
    actual: Any
    detail: str
    session: str | None = None
    fix: dict[str, Any] | None = None
    auto_fixable: bool = False
    missing_sample: list[str] = field(default_factory=list)


def _row0(row: Any) -> Any:
    if row is None:
        return None
    if isinstance(row, Mapping):
        return next(iter(row.values()), None)
    return row[0] if row else None


def _col(rows: Any, key: str) -> list[str]:
    out: list[str] = []
    for row in rows or []:
        v = row.get(key) if isinstance(row, Mapping) else (row[0] if row else None)
        if v:
            out.append(str(v).strip().upper())
    return out


def _count(conn: Any, sql: str, params: tuple[Any, ...]) -> int:
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return int(_row0(cur.fetchone()) or 0)
    except Exception as exc:  # noqa: BLE001 — one failed check must not sink the report
        logger.warning("doctor count failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return -1


def _counts(conn: Any, sql: str, params: tuple[Any, ...]) -> dict[str, int] | None:
    """``SELECT key, count`` → mapping, or None when the query failed."""
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() if hasattr(cur, "fetchall") else []
    except Exception as exc:  # noqa: BLE001
        logger.warning("doctor count query failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return None
    out: dict[str, int] = {}
    for row in rows or []:
        if isinstance(row, Mapping):
            values = list(row.values())
            key, value = values[0], values[1]
        else:
            key, value = row[0], row[1]
        if key:
            out[str(key).strip().upper()] = int(value or 0)
    return out


def _coverage_finding(
    check_id: str,
    title: str,
    live: Mapping[str, int],
    got: Mapping[str, int],
    *,
    session: date,
    fixable: bool,
) -> Finding:
    """One finding for how much of each underlying's live chain the session holds."""
    session_s = session.isoformat()
    short: list[tuple[str, int, int, float]] = []
    for und, want in sorted(live.items()):
        have = int(got.get(und, 0))
        pct = have / want if want else 1.0
        if pct < SNAPSHOT_COVERAGE_MIN:
            short.append((und, have, want, pct))
    total_want = sum(live.values())
    total_have = sum(int(got.get(u, 0)) for u in live)
    overall = total_have / total_want if total_want else 1.0
    worst = sorted(short, key=lambda t: t[3])[:8]
    if not short:
        severity = "ok"
    elif overall < 0.5 or len(short) > max(1, len(live) // 2):
        severity = "crit"
    else:
        severity = "warn"
    detail = (
        f"{total_have}/{total_want} live contracts covered for {session_s} "
        f"({overall:.0%}); {len(short)} of {len(live)} underlyings below "
        f"{SNAPSHOT_COVERAGE_MIN:.0%}."
    )
    if worst:
        detail += " Worst: " + ", ".join(f"{u} {p:.0%}" for u, _h, _w, p in worst) + "."
    if short and not fixable:
        detail += (
            " The chain now reflects a later session, so this one can no longer"
            " be observed — it is lost, not pending."
        )
    return Finding(
        f"{check_id}:{session_s}",
        "eod-pipeline",
        severity,
        title,
        f">= {SNAPSHOT_COVERAGE_MIN:.0%} of {total_want}",
        f"{overall:.0%} ({total_have})",
        detail,
        session=session_s,
        fix=_slot_fix("eod-pipeline", session) if (short and fixable) else None,
        auto_fixable=bool(short) and fixable,
        missing_sample=[u for u, _h, _w, _p in worst],
    )


def _distinct(conn: Any, sql: str, params: tuple[Any, ...], key: str) -> list[str] | None:
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return _col(cur.fetchall() if hasattr(cur, "fetchall") else [], key)
    except Exception as exc:  # noqa: BLE001
        logger.warning("doctor query failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return None


def _freshness(conn: Any) -> dict[str, datetime]:
    out: dict[str, datetime] = {}
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT dimension, last_run_at FROM ops_jobs.ingest_freshness")
            for row in cur.fetchall() or []:
                dim = row.get("dimension") if isinstance(row, Mapping) else row[0]
                last = row.get("last_run_at") if isinstance(row, Mapping) else row[1]
                if dim and isinstance(last, datetime):
                    out[str(dim)] = last if last.tzinfo else last.replace(tzinfo=timezone.utc)
    except Exception as exc:  # noqa: BLE001
        logger.warning("doctor freshness read failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
    return out


def resolve_session(conn: Any, now: datetime) -> tuple[date, bool]:
    """The session the tables should hold by now, and whether that is today.

    Today counts once its EOD batch should have drained (19:30 New York on a
    trading day); before that, or on a weekend, the last completed session.
    """
    now_ny = now.astimezone(_NY)
    today_ny = now_ny.date()
    try:
        trading_today = is_trading_day(conn, today_ny)
    except Exception:
        trading_today = today_ny.weekday() < 5
    if trading_today and now_ny.time() >= EOD_EXPECTED_BY_NY:
        return today_ny, True
    completed = fetch_completed_trading_days(conn, 1, as_of=today_ny)
    if completed:
        return completed[-1], False
    return today_ny, trading_today


def _slot_fix(slot: str, session: date | None, *, force: bool = True) -> dict[str, Any]:
    fix: dict[str, Any] = {"action": "enqueue-slot", "slot": slot, "force": force}
    if session is not None:
        fix["date"] = session.isoformat()
    return fix


def run_doctor(
    conn: Any,
    *,
    scheduler_cfg: Mapping[str, Any] | None = None,
    now: datetime | None = None,
    watchlist: Sequence[str] | None = None,
    worker_health: Mapping[str, Mapping[str, Any] | None] | None = None,
    vendor: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Findings for the session the tables should hold by now, with prescriptions."""
    cfg = dict(scheduler_cfg or {})
    now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    session, session_is_today = resolve_session(conn, now_utc)
    session_s = session.isoformat()
    findings: list[Finding] = []

    symbols = list(watchlist) if watchlist is not None else load_watchlist_symbols(conn, cfg)
    universe = sorted({storage_underlying(s) for s in union_iv_radar_benchmarks(symbols, cfg)})
    optionable = filter_optionable_underlyings(conn, universe)

    # ── EOD option chain: how much of each live chain the session actually holds ──
    if optionable:
        # Half-open UTC range for the NY session so (underlying, snapshot_ts) is used.
        day_start = datetime.combine(session, time(0), tzinfo=_NY).astimezone(timezone.utc)
        day_end = day_start + timedelta(days=1)
        # Only a session the vendor chain still reflects can be re-observed.
        try:
            fixable = chain_session(conn) == session
        except Exception:  # noqa: BLE001 — calendar probe must not sink the report
            fixable = session_is_today
        live = _counts(
            conn,
            """
            SELECT underlying, count(*)::bigint FROM raw_market.option_contract
            WHERE underlying = ANY(%s) AND expiry >= %s AND first_seen_at < %s
            GROUP BY 1
            """,
            (optionable, session, day_end),
        )
        snap = _counts(
            conn,
            """
            SELECT underlying, count(DISTINCT option_ticker)::bigint
            FROM raw_market.option_snapshot
            WHERE underlying = ANY(%s) AND snapshot_ts >= %s AND snapshot_ts < %s
            GROUP BY 1
            """,
            (optionable, day_start, day_end),
        )
        oi = _counts(
            conn,
            """
            SELECT underlying, count(*)::bigint FROM raw_market.option_open_interest
            WHERE underlying = ANY(%s) AND trade_date = %s GROUP BY 1
            """,
            (optionable, session),
        )
        if live is None or snap is None or oi is None:
            findings.append(
                Finding(
                    f"option_chain:{session_s}", "eod-pipeline", "warn", "Option chain coverage",
                    len(optionable), None, "coverage query failed — see API log", session=session_s
                )
            )
        else:
            findings.append(
                _coverage_finding(
                    "option_snapshot", "Option chain snapshot", live, snap,
                    session=session, fixable=fixable,
                )
            )
            findings.append(
                _coverage_finding(
                    "option_open_interest", "Open interest", live, oi,
                    session=session, fixable=fixable,
                )
            )

    # ── Stock EOD: whole market + watchlist for the session ──
    n_daily = _count(conn, "SELECT count(*) FROM raw_market.stock_daily WHERE bar_date = %s", (session,))
    findings.append(
        Finding(
            f"stock_daily:{session_s}", "universe-daily",
            "ok" if n_daily >= STOCK_DAILY_MIN_ROWS else "crit",
            "Stock daily bars (whole market)", f">= {STOCK_DAILY_MIN_ROWS}", n_daily,
            f"{n_daily} stock_daily rows for {session_s}.",
            session=session_s,
            fix=None if n_daily >= STOCK_DAILY_MIN_ROWS else _slot_fix("universe-daily", session),
            auto_fixable=n_daily < STOCK_DAILY_MIN_ROWS,
        )
    )
    if symbols:
        have = _distinct(
            conn,
            "SELECT DISTINCT symbol FROM raw_market.stock_daily WHERE bar_date = %s AND symbol = ANY(%s)",
            (session, list(symbols)),
            "symbol",
        )
        if have is not None:
            missing = [s for s in symbols if s not in set(have)]
            findings.append(
                Finding(
                    f"stock_daily_watchlist:{session_s}", "stock-eod",
                    "ok" if not missing else "warn",
                    "Stock daily bars (watchlist)", len(symbols), len(have),
                    f"{len(have)}/{len(symbols)} watchlist symbols have a {session_s} bar.",
                    session=session_s,
                    fix=_slot_fix("stock-eod", session) if missing else None,
                    auto_fixable=bool(missing),
                    missing_sample=missing[:20],
                )
            )

    n_snap = _count(conn, "SELECT count(*) FROM raw_market.stock_snapshot WHERE session_date = %s", (session,))
    findings.append(
        Finding(
            f"stock_snapshot:{session_s}", "stock-snapshot",
            "ok" if n_snap >= STOCK_SNAPSHOT_MIN_ROWS else "warn",
            "Stock snapshot (whole market)", f">= {STOCK_SNAPSHOT_MIN_ROWS}", n_snap,
            f"{n_snap} stock_snapshot rows for {session_s}."
            + ("" if n_snap >= STOCK_SNAPSHOT_MIN_ROWS or session_is_today else
               " The vendor snapshot is point-in-time; a catch-up lands under today's date."),
            session=session_s,
            fix=None if n_snap >= STOCK_SNAPSHOT_MIN_ROWS else _slot_fix("stock-snapshot", session),
            auto_fixable=n_snap < STOCK_SNAPSHOT_MIN_ROWS and session_is_today,
        )
    )

    # ── Financials & Ratios by date (published the morning after) ──
    n_ratios = _count(conn, "SELECT count(*) FROM raw_market.ratios WHERE period_date = %s", (session,))
    n_sv = _count(conn, "SELECT count(*) FROM raw_market.short_volume WHERE period_date = %s", (session,))
    fund_ok = n_ratios >= RATIOS_MIN_ROWS and n_sv >= SHORT_VOLUME_MIN_ROWS
    findings.append(
        Finding(
            f"fundamentals_market:{session_s}", "fundamentals-market",
            "ok" if fund_ok else ("warn" if session_is_today else "crit"),
            "Ratios + short volume (whole market)",
            f"ratios >= {RATIOS_MIN_ROWS}, short_volume >= {SHORT_VOLUME_MIN_ROWS}",
            {"ratios": n_ratios, "short_volume": n_sv},
            f"ratios={n_ratios}, short_volume={n_sv} rows for {session_s}."
            + (" Published the morning after the session (04:30 UTC slot)." if session_is_today and not fund_ok else ""),
            session=session_s,
            fix=None if fund_ok else _slot_fix("fundamentals-market", session, force=False),
            auto_fixable=not fund_ok and not session_is_today,
        )
    )

    # ── Staleness of the rotate / reference slots ──
    fresh = _freshness(conn)
    for slot, (dim, max_age_h) in STALENESS.items():
        last = fresh.get(dim)
        age_h = (now_utc - last).total_seconds() / 3600.0 if last else None
        stale = age_h is None or age_h > max_age_h
        findings.append(
            Finding(
                f"stale:{slot}", slot, "warn" if stale else "ok",
                f"{slot} freshness", f"< {max_age_h:g}h", None if age_h is None else round(age_h, 1),
                (f"freshness.{dim} is {age_h:.1f}h old (limit {max_age_h:g}h)." if age_h is not None
                 else f"freshness.{dim} has never been written."),
                fix=_slot_fix(slot, None) if stale else None,
                auto_fixable=stale,
            )
        )

    # ── Queue: failed jobs in the last day, stuck running rows ──
    failed_rows: list[Any] = []
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT kind, count(*)::bigint AS n,
                       min(result->>'error') AS sample_error,
                       array_agg(id ORDER BY id DESC) AS ids
                FROM ops_jobs.job_ingest
                WHERE status = 'failed' AND finished_at >= %s
                GROUP BY kind ORDER BY n DESC
                """,
                (now_utc - timedelta(hours=24),),
            )
            failed_rows = list(cur.fetchall() or [])
    except Exception as exc:  # noqa: BLE001
        logger.warning("doctor failed-jobs read failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
    for row in failed_rows:
        kind = str(row.get("kind") if isinstance(row, Mapping) else row[0])
        n = int(row.get("n") if isinstance(row, Mapping) else row[1])
        sample = str((row.get("sample_error") if isinstance(row, Mapping) else row[2]) or "")
        ids = list((row.get("ids") if isinstance(row, Mapping) else row[3]) or [])[:50]
        unentitled = "not entitled" in sample.lower() or "upgrade your plan" in sample.lower()
        findings.append(
            Finding(
                f"failed:{kind}", "queue", "warn", f"Failed jobs: {kind}", 0, n,
                (f"{n} {kind} job(s) failed in 24h — {sample[:160]}"
                 + (" — the plan does not cover this data; not retried." if unentitled else "")),
                fix=None if unentitled else {"action": "retry-jobs", "kind": kind, "job_ids": [int(i) for i in ids]},
                auto_fixable=not unentitled,
            )
        )
    stuck = _count(
        conn,
        """
        SELECT count(*) FROM ops_jobs.job_ingest
        WHERE status = 'running' AND started_at < now() - make_interval(secs => %s)
        """,
        (int(dict(cfg.get("worker") or {}).get("stale_running_sec") or 1800),),
    )
    if stuck > 0:
        findings.append(
            Finding("stuck_running", "queue", "warn", "Stuck running jobs", 0, stuck,
                    f"{stuck} job(s) have been running past the stale limit; the workers reclaim them on their next tick.")
        )

    # ── Workers and vendor (informational: fixes live outside the plugin) ──
    for pool, health in (worker_health or {}).items():
        if health is None:
            findings.append(
                Finding(f"worker:{pool}", "workers", "crit", f"{pool} workers", "reachable", "unreachable",
                        f"/health for the {pool} pool did not answer.",
                        fix={"action": "rollout-restart", "deployment": f"polygon-worker-{pool}"}, auto_fixable=False)
            )
            continue
        last_claim = health.get("last_claim_at")
        lag = health.get("loop_lag_sec")
        # The handlers write synchronously, so a heavy batch holds the loop.
        # Say so rather than calling a busy pool healthy or dead.
        busy = isinstance(lag, (int, float)) and lag > WORKER_LOOP_LAG_WARN_SEC
        findings.append(
            Finding(
                f"worker:{pool}", "workers", "warn" if busy else "ok", f"{pool} workers",
                f"loop lag < {WORKER_LOOP_LAG_WARN_SEC:g}s", "reachable" if not busy else f"lag {lag}s",
                f"done={health.get('jobs_done')} failed={health.get('jobs_failed')} "
                f"last_claim={last_claim or '—'} uptime={health.get('uptime_sec')}s lag={lag}s"
                + (" — the pool is saturated, not down." if busy else ""),
            )
        )
    if vendor is not None:
        reach = vendor.get("reachable")
        code = vendor.get("status_code")
        sev = "ok" if reach and code == 200 else ("crit" if not reach else "warn")
        findings.append(
            Finding("vendor", "vendor", sev, "Vendor API", "HTTP 200", code if reach else "unreachable",
                    vendor.get("detail") or ("marketstatus/now answered" if sev == "ok" else "vendor probe failed"),
                    fix=None if sev == "ok" else {"action": "check-vendor-key"}, auto_fixable=False)
        )

    # ── Prescriptions: one per distinct fix ──
    seen: set[str] = set()
    prescriptions: list[dict[str, Any]] = []
    for f in findings:
        if not f.fix or not f.auto_fixable:
            continue
        key = json.dumps(f.fix, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        prescriptions.append({"finding_ids": [g.id for g in findings if g.fix == f.fix and g.auto_fixable], **f.fix})

    crit = [f for f in findings if f.severity == "crit"]
    warn = [f for f in findings if f.severity == "warn"]
    verdict = "critical" if crit else ("degraded" if warn else "healthy")

    eod = [f for f in findings if f.id.split(":", 1)[0] in EOD_CRITICAL_CHECKS]
    eod_crit = [f for f in eod if f.severity == "crit"]
    eod_warn = [f for f in eod if f.severity == "warn"]
    eod_verdict = "critical" if eod_crit else ("degraded" if eod_warn else "healthy")
    return {
        "ok": True,
        "generated_at": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "session": session_s,
        "session_is_today": session_is_today,
        "universe": {"watchlist": len(symbols), "underlyings": len(universe), "optionable": len(optionable)},
        "verdict": verdict,
        "summary": f"{len(crit)} critical · {len(warn)} warning · {sum(1 for f in findings if f.severity == 'ok')} ok",
        "eod_critical": {
            "verdict": eod_verdict,
            "checks": list(EOD_CRITICAL_CHECKS),
            "findings": [f.id for f in eod_crit + eod_warn],
            "detail": (
                "; ".join(f"{f.title}: {f.actual}" for f in eod_crit + eod_warn)
                or f"{len(eod)} EOD checks complete for {session_s}"
            ),
        },
        "findings": [asdict(f) for f in findings],
        "prescriptions": prescriptions,
        "retired_slots": sorted(SLOT_REQUIREMENTS),
    }


def probe_worker_health(
    urls: Mapping[str, str] | None = None, *, timeout: float = 5.0
) -> dict[str, Mapping[str, Any] | None]:
    out: dict[str, Mapping[str, Any] | None] = {}
    for pool, url in (urls or DEFAULT_WORKER_HEALTH_URLS).items():
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                out[pool] = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, OSError, ValueError):
            out[pool] = None
    return out


def probe_vendor(api_key: str, *, rest_base: str = "https://api.polygon.io", timeout: float = 5.0) -> dict[str, Any]:
    """One cheap authenticated GET: reachable? key accepted?"""
    if not api_key:
        return {"reachable": False, "status_code": None, "detail": "no API key configured"}
    req = urllib.request.Request(
        f"{rest_base.rstrip('/')}/v1/marketstatus/now",
        headers={"Authorization": f"Bearer {api_key}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return {"reachable": True, "status_code": resp.status, "detail": "marketstatus/now answered"}
    except urllib.error.HTTPError as exc:
        return {"reachable": True, "status_code": exc.code, "detail": f"vendor answered HTTP {exc.code}"}
    except (urllib.error.URLError, OSError) as exc:
        return {"reachable": False, "status_code": None, "detail": f"vendor unreachable: {exc}"}


def heal(
    conn: Any,
    *,
    scheduler_cfg: Mapping[str, Any] | None = None,
    report: Mapping[str, Any] | None = None,
    finding_ids: Sequence[str] | None = None,
    dry_run: bool = False,
    doctor: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Execute the doctor's prescriptions (all auto-fixable ones, or the chosen findings)."""
    cfg = dict(scheduler_cfg or {})
    rep = dict(report) if report is not None else (doctor or run_doctor)(conn, scheduler_cfg=cfg)
    wanted = set(finding_ids or [])
    actions: list[dict[str, Any]] = []
    for pres in rep.get("prescriptions", []):
        if wanted and not (wanted & set(pres.get("finding_ids", []))):
            continue
        entry: dict[str, Any] = {k: v for k, v in pres.items() if k != "finding_ids"}
        entry["finding_ids"] = list(pres.get("finding_ids", []))
        if dry_run:
            entry["result"] = "dry_run"
            actions.append(entry)
            continue
        try:
            if pres["action"] == "enqueue-slot":
                target = date.fromisoformat(pres["date"]) if pres.get("date") else None
                res = enqueue_slot(
                    conn,
                    pres["slot"],
                    target_date=target,
                    scheduler_cfg=cfg,
                    force=bool(pres.get("force")),
                )
                entry["result"] = {k: res.get(k) for k in ("enqueued", "deduped", "skipped", "reason", "target_date")}
            elif pres["action"] == "retry-jobs":
                entry["result"] = _retry_jobs(conn, [int(i) for i in pres.get("job_ids", [])])
            else:
                entry["result"] = "not executable by the plugin"
        except Exception as exc:  # noqa: BLE001 — report per action, keep going
            logger.exception("heal action failed: %s", pres)
            entry["result"] = f"error: {exc}"
        actions.append(entry)
    return {
        "ok": True,
        "dry_run": dry_run,
        "session": rep.get("session"),
        "verdict_before": rep.get("verdict"),
        "actions": actions,
        "enqueued": sum(
            int(a["result"].get("enqueued") or 0) for a in actions if isinstance(a.get("result"), dict)
        ),
    }


def _retry_jobs(conn: Any, job_ids: Sequence[int]) -> dict[str, Any]:
    """Re-enqueue failed jobs with their original kind and payload (dedup applies)."""
    if not job_ids:
        return {"enqueued": 0, "deduped": 0}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT kind, payload, priority FROM ops_jobs.job_ingest WHERE id = ANY(%s) AND status = 'failed'",
            (list(job_ids),),
        )
        rows = cur.fetchall() or []
    specs: list[tuple[str, Mapping[str, Any] | None, int, int]] = []
    for row in rows:
        kind = row.get("kind") if isinstance(row, Mapping) else row[0]
        payload = row.get("payload") if isinstance(row, Mapping) else row[1]
        priority = row.get("priority") if isinstance(row, Mapping) else row[2]
        if isinstance(payload, (str, bytes)):
            payload = json.loads(payload)
        specs.append((str(kind), payload or {}, int(priority or 0), 3))
    ids = insert_jobs_bulk(conn, specs)
    return {"enqueued": sum(1 for i in ids if i is not None), "deduped": sum(1 for i in ids if i is None)}
