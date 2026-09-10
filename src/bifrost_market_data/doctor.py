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
import re
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
    load_research_universe,
    load_watchlist_symbols,
    union_iv_radar_benchmarks,
)
from bifrost_market_data.scheduler.enqueue import insert_jobs_bulk
from bifrost_market_data.subscription import SLOT_REQUIREMENTS
from bifrost_market_data.trading_calendar import chain_session, is_trading_day

from bifrost_market_data.contracts import (
    CONTRACTS,
    STOCK_DAILY_MIN_SESSION_SYMBOLS,
    staleness_by_slot,
)
from bifrost_market_data.session import EOD_EXPECTED_BY_NY as _EOD_BY_NY
from bifrost_market_data.session import deadline
from bifrost_market_data.session import resolve_session as _resolve_session

logger = logging.getLogger(__name__)

_NY = ZoneInfo("America/New_York")

#: Re-exported from ``session`` for the callers that read it here.
EOD_EXPECTED_BY_NY = _EOD_BY_NY

# Whole-market floors: below these the pull did not happen, whatever the count.
# A normal session lands ~12.5k stock_daily and ~13k stock_snapshot rows. The
# stock_daily floor is the contract table's, shared with the quality gate.
STOCK_DAILY_MIN_ROWS = STOCK_DAILY_MIN_SESSION_SYMBOLS
STOCK_SNAPSHOT_MIN_ROWS = 12000

# A session's chain must cover this share of the underlying's live contracts.
# Measured, not aspirational: the vendor's snapshot endpoint returns fewer
# contracts than the reference catalogue enumerates (SPY 11,966 of 12,576 on a
# live probe), so 95% of the catalogue is unreachable by construction. Healthy
# sessions measure 94-100%; the sessions the old model broke measured 33-72%.
SNAPSHOT_COVERAGE_MIN = 0.90

# How far ahead a partitioned table must already have partitions. Inserts fail
# outright when a row has no partition to land in, so this has to warn early
# enough to run an elevated script: `ensure_month_partitions` builds three months
# ahead, and only the owner of the parent may create one.
PARTITION_RUNWAY_MIN_DAYS = 45

# Partitioned tables nothing writes to any more. Their runway ran out and it does
# not matter; a critical finding for a retired table is the false alarm this
# check exists to avoid making. Explicit, because retiring one is a decision.
RETIRED_PARTITIONED_TABLES: tuple[str, ...] = ("option_trades",)

# The tiers the EOD slot collects with the near-the-money window. Their rows are
# a deliberate slice of the chain, so they are checked for presence, not share.
WINDOWED_TIERS = ("core", "edge")
# Above this share of windowed names unreached, the slot did not run; below it,
# a handful of names the vendor answered nothing for on the day.
CHAIN_PRESENCE_MISSING_CRIT = 0.10

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
#: Hours after the close by which the whole-market ratio and short-volume pull
#: is due — the tightest of the contracts the `fundamentals-market` slot owns.
FUNDAMENTALS_MARKET_DEADLINE_H = staleness_by_slot()["fundamentals-market"][1]

# The slots whose staleness the doctor polices. The list is deliberate — widening
# it is a decision about what raises a warning, not a consequence of declaring a
# deadline — but the numbers are no longer kept here: a deadline written in two
# places is a dataset that reads healthy on one panel and stale on the next.
POLICED_SLOTS: tuple[str, ...] = (
    "calendar",
    "reference",
    "option-refresh",
    "corporate",
    "fundamentals-rotate",
)
STALENESS: dict[str, tuple[str, float]] = {
    slot: entry for slot, entry in staleness_by_slot().items() if slot in POLICED_SLOTS
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


def _rollback(conn: Any) -> None:
    """A failed read leaves the transaction aborted; the next check needs it clean."""
    try:
        conn.rollback()
    except Exception:  # noqa: BLE001 — nothing useful to do if even this fails
        pass


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


def _presence_finding(
    check_id: str,
    title: str,
    expected: Sequence[str],
    got: Mapping[str, int],
    *,
    session: date,
    fixable: bool,
) -> Finding:
    """One finding for whether the session reached each windowed underlying.

    Core and edge chains are collected near the money by design, so a share of
    the full contract catalogue would grade the design instead of the run. What
    is checkable is presence: the slot either wrote rows for the name this
    session or it did not. ``expected`` is already narrowed to names the vendor
    lists a live chain for.
    """
    session_s = session.isoformat()
    total = len(expected)
    missing = sorted(s for s in expected if int(got.get(s, 0)) <= 0)
    have = total - len(missing)
    if not missing:
        severity = "ok"
    elif total and len(missing) / total > CHAIN_PRESENCE_MISSING_CRIT:
        severity = "crit"
    else:
        severity = "warn"
    detail = (
        f"{have}/{total} windowed chains written for {session_s} "
        f"({(have / total) if total else 1.0:.0%})."
    )
    if missing:
        detail += (
            f" {len(missing)} underlyings with a live chain were not reached: "
            + ", ".join(missing[:8])
            + "."
        )
    if missing and not fixable:
        detail += (
            " The chain now reflects a later session, so this one can no longer"
            " be observed — it is lost, not pending."
        )
    return Finding(
        f"{check_id}:{session_s}",
        "eod-pipeline",
        severity,
        title,
        f"{total} with a live chain",
        have,
        detail,
        session=session_s,
        fix=_slot_fix("eod-pipeline", session) if (missing and fixable) else None,
        auto_fixable=bool(missing) and fixable,
        missing_sample=missing[:8],
    )


#: How far back the doctor looks for a session that never landed. The fourth
#: axis reads 120 days because it is answering "is the middle solid"; the doctor
#: is answering "what can I still fix tonight", and a shorter window keeps the
#: per-day scan off the doctor's latency budget.
CONTINUITY_WINDOW_DAYS = 60

#: Missing sessions prescribed per dataset per run. One option-bars day is
#: ~70,000 jobs, so an unbounded prescription for a dataset that has been broken
#: for a month would enqueue millions in a single nightly heal. Three nights
#: clear nine days, and the finding says plainly when the cap is biting.
CONTINUITY_MAX_PRESCRIBED = 3


def _continuity_findings(
    conn: Any,
    *,
    today: date,
    window_days: int = CONTINUITY_WINDOW_DAYS,
    statement_timeout: str = "30s",
) -> list[Finding]:
    """Sessions that never landed and a slot can still refill.

    The doctor has always been a per-session check with no memory: it reported
    2026-08-11 as critical on 2026-08-11 and forgot by the next morning, so the
    hole sat there for a month. The fourth axis has the memory but is a
    read-only measure — what finds a hole could not fix it, and what fixes could
    not find it. This is the join.

    Only datasets whose contract names a ``backfill_slot`` are considered. A
    missed EOD option chain is gone for good and a prescription for it would be
    a lie, not a repair.
    """
    from bifrost_market_data.continuity import has_continuity, per_day_counts
    from bifrost_market_data.trading_calendar import expected_trading_days

    start = today - timedelta(days=int(window_days))
    try:
        sessions = set(expected_trading_days(conn, start=start, end=today))
    except Exception as exc:  # noqa: BLE001 — without the calendar there is no question to ask
        logger.warning("continuity findings: calendar unavailable: %s", exc)
        _rollback(conn)
        return []
    if not sessions:
        return []

    out: list[Finding] = []
    for c in CONTRACTS:
        if not c.backfill_slot or c.cadence != "session" or not has_continuity(c):
            continue
        counts = per_day_counts(
            conn,
            c.dataset,
            str(c.date_column),
            window_days=window_days,
            statement_timeout=statement_timeout,
        )
        if counts is None:
            continue
        present = {d for d, _ in counts if d in sessions}
        if not present:
            continue
        # Only sessions inside the observed span: a dataset that starts midway
        # through the window has not lost the days before it existed, and the
        # newest session may simply not be due yet.
        first, last = min(present), max(present)
        absent = sorted(d for d in sessions if first <= d <= last and d not in present)
        name = c.dataset.replace("raw_market.", "")
        if not absent:
            out.append(
                Finding(
                    f"continuity:{name}",
                    c.backfill_slot,
                    "ok",
                    f"Continuity: {name}",
                    f"every session in {window_days}d",
                    f"{len(present)} sessions, none missing",
                    f"No session missing from {name} between {first} and {last}.",
                )
            )
            continue
        prescribed = absent[:CONTINUITY_MAX_PRESCRIBED]
        for day in prescribed:
            detail = (
                f"{name} has no rows for {day}, a trading day between {first} and {last}. "
                f"{len(absent)} such session(s) in the last {window_days} days"
            )
            if len(absent) > len(prescribed):
                detail += (
                    f"; prescribing the {len(prescribed)} oldest this run, "
                    f"{len(absent) - len(prescribed)} left for the next"
                )
            out.append(
                Finding(
                    f"continuity:{name}:{day.isoformat()}",
                    c.backfill_slot,
                    "warn",
                    f"Missing session: {name}",
                    "rows for every trading day",
                    "no rows",
                    detail + ".",
                    session=day.isoformat(),
                    fix=_slot_fix(c.backfill_slot, day),
                    auto_fixable=True,
                    missing_sample=[d.isoformat() for d in absent[:10]],
                )
            )
    return out


def _presence_findings(
    conn: Any,
    expected: Sequence[str],
    *,
    session: date,
    session_s: str,
    day_start: datetime,
    day_end: datetime,
    fixable: bool,
) -> list[Finding]:
    """Snapshot and open-interest presence for the windowed part of the universe.

    Deliberately outside ``EOD_CRITICAL_CHECKS``: widening what blocks the
    Research batch is a decision about the gate, not a consequence of adding a
    check. These report; they do not gate.
    """
    syms = list(expected)
    snap = _counts(
        conn,
        """
        SELECT underlying, count(*)::bigint FROM raw_market.option_snapshot
        WHERE underlying = ANY(%s) AND snapshot_ts >= %s AND snapshot_ts < %s
        GROUP BY 1
        """,
        (syms, day_start, day_end),
    )
    oi = _counts(
        conn,
        """
        SELECT underlying, count(*)::bigint FROM raw_market.option_open_interest
        WHERE underlying = ANY(%s) AND trade_date = %s GROUP BY 1
        """,
        (syms, session),
    )
    if snap is None or oi is None:
        return [
            Finding(
                f"option_chain_windowed:{session_s}",
                "eod-pipeline",
                "warn",
                "Windowed chain presence",
                len(syms),
                None,
                "presence query failed — see API log",
                session=session_s,
            )
        ]
    return [
        _presence_finding(
            "option_chain_windowed",
            "Windowed chain snapshot",
            syms,
            snap,
            session=session,
            fixable=fixable,
        ),
        _presence_finding(
            "option_oi_windowed",
            "Windowed chain open interest",
            syms,
            oi,
            session=session,
            fixable=fixable,
        ),
    ]


_PARTITION_BOUND_TO = re.compile(r"TO \('([^']+)'\)")


def _partition_runway(conn: Any) -> list[tuple[str, date | None, bool]] | None:
    """Per partitioned table: how far its partitions reach, and whether we own it.

    A row with no partition to land in is rejected, so running out is an outage
    rather than a degradation — and the plugin cannot extend a table it does not
    own. On 2026-09-09 every partition in raw_market was owned by ``postgres``
    while the plugin runs as ``bifrost``, so ``ensure_month_partitions`` would
    have started failing three months before the first insert had nowhere to go.
    ``make ownership-sql`` emits the fix for a privileged session.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.relname,
                       pg_get_userbyid(p.relowner) = current_user AS owned,
                       pg_get_expr(c.relpartbound, c.oid)
                FROM pg_class p
                JOIN pg_namespace n ON n.oid = p.relnamespace
                JOIN pg_inherits i ON i.inhparent = p.oid
                JOIN pg_class c ON c.oid = i.inhrelid
                WHERE n.nspname = 'raw_market' AND p.relkind = 'p'
                """
            )
            rows = cur.fetchall() if hasattr(cur, "fetchall") else []
    except Exception as exc:  # noqa: BLE001 — a catalogue probe must not sink the report
        logger.warning("doctor partition runway read failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return None

    reach: dict[str, tuple[date | None, bool]] = {}
    for row in rows or []:
        if isinstance(row, Mapping):
            values = list(row.values())
            parent, owned, bound = values[0], values[1], values[2]
        else:
            parent, owned, bound = row[0], row[1], row[2]
        parent = str(parent)
        match = _PARTITION_BOUND_TO.search(str(bound or ""))
        upper: date | None = None
        if match:
            try:
                upper = datetime.fromisoformat(match.group(1)).date()
            except ValueError:
                upper = None
        current, current_owned = reach.get(parent, (None, bool(owned)))
        if upper is not None and (current is None or upper > current):
            current = upper
        reach[parent] = (current, bool(owned))
    return [(k, v[0], v[1]) for k, v in sorted(reach.items())]


def _failed_since(conn: Any, since: datetime) -> dict[str, int] | None:
    """Failures per kind from ``ops_jobs.queue_sample``, or None when unreadable.

    The job rows do not survive their own retention: the finished-row cap used
    to be a count, and once throughput reached 2,700 a minute that count held
    about fifteen minutes, so a check that said "in 24h" was reading a quarter
    of an hour. The samples keep the counts. They do not keep job ids, which is
    why the retry prescription still reads the queue — a failure whose row has
    been trimmed cannot be retried anyway.

    Counts before the sampler existed are missing rather than wrong; the window
    fills in as it runs.

    Not ``_counts``: that one upper-cases its keys because it was written for
    symbols, and a job kind is lower-case.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT kind, sum(failed_delta)::bigint
                FROM ops_jobs.queue_sample
                WHERE sample_ts >= %s
                GROUP BY 1 HAVING sum(failed_delta) > 0
                """,
                (since,),
            )
            rows = cur.fetchall() if hasattr(cur, "fetchall") else []
    except Exception as exc:  # noqa: BLE001 — no samples means fall back to the rows
        logger.warning("doctor failure-sample read failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return None
    out: dict[str, int] = {}
    for row in rows or []:
        if isinstance(row, Mapping):
            values = list(row.values())
            kind, n = values[0], values[1]
        else:
            kind, n = row[0], row[1]
        if kind:
            out[str(kind).strip()] = int(n or 0)
    return out


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
    """The session the tables should hold by now — see ``session.resolve_session``.

    Re-exported so the doctor's callers keep their import; the definition lives
    in one module because there used to be four of it.
    """
    return _resolve_session(
        conn,
        now,
        is_trading_day=is_trading_day,
        fetch_completed_trading_days=fetch_completed_trading_days,
    )


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
    # The population the collector actually works on. Since the three-tier rule
    # landed, ``eod-pipeline`` enqueues research.option_universe together with
    # the benchmarks -- 575 names -- while this check still divided by the
    # 28-name watchlist it predated, so a session that collected 26 of them read
    # healthy for weeks.
    tier_of: dict[str, str] = {}
    for row in load_research_universe(conn) or []:
        sym = storage_underlying(str(row.get("symbol") or ""))
        if sym:
            tier_of[sym] = str(row.get("tier") or "")
    universe = sorted(
        {storage_underlying(s) for s in union_iv_radar_benchmarks(symbols, cfg)} | set(tier_of)
    )
    optionable = filter_optionable_underlyings(conn, universe)
    # Two questions, because the collector asks two different things of these
    # names. Resident names -- and anything outside the rule, the benchmarks
    # included -- are snapshotted whole, so they answer a ratio. Core and edge
    # get the near-the-money window on purpose (``config/schedule.yaml``
    # eod-pipeline: the next few expiries, strikes within +-15% of spot), so
    # dividing their rows by the full contract catalogue would report the design
    # as a shortfall. They answer presence instead.
    windowed = [s for s in optionable if tier_of.get(s) in WINDOWED_TIERS]
    whole_chain = [s for s in optionable if tier_of.get(s) not in WINDOWED_TIERS]

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
            # The live catalogue is read for every name: the ratio divides by it
            # and the presence check uses it to tell "nothing to collect" apart
            # from "not collected".
            (optionable, session, day_end),
        )
        # ``count(DISTINCT option_ticker)`` is what a ratio needs and it is the
        # costly shape — 5.5s for the 27 whole-chain names on DEV, 11.0s when it
        # covered all 575. Presence needs no DISTINCT, so the windowed names get
        # a plain count in ``_presence_findings``: 0.6s for 543 of them.
        snap = _counts(
            conn,
            """
            SELECT underlying, count(DISTINCT option_ticker)::bigint
            FROM raw_market.option_snapshot
            WHERE underlying = ANY(%s) AND snapshot_ts >= %s AND snapshot_ts < %s
            GROUP BY 1
            """,
            (whole_chain, day_start, day_end),
        )
        oi = _counts(
            conn,
            """
            SELECT underlying, count(*)::bigint FROM raw_market.option_open_interest
            WHERE underlying = ANY(%s) AND trade_date = %s GROUP BY 1
            """,
            (whole_chain, session),
        )
        if live is None or snap is None or oi is None:
            findings.append(
                Finding(
                    f"option_chain:{session_s}",
                    "eod-pipeline",
                    "warn",
                    "Option chain coverage",
                    len(optionable),
                    None,
                    "coverage query failed — see API log",
                    session=session_s,
                )
            )
        else:
            whole_set = set(whole_chain)
            live_whole = {u: n for u, n in live.items() if u in whole_set}
            # A check with no population is not a passing check: emit the ratio
            # only when there is a whole chain to divide by.
            if live_whole:
                findings.append(
                    _coverage_finding(
                        "option_snapshot",
                        "Option chain snapshot",
                        live_whole,
                        snap,
                        session=session,
                        fixable=fixable,
                    )
                )
                findings.append(
                    _coverage_finding(
                        "option_open_interest",
                        "Open interest",
                        live_whole,
                        oi,
                        session=session,
                        fixable=fixable,
                    )
                )
            # An underlying the vendor lists no unexpired contracts for has
            # nothing to collect, and calling that a gap is the mis-attribution
            # C-B3 exists to prevent: five names (CIX, EA, ISTR, NVR, SENEA)
            # read as missing on the 2026-09-08 session for that reason alone.
            expect_present = [s for s in windowed if live.get(s, 0) > 0]
            if expect_present:
                findings.extend(
                    _presence_findings(
                        conn,
                        expect_present,
                        session=session,
                        session_s=session_s,
                        day_start=day_start,
                        day_end=day_end,
                        fixable=fixable,
                    )
                )

    # ── Stock EOD: whole market + watchlist for the session ──
    n_daily = _count(
        conn, "SELECT count(*) FROM raw_market.stock_daily WHERE bar_date = %s", (session,)
    )
    findings.append(
        Finding(
            f"stock_daily:{session_s}",
            "universe-daily",
            "ok" if n_daily >= STOCK_DAILY_MIN_ROWS else "crit",
            "Stock daily bars (whole market)",
            f">= {STOCK_DAILY_MIN_ROWS}",
            n_daily,
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
                    f"stock_daily_watchlist:{session_s}",
                    "stock-eod",
                    "ok" if not missing else "warn",
                    "Stock daily bars (watchlist)",
                    len(symbols),
                    len(have),
                    f"{len(have)}/{len(symbols)} watchlist symbols have a {session_s} bar.",
                    session=session_s,
                    fix=_slot_fix("stock-eod", session) if missing else None,
                    auto_fixable=bool(missing),
                    missing_sample=missing[:20],
                )
            )

    n_snap = _count(
        conn, "SELECT count(*) FROM raw_market.stock_snapshot WHERE session_date = %s", (session,)
    )
    findings.append(
        Finding(
            f"stock_snapshot:{session_s}",
            "stock-snapshot",
            "ok" if n_snap >= STOCK_SNAPSHOT_MIN_ROWS else "warn",
            "Stock snapshot (whole market)",
            f">= {STOCK_SNAPSHOT_MIN_ROWS}",
            n_snap,
            f"{n_snap} stock_snapshot rows for {session_s}."
            + (
                ""
                if n_snap >= STOCK_SNAPSHOT_MIN_ROWS or session_is_today
                else " The vendor snapshot is point-in-time; a catch-up lands under today's date."
            ),
            session=session_s,
            fix=None if n_snap >= STOCK_SNAPSHOT_MIN_ROWS else _slot_fix("stock-snapshot", session),
            auto_fixable=n_snap < STOCK_SNAPSHOT_MIN_ROWS and session_is_today,
        )
    )

    # ── Financials & Ratios by date (published the morning after) ──
    n_ratios = _count(
        conn, "SELECT count(*) FROM raw_market.ratios WHERE period_date = %s", (session,)
    )
    n_sv = _count(
        conn, "SELECT count(*) FROM raw_market.short_volume WHERE period_date = %s", (session,)
    )
    fund_ok = n_ratios >= RATIOS_MIN_ROWS and n_sv >= SHORT_VOLUME_MIN_ROWS
    # C-F2: overdue is measured against the deadline the contract declares, not
    # against a calendar rollover. This read `session_is_today`, which flips at
    # New York midnight — 04:00 UTC in daylight time, half an hour before the
    # 04:30 UTC slot publishes — so on 2026-09-09 at 04:10 UTC the doctor called
    # the whole session critical for data that was not yet due.
    fund_due = deadline(session, FUNDAMENTALS_MARKET_DEADLINE_H)
    fund_overdue = now_utc >= fund_due
    findings.append(
        Finding(
            f"fundamentals_market:{session_s}",
            "fundamentals-market",
            "ok" if fund_ok else ("crit" if fund_overdue else "warn"),
            "Ratios + short volume (whole market)",
            f"ratios >= {RATIOS_MIN_ROWS}, short_volume >= {SHORT_VOLUME_MIN_ROWS}",
            {"ratios": n_ratios, "short_volume": n_sv},
            f"ratios={n_ratios}, short_volume={n_sv} rows for {session_s}."
            + (
                f" Published the morning after the session; due {fund_due:%Y-%m-%d %H:%M} UTC."
                if not fund_ok and not fund_overdue
                else ""
            ),
            session=session_s,
            fix=None if fund_ok else _slot_fix("fundamentals-market", session, force=False),
            auto_fixable=not fund_ok and fund_overdue,
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
                f"stale:{slot}",
                slot,
                "warn" if stale else "ok",
                f"{slot} freshness",
                f"< {max_age_h:g}h",
                None if age_h is None else round(age_h, 1),
                (
                    f"freshness.{dim} is {age_h:.1f}h old (limit {max_age_h:g}h)."
                    if age_h is not None
                    else f"freshness.{dim} has never been written."
                ),
                fix=_slot_fix(slot, None) if stale else None,
                auto_fixable=stale,
            )
        )

    # ── Queue: failed jobs in the last day, stuck running rows ──
    # How many failed is history and comes from the samples; which ones can be
    # retried is a fact about the queue right now and comes from the rows still
    # on it. They are different numbers, and the finding says so when they are.
    since_24h = now_utc - timedelta(hours=24)
    failed_counts = _failed_since(conn, since_24h)
    on_queue: dict[str, tuple[int, str, list[int]]] = {}
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
                (since_24h,),
            )
            for row in list(cur.fetchall() or []):
                kind = str(row.get("kind") if isinstance(row, Mapping) else row[0])
                on_queue[kind] = (
                    int(row.get("n") if isinstance(row, Mapping) else row[1]),
                    str((row.get("sample_error") if isinstance(row, Mapping) else row[2]) or ""),
                    [int(i) for i in list((row.get("ids") if isinstance(row, Mapping) else row[3]) or [])[:50]],
                )
    except Exception as exc:  # noqa: BLE001
        logger.warning("doctor failed-jobs read failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass

    for kind in sorted(set(failed_counts or {}) | set(on_queue)):
        present, sample, ids = on_queue.get(kind, (0, "", []))
        recorded = (failed_counts or {}).get(kind)
        n = recorded if recorded is not None else present
        if n <= 0:
            continue
        unentitled = "not entitled" in sample.lower() or "upgrade your plan" in sample.lower()
        detail = f"{n} {kind} job(s) failed in 24h"
        if sample:
            detail += f" — {sample[:160]}"
            if recorded is not None and present < n:
                detail += f" — {present} still on the queue and retryable"
        else:
            detail += " — the rows have been trimmed, so no error text or retry survives"
        if unentitled:
            detail += " — the plan does not cover this data; not retried."
        findings.append(
            Finding(
                f"failed:{kind}",
                "queue",
                "warn",
                f"Failed jobs: {kind}",
                0,
                n,
                detail,
                fix=None
                if unentitled or not ids
                else {"action": "retry-jobs", "kind": kind, "job_ids": ids},
                auto_fixable=not unentitled and bool(ids),
            )
        )
    # ── Partition runway: a row with nowhere to land is rejected, not degraded ──
    runway = [
        row
        for row in (_partition_runway(conn) or [])
        if row[0] not in RETIRED_PARTITIONED_TABLES
    ]
    for parent, reaches, owned in runway:
        if reaches is None:
            continue
        days_left = (reaches - session).days
        if days_left >= PARTITION_RUNWAY_MIN_DAYS:
            continue
        detail = (
            f"raw_market.{parent} has partitions through {reaches.isoformat()}, "
            f"{days_left} days out; inserts past that are rejected."
        )
        detail += (
            " The plugin owns the table and builds them ahead automatically."
            if owned
            else " The plugin does not own the table and cannot create the next one —"
            " run `make ownership-sql` and pipe it into a privileged session."
        )
        findings.append(
            Finding(
                f"partition_runway:{parent}",
                "trim",
                "crit" if days_left < 14 or not owned else "warn",
                f"Partition runway: {parent}",
                f">= {PARTITION_RUNWAY_MIN_DAYS} days",
                f"{days_left} days",
                detail,
                session=session_s,
            )
        )
    # Ownership is a standing condition, not a countdown: it guarantees the
    # runway will one day run out with no way to extend it. Reported once, now,
    # rather than as a surprise the month it starts to matter.
    unowned = sorted(parent for parent, _reaches, owned in runway if not owned)
    if unowned:
        findings.append(
            Finding(
                "partition_ownership",
                "trim",
                "warn",
                "Partition ownership",
                0,
                len(unowned),
                f"{len(unowned)} partitioned table(s) in raw_market are owned by another "
                f"role, so the plugin can neither drop nor create their partitions: "
                f"{', '.join(unowned)}. Retention deletes rows instead, but the next "
                f"partition cannot be built — run `make ownership-sql` and pipe it "
                f"into a privileged session.",
                session=session_s,
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
            Finding(
                "stuck_running",
                "queue",
                "warn",
                "Stuck running jobs",
                0,
                stuck,
                f"{stuck} job(s) have been running past the stale limit; the workers reclaim them on their next tick.",
            )
        )

    # ── Workers and vendor (informational: fixes live outside the plugin) ──
    for pool, health in (worker_health or {}).items():
        if health is None:
            findings.append(
                Finding(
                    f"worker:{pool}",
                    "workers",
                    "crit",
                    f"{pool} workers",
                    "reachable",
                    "unreachable",
                    f"/health for the {pool} pool did not answer.",
                    fix={"action": "rollout-restart", "deployment": f"polygon-worker-{pool}"},
                    auto_fixable=False,
                )
            )
            continue
        last_claim = health.get("last_claim_at")
        lag = health.get("loop_lag_sec")
        # The handlers write synchronously, so a heavy batch holds the loop.
        # Say so rather than calling a busy pool healthy or dead.
        busy = isinstance(lag, (int, float)) and lag > WORKER_LOOP_LAG_WARN_SEC
        findings.append(
            Finding(
                f"worker:{pool}",
                "workers",
                "warn" if busy else "ok",
                f"{pool} workers",
                f"loop lag < {WORKER_LOOP_LAG_WARN_SEC:g}s",
                "reachable" if not busy else f"lag {lag}s",
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
            Finding(
                "vendor",
                "vendor",
                sev,
                "Vendor API",
                "HTTP 200",
                code if reach else "unreachable",
                vendor.get("detail")
                or ("marketstatus/now answered" if sev == "ok" else "vendor probe failed"),
                fix=None if sev == "ok" else {"action": "check-vendor-key"},
                auto_fixable=False,
            )
        )

    # ── Continuity: sessions that never landed and can still be refilled ──
    # In a guard of its own. This is the newest check and the only one that
    # looks past the current session; a fault in it must not erase the
    # per-session findings that already succeeded.
    try:
        findings.extend(_continuity_findings(conn, today=now_utc.date()))
    except Exception as exc:  # noqa: BLE001
        logger.warning("continuity findings failed: %s", exc)
        _rollback(conn)

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
        prescriptions.append(
            {"finding_ids": [g.id for g in findings if g.fix == f.fix and g.auto_fixable], **f.fix}
        )

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
        "universe": {
            "watchlist": len(symbols),
            "underlyings": len(universe),
            "optionable": len(optionable),
            # How the chain checks split it: whole chains answer a ratio, the
            # windowed tiers answer presence.
            "whole_chain": len(whole_chain),
            "windowed": len(windowed),
        },
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


def probe_vendor(
    api_key: str, *, rest_base: str = "https://api.polygon.io", timeout: float = 5.0
) -> dict[str, Any]:
    """One cheap authenticated GET: reachable? key accepted?"""
    if not api_key:
        return {"reachable": False, "status_code": None, "detail": "no API key configured"}
    req = urllib.request.Request(
        f"{rest_base.rstrip('/')}/v1/marketstatus/now",
        headers={"Authorization": f"Bearer {api_key}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return {
                "reachable": True,
                "status_code": resp.status,
                "detail": "marketstatus/now answered",
            }
    except urllib.error.HTTPError as exc:
        return {
            "reachable": True,
            "status_code": exc.code,
            "detail": f"vendor answered HTTP {exc.code}",
        }
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
                entry["result"] = {
                    k: res.get(k)
                    for k in ("enqueued", "deduped", "skipped", "reason", "target_date")
                }
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
            int(a["result"].get("enqueued") or 0)
            for a in actions
            if isinstance(a.get("result"), dict)
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
    return {
        "enqueued": sum(1 for i in ids if i is not None),
        "deduped": sum(1 for i in ids if i is None),
    }
