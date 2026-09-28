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
import statistics
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

from bifrost_market_data.ingest._upsert import session_anchor
from bifrost_market_data.ingest.index_options import VENDOR_SPOT_SOURCE, storage_underlying
from bifrost_market_data.quality import fetch_completed_trading_days, filter_optionable_underlyings
from bifrost_market_data.scheduler.daily import (
    OPTION_PLAN_RETRY_DAYS,
    OPTION_PLAN_VOID_PREFIX,
    SKIP_ON_HOLIDAY_SLOTS,
    enqueue_slot,
    load_research_universe,
    load_snapshot_windows,
    load_watchlist_symbols,
    plan_option_depth,
    union_iv_radar_benchmarks,
)
from bifrost_market_data.symbol_void import load_voids_by_prefix
from bifrost_market_data.scheduler.enqueue import insert_jobs_bulk
from bifrost_market_data.subscription import SLOT_REQUIREMENTS
from bifrost_market_data.trading_calendar import (
    chain_session,
    expected_trading_days,
    is_trading_day,
)

from bifrost_market_data.contracts import (
    CONTRACTS,
    STOCK_DAILY_MIN_SESSION_SYMBOLS,
    contract_for,
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
#: The floors above say "the pull happened"; this says "it came back whole". They
#: date from before the feeds reached full width, and ratios ran ~4,790 a session
#: against 2,000, so 2026-09-21 and 09-22 landed a third short (3,217) and read
#: ok — and ratios cannot be asked for again once the vendor moves on. A session
#: must also reach this share of the median of the ones before it.
SESSION_COUNT_FLOOR_RATIO = 0.9
SESSION_COUNT_BASELINE_SESSIONS = 10
#: Hours after the close by which the whole-market ratio and short-volume pull
#: is due — the tightest of the contracts the `fundamentals-market` slot owns.
FUNDAMENTALS_MARKET_DEADLINE_H = staleness_by_slot()["fundamentals-market"][1]
#: The same slot run carries short volume for the session and ratios for the
#: one before it. Asking for the session's own ratios read 0 rows every hour the
#: session was current — warn on weekdays, critical from Sunday 02:00 UTC to
#: Monday evening — while the table held every session the vendor had issued.
RATIOS_LAG_SESSIONS = contract_for("raw_market.ratios").publication_lag_sessions

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
# A session's chain can arrive whole and still be wrong. Measured by Research on
# the 2026-09-22 session: twelve underlyings held about half their usual number
# of IV-bearing contracts at an IV two to three times higher, with the sessions
# on either side normal and the same day's large caps fine. AJG's own IV that
# day solved to 0.337 out of option_daily, matching its neighbours -- so the
# quotes were right and the snapshot of them was not. NET on 09-17 and SHW on
# 09-11 look the same.
#
# Neither half of the signal is usable alone. A thin session is ordinary when
# the vendor lists fewer contracts that day, and IV moves on its own. Both at
# once, on one name, on one session, is not something a market does.
#
# Nothing here can be repaired later: the vendor's chain endpoint only ever
# returns the current session, so this has to be found and refetched the same
# evening. The self-heal runs 00:45 UTC, which is 20:45 in New York -- after the
# 18:00 collection and before the next open -- so the window exists, and
# ``fixable`` is what decides whether it is still open.
DEGRADED_SNAPSHOT_BASELINE_SESSIONS = 5
DEGRADED_SNAPSHOT_COUNT_RATIO = 0.60
DEGRADED_SNAPSHOT_IV_RATIO = 1.80
#: Under this many IV-bearing contracts on the baseline the two ratios are
#: rounding noise, not a signal: half of six is three.
DEGRADED_SNAPSHOT_MIN_BASELINE_ROWS = 20
#: Six sessions of IV rows across the optionable universe is the widest read in
#: the report, so it gets its own budget and answers `unprobed` when it runs out
#: rather than a clean bill. A cancelled query is not a reading of zero.
DEGRADED_SNAPSHOT_TIMEOUT = "25s"
#: One exact anchor across the optionable universe, measured 2026-09-27 at 1.8s
#: (1.0s before the ticker join). The same read over a whole session's range
#: costs 3.3s for an identical answer, because the spot is a fact about
#: (underlying, session) and reading more of the chain cannot sharpen it.
CHAIN_SPOT_TIMEOUT = "20s"

# Above this the worker loop is wedged behind synchronous batch writes.
WORKER_LOOP_LAG_WARN_SEC = 60.0

DEFAULT_WORKER_HEALTH_URLS = {
    "stocks": "http://market-data-health-stocks:8080/health",
    "options": "http://market-data-health-options:8080/health",
}


#: The severity vocabulary, worst first. ``boundary`` is the Console's own
#: definition, carried over from the coverage matrix: "a boundary is not a gap:
#: the vendor cannot backfill it". So it separates a limit no action on this side
#: can move from a fault that a refetch would fix — a distinction the three-value
#: vocabulary had to spend either ``warn`` or ``ok`` on, and got wrong both ways.
#: ``warn`` left the report permanently amber for something unfixable, which is
#: how a surface teaches people to ignore it; ``ok`` painted it green, which
#: claims a vendor number nobody holds.
SEVERITIES: tuple[str, ...] = ("crit", "warn", "boundary", "ok")


@dataclass
class Finding:
    id: str
    slot: str
    #: One of ``SEVERITIES``. Only ``crit`` and ``warn`` move a verdict.
    severity: str
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


#: Rows per publication date for the whole-market tables judged against their
#: own recent sessions. Table and column are fixed here, never taken from input.
_SESSION_BASELINE_SQL = {
    "ratios": (
        "SELECT period_date, count(*)::bigint /* doctor: session-baseline */ "
        "FROM raw_market.ratios WHERE period_date < %s AND period_date >= %s "
        "GROUP BY 1 ORDER BY 1 DESC LIMIT %s"
    ),
    "short_volume": (
        "SELECT period_date, count(*)::bigint /* doctor: session-baseline */ "
        "FROM raw_market.short_volume WHERE period_date < %s AND period_date >= %s "
        "GROUP BY 1 ORDER BY 1 DESC LIMIT %s"
    ),
}


def _session_floor(conn: Any, dataset: str, before: date, absolute: int) -> int:
    """The row count ``dataset`` owes for the session after ``before``.

    The absolute floor, or ``SESSION_COUNT_FLOOR_RATIO`` of the median of the
    last ``SESSION_COUNT_BASELINE_SESSIONS`` dates, whichever is higher. With
    fewer than three dates to judge by — a new feed, a failed read — the
    absolute floor stands alone.
    """
    counts = _counts(
        conn,
        _SESSION_BASELINE_SQL[dataset],
        (before, before - timedelta(days=31), SESSION_COUNT_BASELINE_SESSIONS),
    )
    values = [n for n in (counts or {}).values() if n > 0]
    if len(values) < 3:
        return absolute
    return max(absolute, int(statistics.median(values) * SESSION_COUNT_FLOOR_RATIO))


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


#: One underlying whose EOD chain arrived thin and hot on a session.
@dataclass
class DegradedSnapshot:
    underlying: str
    iv_rows: int
    iv_median: float
    base_rows: float
    base_iv_median: float

    @property
    def row_ratio(self) -> float:
        return self.iv_rows / self.base_rows if self.base_rows else 1.0

    @property
    def iv_ratio(self) -> float:
        return self.iv_median / self.base_iv_median if self.base_iv_median else 1.0


def _degraded_snapshots(
    conn: Any,
    underlyings: Sequence[str],
    *,
    session: date,
    baseline: Sequence[date],
) -> list[DegradedSnapshot] | None:
    """Names whose session chain is both much thinner and much hotter than usual.

    ``None`` means the question could not be asked -- an unreadable calendar, a
    cancelled query -- and the caller must report that rather than an empty list.

    Only EOD rows are compared. Their ``snapshot_ts`` is exactly the session's
    16:00 New York anchor, so the comparison is an equality on the
    ``(underlying, snapshot_ts)`` index and the intraday observations, which
    carry their own instants, cannot drift into the baseline.
    """
    syms = sorted({str(u).strip().upper() for u in underlyings if str(u).strip()})
    base_days = [d for d in baseline if d != session]
    if not syms or not base_days:
        return None
    anchors = [session_anchor(d) for d in [session, *base_days]]
    this_anchor = session_anchor(session)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL statement_timeout = '{DEGRADED_SNAPSHOT_TIMEOUT}'")
            cur.execute(
                """
                /* doctor: degraded-snapshot */
                SELECT underlying, snapshot_ts,
                       count(*)::bigint AS iv_rows,
                       percentile_cont(0.5) WITHIN GROUP (ORDER BY iv) AS iv_median
                FROM raw_market.option_snapshot
                WHERE underlying = ANY(%s) AND snapshot_ts = ANY(%s) AND iv > 0
                GROUP BY 1, 2
                """,
                (syms, anchors),
            )
            rows = cur.fetchall() if hasattr(cur, "fetchall") else []
    except Exception as exc:  # noqa: BLE001 -- an unanswerable check is not a clean one
        logger.warning("degraded-snapshot probe failed: %s", exc)
        _rollback(conn)
        return None

    today: dict[str, tuple[int, float]] = {}
    history: dict[str, list[tuple[int, float]]] = {}
    for row in rows:
        if isinstance(row, Mapping):
            und, ts, n, med = (
                row.get("underlying"), row.get("snapshot_ts"),
                row.get("iv_rows"), row.get("iv_median"),
            )
        else:
            und, ts, n, med = row[0], row[1], row[2], row[3]
        if not und or med is None:
            continue
        key = str(und).strip().upper()
        pair = (int(n or 0), float(med))
        if ts == this_anchor:
            today[key] = pair
        else:
            history.setdefault(key, []).append(pair)

    def _median(xs: list[float]) -> float:
        ordered = sorted(xs)
        mid = len(ordered) // 2
        return ordered[mid] if len(ordered) % 2 else (ordered[mid - 1] + ordered[mid]) / 2

    out: list[DegradedSnapshot] = []
    for und, (n_now, iv_now) in sorted(today.items()):
        past = history.get(und) or []
        # A name new to the universe has no baseline. Not arrived is not a
        # finding, and inventing one out of one prior session would make every
        # addition to research.option_universe look broken on its second day.
        if len(past) < 2:
            continue
        base_n = _median([float(n) for n, _iv in past])
        base_iv = _median([iv for _n, iv in past])
        if base_n < DEGRADED_SNAPSHOT_MIN_BASELINE_ROWS or base_iv <= 0:
            continue
        d = DegradedSnapshot(und, n_now, iv_now, base_n, base_iv)
        if (
            d.row_ratio < DEGRADED_SNAPSHOT_COUNT_RATIO
            and d.iv_ratio > DEGRADED_SNAPSHOT_IV_RATIO
        ):
            out.append(d)
    return out


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

#: Days read before the window so its first sessions have a trailing baseline
#: for the narrow test (``continuity.NEIGHBOURHOOD`` sessions and change).
NARROW_BASELINE_LEAD_DAYS = 21


def _continuity_findings(
    conn: Any,
    *,
    today: date,
    session: date | None = None,
    window_days: int = CONTINUITY_WINDOW_DAYS,
    statement_timeout: str = "30s",
) -> list[Finding]:
    """Sessions that never landed, or landed narrow, and a slot can still refill.

    The doctor has always been a per-session check with no memory: it reported
    2026-08-11 as critical on 2026-08-11 and forgot by the next morning, so the
    hole sat there for a month. The fourth axis has the memory but is a
    read-only measure — what finds a hole could not fix it, and what fixes could
    not find it. This is the join.

    Only datasets whose contract can actually be refilled for a named date are
    considered — ``refill.how`` of ``slot`` or ``kind``. A missed EOD option
    chain is gone for good and a prescription for it would be a lie; a dataset
    whose slot carries its own lookback repairs itself and needs none.

    A contract with ``refill_narrow`` also has its present sessions judged on
    breadth — distinct symbols against the sessions around it (``narrow_days``).
    Only sessions before ``session``: the one being written tonight is narrow
    until its batch drains, and the heal would refill it mid-write.
    """
    from bifrost_market_data.continuity import (
        has_continuity,
        missing_sessions,
        narrow_days,
        per_day_breadth,
    )
    from bifrost_market_data.trading_calendar import expected_trading_days

    start = today - timedelta(days=int(window_days))
    # The narrow test's trailing baseline needs the weeks before the window.
    lead = timedelta(days=NARROW_BASELINE_LEAD_DAYS)
    try:
        calendar = sorted(set(expected_trading_days(conn, start=start - lead, end=today)))
    except Exception as exc:  # noqa: BLE001 — without the calendar there is no question to ask
        logger.warning("continuity findings: calendar unavailable: %s", exc)
        _rollback(conn)
        return []
    sessions = [d for d in calendar if d >= start]
    if not sessions:
        return []

    out: list[Finding] = []
    for c in CONTRACTS:
        if c.refill.how not in ("slot", "kind") or not c.refill.target:
            continue
        if c.cadence != "session" or not has_continuity(c):
            continue
        gaps = missing_sessions(
            conn,
            c.dataset,
            str(c.date_column),
            sessions,
            statement_timeout=statement_timeout,
        )
        if gaps is None:
            continue
        present = [d for d in sessions if d not in set(gaps)]
        if not present:
            continue
        # Only sessions inside the observed span: a dataset that starts midway
        # through the window has not lost the days before it existed, and the
        # newest session may simply not be due yet.
        first, last = present[0], present[-1]
        absent = [d for d in gaps if first <= d <= last]
        narrow: dict[date, tuple[int, int]] = {}
        if c.refill_narrow and c.symbol_column:
            breadth = per_day_breadth(
                conn,
                c.dataset,
                str(c.date_column),
                str(c.symbol_column),
                window_days=int(window_days) + NARROW_BASELINE_LEAD_DAYS,
                statement_timeout=statement_timeout,
                where=c.narrow_filter,
            )
            on_calendar = set(calendar)
            series = [(d, n) for d, n in breadth or [] if d in on_calendar]
            cutoff = session or today
            for day, n, baseline in narrow_days(series):
                if start <= day < cutoff:
                    narrow[day] = (n, baseline)
        name = c.dataset.replace("raw_market.", "")
        if not absent and not narrow:
            out.append(
                Finding(
                    f"continuity:{name}",
                    c.refill.target,
                    "ok",
                    f"Continuity: {name}",
                    f"every session in {window_days}d",
                    f"{len(present)} sessions, none missing"
                    + (", none narrow" if c.refill_narrow else ""),
                    f"No session missing from {name} between {first} and {last}.",
                )
            )
            continue
        # One cap across both: a narrow option-bars day refills as many jobs as
        # an absent one.
        due = sorted(set(absent) | set(narrow))
        prescribed = due[:CONTINUITY_MAX_PRESCRIBED]
        for day in prescribed:
            if day in narrow:
                n, baseline = narrow[day]
                counted = f" with rows where {c.narrow_filter}" if c.narrow_filter else ""
                detail = (
                    f"{name} holds {n} symbols{counted} for {day} against {baseline} in the sessions before it. "
                    f"{len(narrow)} narrow and {len(absent)} missing session(s) in the last {window_days} days"
                )
            else:
                detail = (
                    f"{name} has no rows for {day}, a trading day between {first} and {last}. "
                    f"{len(absent)} such session(s) in the last {window_days} days"
                )
            if len(due) > len(prescribed):
                detail += (
                    f"; prescribing the {len(prescribed)} oldest this run, "
                    f"{len(due) - len(prescribed)} left for the next"
                )
            out.append(
                Finding(
                    f"continuity:{name}:{day.isoformat()}",
                    c.refill.target,
                    "warn",
                    f"Narrow session: {name}" if day in narrow else f"Missing session: {name}",
                    "symbols in line with the sessions before" if day in narrow else "rows for every trading day",
                    f"{narrow[day][0]} symbols" if day in narrow else "no rows",
                    detail + ".",
                    session=day.isoformat(),
                    fix=_refill_fix(c, day),
                    auto_fixable=True,
                    missing_sample=[d.isoformat() for d in due[:10]],
                )
            )
    return out


def _depth_hole_findings(conn: Any, *, cfg: Mapping[str, Any], today: date) -> list[Finding]:
    """Whole months of ``option_daily`` empty after a name's oldest bar, not yet planned.

    Depth used to be judged by the oldest bar alone — in the coverage matrix
    and in the option-depth slot — so a name with a two-year-old first bar read
    at depth whatever was missing in between. 2026-09-28: eleven names held 1–11
    empty months each, KLAC eight in a row before its split. The months come
    from the same ``plan_option_depth`` the slot runs, so what this reports is
    exactly what its prescription would plan; months the slot has already asked
    the vendor about are left out, so a month the vendor has nothing for (AXTI
    2025-06: every contract outside the strike band) is not a standing warning.
    """
    scfg = dict((cfg.get("slots") or {}).get("option-depth") or {})
    months = int(scfg.get("months") or 24)
    months_of: dict[str, int] = {}
    symbols: list[str] = []
    if str(scfg.get("universe") or "").lower() == "research":
        universe = load_research_universe(conn)
        symbols = [u["symbol"] for u in universe]
        months_of = {u["symbol"]: u["history_months"] for u in universe}
    if not symbols:
        symbols = load_watchlist_symbols(conn, cfg)
    names = union_iv_radar_benchmarks(symbols, cfg)
    planned = load_voids_by_prefix(conn, OPTION_PLAN_VOID_PREFIX, max_age_days=OPTION_PLAN_RETRY_DAYS)
    plan = plan_option_depth(
        conn,
        day=today,
        names=names,
        months_of=months_of,
        months=months,
        dte=int(scfg.get("dte") or 90),
        grace=timedelta(days=int(scfg.get("grace_days") or 30)),
        planned=planned,
    )
    if plan is None or not plan["empty_read"]:
        return []
    gaps: dict[str, list[date]] = plan["gaps"]
    open_names = sorted(s for s, m in plan["holes"].items() if m)
    empty_total = sum(len(v) for v in gaps.values())

    def _span(sym: str) -> str:
        m = gaps[sym]
        return f"{sym} {len(m)} ({m[0]:%Y-%m}..{m[-1]:%Y-%m})"

    if not open_names:
        detail = (
            f"No month after a name's oldest bar is empty across {len(names)} names."
            if not gaps
            else (
                f"{empty_total} empty month(s) on {len(gaps)} name(s), every one planned within "
                f"{OPTION_PLAN_RETRY_DAYS} days: {', '.join(_span(s) for s in sorted(gaps)[:8])}."
            )
        )
        return [
            Finding(
                "depth_holes:option_daily",
                "option-depth",
                "ok",
                "Option depth: empty months",
                "no unplanned empty month",
                f"{len(gaps)} names planned" if gaps else "none",
                detail,
            )
        ]
    worst = sorted(open_names, key=lambda s: -len(gaps.get(s, ())))
    return [
        Finding(
            "depth_holes:option_daily",
            "option-depth",
            "warn",
            "Option depth: empty months",
            "no unplanned empty month",
            f"{len(open_names)} names",
            f"{len(open_names)} name(s) hold whole months of option_daily with no bar after their "
            f"oldest one, not yet planned: {', '.join(_span(s) for s in worst[:8])}"
            + (f" and {len(worst) - 8} more" if len(worst) > 8 else "")
            + ". The oldest-bar depth test reads these names as at depth.",
            fix=_slot_fix("option-depth", None, force=False),
            auto_fixable=True,
            missing_sample=worst[:12],
        )
    ]


@dataclass
class ChainSpot:
    """What the view hands Research as the spot behind one underlying's chain."""

    underlying: str
    price: float | None
    source: str | None
    #: True only when ``raw_market.ticker`` still calls the symbol active. An
    #: unpriced chain means something different either side of this: an active
    #: one losing its close is the whole-market pull dropping a name it should
    #: have had, while an inactive one has no close anywhere to be dropped from.
    #: Measured 2026-09-26 on AVB, ISSC, SATS and WBS -- the vendor's reference
    #: listing gives each a ``delisted_utc``, and its own per-ticker aggregates
    #: end on the same session ``stock_daily`` does. The flag is not the cause of
    #: either state; ``stock_daily_grouped`` writes every bar the vendor returns
    #: and never reads it. It only says which of the two a reader is looking at,
    #: and it is all the probe has: we store ``active``, not ``delisted_utc``.
    ticker_active: bool


def _chain_spots(
    conn: Any,
    underlyings: Sequence[str],
    *,
    session: date,
) -> list[ChainSpot] | None:
    """The spot each chain carries for ``session``, read from the view itself.

    Deliberately a read of ``v_option_snapshot_with_stock`` rather than a
    recomputation of what it does: this check exists to report what Research
    actually sees, and a second implementation of the fallback would eventually
    disagree with the first. Returns None when the probe cannot be answered --
    a cancelled query is not a reading of "no spot anywhere".
    """
    syms = sorted({str(u).strip().upper() for u in underlyings if str(u).strip()})
    if not syms:
        return []
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL statement_timeout = '{CHAIN_SPOT_TIMEOUT}'")
            cur.execute(
                """
                /* doctor: chain-spot */
                SELECT DISTINCT ON (v.underlying)
                       v.underlying, v.underlying_price, v.underlying_price_source,
                       COALESCE(t.active, false) AS ticker_active
                FROM raw_market.v_option_snapshot_with_stock v
                LEFT JOIN raw_market.ticker t ON t.symbol = v.underlying
                WHERE v.underlying = ANY(%s) AND v.snapshot_ts = %s
                ORDER BY v.underlying
                """,
                (syms, session_anchor(session)),
            )
            rows = cur.fetchall() if hasattr(cur, "fetchall") else []
    except Exception as exc:  # noqa: BLE001 -- an unanswerable check is not a clean one
        logger.warning("chain-spot probe failed: %s", exc)
        _rollback(conn)
        return None
    out: list[ChainSpot] = []
    for row in rows or []:
        if isinstance(row, Mapping):
            und, price, src, active = (
                row.get("underlying"),
                row.get("underlying_price"),
                row.get("underlying_price_source"),
                row.get("ticker_active"),
            )
        else:
            und, price, src, active = (tuple(row) + (None, None, None, None))[:4]
        if not und:
            continue
        out.append(
            ChainSpot(
                str(und).strip().upper(),
                float(price) if price is not None else None,
                str(src) if src is not None else None,
                bool(active),
            )
        )
    return out


def _sample(names: Sequence[str], limit: int = 12) -> str:
    head = ", ".join(names[:limit])
    return head + (f" (+{len(names) - limit} more)" if len(names) > limit else "")


def _chain_spot_finding(
    conn: Any,
    underlyings: Sequence[str],
    *,
    session: date,
    session_s: str,
) -> Finding:
    """Where the spot behind each chain came from, and who has none.

    Research cannot place an at-the-money strike without one: its primary ATM IV
    read requires ``underlying_price IS NOT NULL``, so an underlying with no spot
    contributes nothing no matter how complete its chain is. SPX sat in exactly
    that state with 29,786 vendor IVs a session until 0.43.0, and nothing said so.

    A derived spot is not a fault -- an index level needs a plan we do not hold,
    and the tracking ETF is the answer we chose. It is reported so that no reader
    mistakes it for a vendor close: SPY x 10 measured 0.41% below SPX on
    2026-09-25, fine for placing a strike and about 1.8 vol points wrong in a
    solve. Having *no* spot is a fault while the ticker is active: the chain is
    invisible and a close was there to be had. Once the vendor has stopped
    listing the name there is no close to fetch, and the same silence is a
    boundary rather than a fault.
    """
    spots = _chain_spots(conn, underlyings, session=session)
    if spots is None:
        return Finding(
            f"chain_spot:{session_s}",
            "eod-pipeline",
            "warn",
            "Spot behind the chain",
            "every chain carries a spot",
            None,
            "unprobed — the view could not be read, so no underlying was cleared "
            "and none was accused. See API log.",
            session=session_s,
        )
    unpriced_live = sorted(s.underlying for s in spots if s.price is None and s.ticker_active)
    unpriced_gone = sorted(
        s.underlying for s in spots if s.price is None and not s.ticker_active
    )
    derived: dict[str, list[str]] = {}
    for s in spots:
        if s.price is not None and s.source and s.source != VENDOR_SPOT_SOURCE:
            derived.setdefault(s.source, []).append(s.underlying)
    vendor = sum(1 for s in spots if s.source == VENDOR_SPOT_SOURCE)

    parts = [f"{vendor} of {len(spots)} chains carry a vendor close for {session_s}."]
    for source in sorted(derived):
        names = sorted(derived[source])
        parts.append(
            f"{len(names)} on a derived spot ({source}): {', '.join(names)} — "
            "good enough to place a strike, not to solve an IV."
        )
    if unpriced_live:
        parts.append(
            f"{len(unpriced_live)} are active tickers with no close for the session, "
            f"so the whole-market pull dropped a name it should have had and "
            f"Research's ATM IV cannot see their chains: {_sample(unpriced_live)}."
        )
    if unpriced_gone:
        parts.append(
            f"{len(unpriced_gone)} carry no spot and raw_market.ticker no longer "
            f"calls them active: {_sample(unpriced_gone)}. The vendor publishes no "
            "close for a name it has stopped listing — their own per-ticker "
            "aggregates end on the same session — so no refetch reaches these. "
            "Their chains are still collected every session and Research cannot "
            "see them; what to do about that is upstream of this check, in who is "
            "still in the option universe."
        )
    # A derived spot and a delisted ticker are both limits the vendor will not
    # move; only a live ticker missing from the whole-market pull is ours.
    severity = "warn" if unpriced_live else ("boundary" if (unpriced_gone or derived) else "ok")
    return Finding(
        f"chain_spot:{session_s}",
        "eod-pipeline",
        severity,
        "Spot behind the chain",
        "every chain carries a spot",
        len(unpriced_live) + len(unpriced_gone),
        " ".join(parts),
        session=session_s,
        missing_sample=(unpriced_live + unpriced_gone)[:12],
    )


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


def _sessions_before(conn: Any, session: date, lag: int) -> date:
    """The trading session ``lag`` sessions before ``session``; itself at zero.

    Filtered rather than sliced: while ``session`` is still today in UTC the
    calendar helper leaves it out, and after midnight it keeps it in.
    """
    if lag <= 0:
        return session
    prior = [d for d in fetch_completed_trading_days(conn, lag + 1, as_of=session) if d < session]
    return prior[-lag] if len(prior) >= lag else session


def _closed_days_since(conn: Any, last: datetime, now: datetime) -> int:
    """New York dates after ``last``'s, through today's, on which the market was shut.

    A holiday-gated slot enqueues nothing when it fires on one of them, so a
    slot that ran on schedule is that many days older than its limit allows
    for without anything having gone wrong.
    """
    first = last.astimezone(_NY).date() + timedelta(days=1)
    today = now.astimezone(_NY).date()
    if today < first:
        return 0
    return (today - first).days + 1 - len(expected_trading_days(conn, start=first, end=today))


def _slot_fix(slot: str, session: date | None, *, force: bool = True) -> dict[str, Any]:
    fix: dict[str, Any] = {"action": "enqueue-slot", "slot": slot, "force": force}
    if session is not None:
        fix["date"] = session.isoformat()
    return fix


def _snapshot_refetch_fix(
    conn: Any,
    names: Sequence[str],
    *,
    session: date,
    tier_of: Mapping[str, str],
    cfg: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Refetch just these underlyings' chains for this session.

    Not ``_slot_fix("eod-pipeline")``: that re-enqueues all 662 underlyings to
    repair twelve, and the whole point of a same-evening repair is that it has
    to fit in the evening. Dedup is a partial index over pending and running
    rows only, so an identical payload whose original job is ``done`` enqueues
    again rather than folding onto it, and the handler upserts onto the same
    session anchor -- the bad rows are replaced, not duplicated.

    The window comes from the same function the slot uses, so a repair buys the
    window the plan says these names get today, not whatever it was when the
    bad rows were written.
    """
    syms = sorted({str(n).strip().upper() for n in names if str(n).strip()})
    if not syms:
        return None
    scfg = dict((cfg.get("slots") or {}).get("eod-pipeline") or {})
    bounded = [s for s in syms if tier_of.get(s) in WINDOWED_TIERS]
    windows: dict[str, tuple[float, float, str]] = {}
    if bounded:
        try:
            windows = load_snapshot_windows(
                conn,
                bounded,
                as_of=session,
                expiries=int(scfg.get("expiries") or 3),
                strike_pct=float(scfg.get("strike_pct") or 0.15),
                min_days=int(scfg.get("min_days") or 0),
                min_strikes_each_side=int(scfg.get("min_strikes_each_side") or 0),
            )
        except Exception as exc:  # noqa: BLE001 -- an unbounded refetch beats none
            logger.warning("refetch window lookup failed; whole chains: %s", exc)
            _rollback(conn)
    payloads: list[dict[str, Any]] = []
    for sym in syms:
        payload: dict[str, Any] = {
            "underlying": storage_underlying(sym),
            "trade_date": session.isoformat(),
        }
        win = windows.get(sym)
        if win is not None:
            payload["strike_gte"], payload["strike_lte"], payload["expiration_lte"] = win
        payloads.append(payload)
    return {"action": "enqueue", "kind": "option_snapshot", "payloads": payloads}


def _refill_fix(c: Any, day: date) -> dict[str, Any]:
    """The call that refills one named session for this dataset.

    A slot where the slot is the right unit, a single job kind where it is not:
    short_volume's slot would also fire ratios_market, whose endpoint ignores
    the date, and short_interest_market, whose own 45-day lookback already
    covers it. Two wasted jobs per repaired session, and the contract says so.
    """
    if c.refill.how == "kind":
        return {
            "action": "enqueue",
            "kind": c.refill.target,
            "payload": {"date": day.isoformat()},
        }
    return _slot_fix(str(c.refill.target), day)


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

        # ── The chain arrived, and nothing can price it ──
        # Coverage answers "did the rows come" and the degraded check below
        # answers "are they the session's". This one answers "can anything place
        # a strike in them", which is a separate way for a complete chain to be
        # useless. Also outside EOD_CRITICAL_CHECKS: it reports, it does not gate.
        findings.append(
            _chain_spot_finding(conn, optionable, session=session, session_s=session_s)
        )

        # ── The chain arrived, and it is wrong ──
        # Coverage above answers "did the rows come"; this answers "are they the
        # session's". Deliberately outside EOD_CRITICAL_CHECKS: widening what
        # blocks the Research batch is a decision about the gate, not a
        # consequence of adding a check. Research already declines to project a
        # session it judges this way (0.118.0, the same rule), so the value here
        # is the refetch, which only this side can do and only tonight.
        try:
            baseline = fetch_completed_trading_days(
                conn, DEGRADED_SNAPSHOT_BASELINE_SESSIONS + 1, as_of=session
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("degraded-snapshot calendar read failed: %s", exc)
            _rollback(conn)
            baseline = []
        degraded = (
            _degraded_snapshots(conn, optionable, session=session, baseline=baseline)
            if baseline
            else None
        )
        if degraded is None:
            findings.append(
                Finding(
                    f"option_snapshot_degraded:{session_s}",
                    "eod-pipeline",
                    "warn",
                    "Degraded option chain snapshots",
                    f"IV rows >= {DEGRADED_SNAPSHOT_COUNT_RATIO:.0%} of the prior "
                    f"{DEGRADED_SNAPSHOT_BASELINE_SESSIONS} sessions",
                    None,
                    "unprobed — the comparison could not be made, so no name was "
                    "cleared and none was accused. See API log.",
                    session=session_s,
                )
            )
        else:
            worst = sorted(degraded, key=lambda d: d.row_ratio)[:8]
            detail = (
                f"{len(degraded)} of {len(optionable)} underlyings hold under "
                f"{DEGRADED_SNAPSHOT_COUNT_RATIO:.0%} of their usual IV-bearing "
                f"contracts at over {DEGRADED_SNAPSHOT_IV_RATIO:.1f}x their usual IV "
                f"for {session_s}."
                if degraded
                else f"No chain arrived thin and hot for {session_s}."
            )
            if worst:
                detail += " Worst: " + ", ".join(
                    f"{d.underlying} {d.iv_rows} rows vs {d.base_rows:.0f} "
                    f"(IV {d.iv_median:.2f} vs {d.base_iv_median:.2f})"
                    for d in worst
                ) + "."
            fix = (
                _snapshot_refetch_fix(
                    conn,
                    [d.underlying for d in degraded],
                    session=session,
                    tier_of=tier_of,
                    cfg=cfg,
                )
                if (degraded and fixable)
                else None
            )
            if degraded and not fixable:
                detail += (
                    " The chain now reflects a later session, so this one can no"
                    " longer be re-observed — it is lost, not pending."
                )
            findings.append(
                Finding(
                    f"option_snapshot_degraded:{session_s}",
                    "eod-pipeline",
                    "warn" if degraded else "ok",
                    "Degraded option chain snapshots",
                    f"IV rows >= {DEGRADED_SNAPSHOT_COUNT_RATIO:.0%} of the prior "
                    f"{DEGRADED_SNAPSHOT_BASELINE_SESSIONS} sessions",
                    len(degraded),
                    detail,
                    session=session_s,
                    fix=fix,
                    auto_fixable=fix is not None,
                    missing_sample=[d.underlying for d in worst],
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
    # Short volume for this session, ratios for the one the vendor has issued
    # by the time the slot runs — see RATIOS_LAG_SESSIONS.
    ratios_session = _sessions_before(conn, session, RATIOS_LAG_SESSIONS)
    ratios_s = ratios_session.isoformat()
    n_ratios = _count(
        conn, "SELECT count(*) FROM raw_market.ratios WHERE period_date = %s", (ratios_session,)
    )
    n_sv = _count(
        conn, "SELECT count(*) FROM raw_market.short_volume WHERE period_date = %s", (session,)
    )
    ratios_floor = _session_floor(conn, "ratios", ratios_session, RATIOS_MIN_ROWS)
    sv_floor = _session_floor(conn, "short_volume", session, SHORT_VOLUME_MIN_ROWS)
    fund_ok = n_ratios >= ratios_floor and n_sv >= sv_floor
    # Landed, and short. Not "not yet due": the publication is already here and
    # came back partial, and ratios can only be asked for again until the vendor
    # issues the next date — the 30h deadline below is days past that. A refetch
    # while the first pull is still writing dedups on the running job's payload.
    fund_short = 0 < n_ratios < ratios_floor or 0 < n_sv < sv_floor
    # C-F2: overdue is measured against the deadline the contract declares, not
    # against a calendar rollover. This read `session_is_today`, which flips at
    # New York midnight — 04:00 UTC in daylight time, half an hour before the
    # 04:30 UTC slot publishes — so on 2026-09-09 at 04:10 UTC the doctor called
    # the whole session critical for data that was not yet due.
    fund_due = deadline(session, FUNDAMENTALS_MARKET_DEADLINE_H)
    fund_overdue = now_utc >= fund_due
    # Not yet due is not a warning. The nightly self-heal runs at 00:45 UTC and
    # the slot at 04:30, so a warning here was the only thing between the
    # doctor and "healthy" on 7 of 7 nights from 09-16 to 09-26 — the verdict
    # said degraded every night and meant nothing by it. `is_late` in
    # ``session`` already draws the line here for the quality gate.
    fund_late = not fund_ok and fund_overdue
    fund_fix = fund_late or fund_short
    findings.append(
        Finding(
            f"fundamentals_market:{session_s}",
            "fundamentals-market",
            "crit" if fund_late else ("warn" if fund_short else "ok"),
            "Ratios + short volume (whole market)",
            f"ratios >= {ratios_floor} for {ratios_s}, short_volume >= {sv_floor}",
            {"ratios": n_ratios, "short_volume": n_sv},
            f"ratios={n_ratios} rows for {ratios_s}, short_volume={n_sv} rows for {session_s}."
            + (
                f" Landed short of {SESSION_COUNT_FLOOR_RATIO:.0%} of the"
                f" {SESSION_COUNT_BASELINE_SESSIONS} sessions before; refetching while"
                " the vendor still serves this publication."
                if fund_short and not fund_late
                else ""
            )
            + (
                " Not yet due: short volume is published the morning after the session"
                f" and ratios a session later; due {fund_due:%Y-%m-%d %H:%M} UTC."
                if not fund_ok and not fund_overdue and not fund_short
                else ""
            ),
            session=session_s,
            fix=_slot_fix("fundamentals-market", session, force=False) if fund_fix else None,
            auto_fixable=fund_fix,
        )
    )

    # ── Staleness of the rotate / reference slots ──
    fresh = _freshness(conn)
    for slot, (dim, max_age_h) in STALENESS.items():
        last = fresh.get(dim)
        age_h = (now_utc - last).total_seconds() / 3600.0 if last else None
        # A slot that skips closed days is not late for them. fundamentals-rotate
        # fires every day and enqueues nothing on the fires that fall on a New
        # York Saturday or Sunday, so its 72-hour weekend gap read stale against
        # 48 hours from Monday 03:00 UTC to Tuesday 03:00 every week — and on
        # 09-15 the self-heal answered with a forced rotation of 4,419 jobs, two
        # hours before the scheduled one enqueued 4,417 more.
        closed = 0
        if last is not None and slot in SKIP_ON_HOLIDAY_SLOTS:
            try:
                closed = _closed_days_since(conn, last, now_utc)
            except Exception as exc:  # noqa: BLE001 — fall back to the flat limit
                logger.warning("doctor closed-day count failed for %s: %s", slot, exc)
                _rollback(conn)
        due_h = None if age_h is None else age_h - 24.0 * closed
        stale = due_h is None or due_h > max_age_h
        findings.append(
            Finding(
                f"stale:{slot}",
                slot,
                "warn" if stale else "ok",
                f"{slot} freshness",
                f"< {max_age_h:g}h",
                None if due_h is None else round(due_h, 1),
                (
                    f"freshness.{dim} is {age_h:.1f}h old"
                    + (
                        f", {due_h:.1f}h not counting {closed} closed day(s) the slot does not run on"
                        if closed
                        else ""
                    )
                    + f" (limit {max_age_h:g}h)."
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
        findings.extend(_continuity_findings(conn, today=now_utc.date(), session=session))
    except Exception as exc:  # noqa: BLE001
        logger.warning("continuity findings failed: %s", exc)
        _rollback(conn)
    try:
        findings.extend(_depth_hole_findings(conn, cfg=cfg, today=now_utc.date()))
    except Exception as exc:  # noqa: BLE001
        logger.warning("depth hole findings failed: %s", exc)
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
        # Every severity is named, so adding one cannot silently drop findings
        # out of the count the way a hard-coded "crit · warn · ok" would.
        "summary": " · ".join(
            f"{sum(1 for f in findings if f.severity == sev)} {label}"
            for sev, label in (
                ("crit", "critical"),
                ("warn", "warning"),
                ("boundary", "boundary"),
                ("ok", "ok"),
            )
        ),
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
            elif pres["action"] == "enqueue":
                # One job kind rather than a whole slot. short_volume's slot
                # would also fire ratios_market, whose endpoint ignores the
                # date, and short_interest_market, whose own 45-day lookback
                # already covers it — two wasted jobs per repaired session.
                #
                # ``payloads`` is the same thing for a named set of underlyings:
                # the degraded-chain repair touches the twelve names that came
                # back wrong, and re-running their slot to reach them would
                # enqueue all 662. One prescription, because it is one decision.
                bodies = [dict(b) for b in (pres.get("payloads") or [])]
                if not bodies:
                    bodies = [dict(pres.get("payload") or {})]
                ids = insert_jobs_bulk(
                    conn,
                    [(str(pres["kind"]), body, 4, 3) for body in bodies],
                )
                entry["result"] = {
                    "enqueued": sum(1 for i in ids if i is not None),
                    "deduped": sum(1 for i in ids if i is None),
                    "kind": pres["kind"],
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
