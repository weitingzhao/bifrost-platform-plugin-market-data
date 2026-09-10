"""The fourth axis: is the middle solid.

Breadth asks how many symbols, depth how far back, freshness whether today's
arrived. All three can read healthy over a dataset full of holes, and on
2026-09-09 all three did: `stock_daily` reported 20,695 symbols, five years of
history and a fresh session, while seven ordinary trading days in the previous
ninety held eighteen rows instead of twelve thousand. The whole-market pull had
failed on those days, the doctor had said so at the time, and nothing kept the
answer — it is a per-session check with no memory.

Measured on read rather than recorded forward. A recorded measure can only see
from the day it was switched on, and seeing backwards is the entire point.

Two numbers, never merged. A day that is *absent* is compared against the
trading calendar; a day that is *present but thin* is compared against what the
feed had been producing in the days before it, not against the window's median
— a universe that grows from 26 names to 575 is a step, not five hundred holes,
and a median taken across the step calls every earlier day a hole.
"""

from __future__ import annotations

import logging
import statistics
from datetime import date, timedelta
from typing import Any, Iterable, Mapping, Sequence

from bifrost_market_data.contracts import DatasetContract

logger = logging.getLogger(__name__)

#: Depth targets that accumulate one set of rows per session, and therefore
#: should have no gaps. A catalogue has no cadence; a point-in-time snapshot has
#: no history; quarterly financials are not a daily series.
CONTINUITY_KINDS = frozenset({"rolling_days", "sessions", "forward_only"})

#: How far back to look. Long enough to see a pattern, short enough that the
#: scan stays a few seconds on a 13.6M-row table.
WINDOW_DAYS = 120

#: How many preceding days define "what this feed had been producing".
NEIGHBOURHOOD = 10

#: Fewer trailing days than this and there is nothing to judge against — the
#: start of a window, or of the dataset's life.
MIN_TRAILING = 3

#: Below this share of the neighbourhood's median, a day is a hole rather than a
#: quiet session. Half is deliberately blunt: the real holes measured 0.1% to 40%
#: of their neighbours, and nothing legitimate sat near the line.
FLOOR_RATIO = 0.5


def has_continuity(contract: DatasetContract) -> bool:
    """Whether a session-level gap means anything for this dataset."""
    return bool(contract.date_column) and contract.depth.kind in CONTINUITY_KINDS


def thin_days(
    counts: Sequence[tuple[date, int]],
    *,
    neighbourhood: int = NEIGHBOURHOOD,
    floor_ratio: float = FLOOR_RATIO,
    min_trailing: int = MIN_TRAILING,
) -> list[tuple[date, int, int]]:
    """Days far below what the feed had been producing, as ``(day, rows, baseline)``.

    The baseline is the preceding days, not the surrounding ones. Against the
    window's own median, the 2026-09-08 expansion from 26 underlyings to 575
    read as twenty-five holes; against a *centred* neighbourhood it still
    mislabels the days either side of any step, because their neighbourhood
    straddles it. Trailing tells the three apart on their own terms: a hole sits
    below what came before it, a step up does not, and the start of a ramp does
    not either. A step *down* still registers, which is right — a feed that
    halves and stays halved is a regression, not a shape.
    """
    ordered = sorted(counts)
    out: list[tuple[date, int, int]] = []
    for i, (day, n) in enumerate(ordered):
        trailing = [c for _d, c in ordered[max(0, i - neighbourhood) : i]]
        if len(trailing) < min_trailing:
            continue
        baseline = statistics.median(trailing)
        if baseline > 0 and n < baseline * floor_ratio:
            out.append((day, n, int(baseline)))
    return out


def _rows(cur: Any) -> list[tuple[Any, ...]]:
    fetched = cur.fetchall() if hasattr(cur, "fetchall") else []
    return [tuple(r.values()) if isinstance(r, Mapping) else tuple(r) for r in fetched or []]


def per_day_counts(
    conn: Any,
    table: str,
    column: str,
    *,
    window_days: int = WINDOW_DAYS,
    statement_timeout: str = "120s",
) -> list[tuple[date, int]] | None:
    """Rows per day over the window, or None when the read failed.

    The table and column come from the contract table, never from a request.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL statement_timeout = '{statement_timeout}'")
            cur.execute(
                f"""
                SELECT {column}::date AS d, count(*)::bigint AS n
                FROM {table}
                WHERE {column} >= now() - make_interval(days => %s)
                GROUP BY 1 ORDER BY 1
                """,
                (int(window_days),),
            )
            rows = _rows(cur)
        # Parsing belongs inside the guard too: an unexpected row shape is as
        # much a failed read as a timeout, and neither may escape.
        return [(r[0], int(r[1] or 0)) for r in rows if r and r[0] is not None]
    except Exception as exc:  # noqa: BLE001 — one unreadable dataset must not sink the page
        logger.warning("continuity read failed for %s: %s", table, exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return None


def missing_sessions(
    conn: Any,
    table: str,
    column: str,
    sessions: Sequence[date],
    *,
    statement_timeout: str = "30s",
) -> list[date] | None:
    """Which of ``sessions`` the table holds no row for at all, or None on a failed read.

    Not ``per_day_counts``: that one was written for the thin-day statistic and
    therefore has to count, and counting to answer a presence question cost the
    doctor about ten seconds across five datasets.

    Nor a probe per day. Measured 2026-09-10, that traded the aggregate for 210
    round trips and the doctor's median barely moved — the cost had become
    latency, not work. The calendar goes to the server as a VALUES list so the
    whole question is one statement.

    LATERAL with LIMIT 1 rather than NOT EXISTS. Against a forty-row outer side
    the planner reads an anti-join as an invitation to hash the whole inner
    relation, and on option_daily that ran past the 30s budget and the dataset
    was skipped — the check silently missing the very table it was built for. A
    lateral with a limit has to be evaluated per day and stops at the first row.

    Half-open bounds rather than a cast, so a timestamp column (``bar_time``) is
    compared on the index instead of through ``::date``.
    """
    days = list(sessions)
    if not days:
        return []
    values = ", ".join(["(%s::date)"] + ["(%s)"] * (len(days) - 1))
    sql = f"""
        SELECT v.d
        FROM (VALUES {values}) AS v(d)
        LEFT JOIN LATERAL (
            SELECT 1 AS hit FROM {table}
            WHERE {column} >= v.d AND {column} < v.d + 1
            LIMIT 1
        ) p ON true
        WHERE p.hit IS NULL
        ORDER BY v.d
    """
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL statement_timeout = '{statement_timeout}'")
            cur.execute(sql, tuple(days))
            rows = cur.fetchall() if hasattr(cur, "fetchall") else []
    except Exception as exc:  # noqa: BLE001 — one unreadable dataset must not sink the check
        logger.warning("session presence read failed for %s: %s", table, exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return None
    out: list[date] = []
    for r in rows or []:
        v = tuple(r.values())[0] if isinstance(r, Mapping) else (r[0] if r else None)
        if v is not None:
            out.append(v)
    return out


def measure(
    conn: Any,
    contract: DatasetContract,
    *,
    window_days: int = WINDOW_DAYS,
    expected_days: Iterable[date] | None = None,
    statement_timeout: str = "120s",
) -> dict[str, Any]:
    """Absent days and thin days over the window, reported separately."""
    if not has_continuity(contract):
        return {"measured": False, "why": f"{contract.depth.kind} has no session cadence"}

    table = contract.dataset
    counts = per_day_counts(
        conn,
        table,
        str(contract.date_column),
        window_days=window_days,
        statement_timeout=statement_timeout,
    )
    if counts is None:
        return {"measured": False, "why": "read failed"}

    # Only a dataset published every trading day can be judged against the
    # trading calendar. short_interest settles twice a month; measured against
    # sessions it reported 56 missing days that were never due. An empty
    # calendar means unreadable, not "no trading days" — filtering on it would
    # blank the axis rather than report it.
    sessions: set[date] | None = None
    if expected_days is not None and contract.cadence == "session":
        cal = set(expected_days)
        sessions = cal or None

    # A row dated on a Saturday is not a thin session. Non-trading days are
    # dropped from the judged series *and* from the baseline it is judged
    # against: measured 2026-09-10, four of option_open_interest's five worst
    # "thin" days were weekends, and leaving those few-row days in the trailing
    # median also drags the floor down where a real weekday hole should trip it.
    judged = counts if sessions is None else [(d, n) for d, n in counts if d in sessions]
    off_calendar = [] if sessions is None else [d for d, _ in counts if d not in sessions]

    present = {d for d, _ in judged}
    holes = thin_days(judged)

    # How often this dataset actually publishes, measured rather than assumed.
    # A settlement series reads 15; a daily one reads 1. Freshness needs it:
    # "27 days since the newest row" is late for a daily feed and routine for
    # short_interest, whose 2026-08-14 settlement was the newest FINRA had
    # published — measured 2026-09-10, with 08-14 / 07-31 / 07-15 / 06-30 /
    # 06-15 all held and no gap between them.
    days_sorted = sorted(present)
    gaps = [(b - a).days for a, b in zip(days_sorted, days_sorted[1:])]
    interval = int(statistics.median(gaps)) if gaps else None

    absent: list[date] = []
    if sessions is not None:
        # Only sessions inside the observed span: a dataset that starts midway
        # through the window has not lost the days before it existed.
        first = min(present) if present else None
        last = max(present) if present else None
        if first is not None and last is not None:
            absent = [d for d in expected_days if first <= d <= last and d not in present]

    return {
        "measured": True,
        "window_days": window_days,
        # Sessions with rows, once non-trading days are set aside — the same
        # unit days_absent counts, so the two can be read against each other.
        "days_present": len(judged),
        # Two numbers, never merged: one is "the day is missing", the other is
        # "the day is there and nearly empty". They have different causes.
        "days_absent": len(absent),
        "cadence": contract.cadence,
        "days_thin": len(holes),
        # Rows dated on a day the market was shut. Not a hole — the opposite —
        # but nothing else in the system would say so.
        "median_interval_days": interval,
        "days_off_calendar": len(off_calendar),
        "off_calendar_sample": [d.isoformat() for d in sorted(off_calendar)[:5]],
        "worst": [
            {"date": d.isoformat(), "rows": n, "neighbours": m}
            for d, n, m in sorted(holes, key=lambda t: t[1])[:5]
        ],
        "absent_sample": [d.isoformat() for d in absent[:5]],
    }


def window_start(window_days: int = WINDOW_DAYS, today: date | None = None) -> date:
    from datetime import datetime, timezone

    base = today or datetime.now(timezone.utc).date()
    return base - timedelta(days=int(window_days))


__all__ = [
    "CONTINUITY_KINDS",
    "WINDOW_DAYS",
    "NEIGHBOURHOOD",
    "FLOOR_RATIO",
    "has_continuity",
    "thin_days",
    "per_day_counts",
    "missing_sessions",
    "measure",
    "window_start",
]
