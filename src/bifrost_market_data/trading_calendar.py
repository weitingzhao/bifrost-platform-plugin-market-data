"""US equity session helpers backed by ``market.us_market_holiday``.

Canonical holiday detail lives in Golden Source ``market.us_market_holiday``.
Plugin scheduler / quality / coverage derive trading days as:

  weekday AND NOT (NYSE closed holiday)

``early-close`` rows remain trading days (shortened hours).
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def fetch_closed_holiday_dates(
    conn: Any,
    *,
    start: date | None = None,
    end: date | None = None,
    exchange: str = "NYSE",
) -> set[date]:
    """Return NYSE ``status='closed'`` holiday dates in ``[start, end]`` (inclusive)."""
    clauses = ["exchange = %s", "status = 'closed'"]
    params: list[Any] = [exchange]
    if start is not None:
        clauses.append("holiday_date >= %s")
        params.append(start)
    if end is not None:
        clauses.append("holiday_date <= %s")
        params.append(end)
    sql = f"""
        SELECT holiday_date
        FROM raw_market.us_market_holiday
        WHERE {' AND '.join(clauses)}
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() if hasattr(cur, "fetchall") else []
    except Exception:
        return set()

    out: set[date] = set()
    for row in rows or []:
        if isinstance(row, Mapping):
            raw = row.get("holiday_date") or next(iter(row.values()), None)
        else:
            raw = row[0] if row else None
        d = _as_date(raw)
        if d is not None:
            out.add(d)
    return out


def is_trading_day(conn: Any, d: date) -> bool:
    """True when ``d`` is a NYSE session (weekday, not a closed holiday).

    Missing holiday rows fall back to weekday-only (calendar may be empty).
    """
    if d.weekday() >= 5:
        return False
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM raw_market.us_market_holiday
            WHERE exchange = 'NYSE'
              AND holiday_date = %s
              AND status = 'closed'
            LIMIT 1
            """,
            (d,),
        )
        row = cur.fetchone() if hasattr(cur, "fetchone") else None
    return row is None


def iter_weekdays_excluding(
    *,
    end: date,
    n: int,
    closed: Sequence[date] | set[date],
) -> list[date]:
    """Walk backward from ``end`` and collect up to ``n`` weekday dates not in ``closed``."""
    closed_set = set(closed)
    out: list[date] = []
    cur_d = end
    # Bound scan: weekends + holidays; allow generous slack.
    guard = 0
    max_steps = max(n * 4, n + 60)
    while len(out) < n and guard < max_steps:
        if cur_d.weekday() < 5 and cur_d not in closed_set:
            out.append(cur_d)
        cur_d -= timedelta(days=1)
        guard += 1
    return sorted(out)


def fetch_recent_trading_days(
    conn: Any,
    n: int,
    *,
    as_of: date | None = None,
) -> list[date]:
    """Return up to ``n`` most recent NYSE trading days ending at ``as_of`` (default UTC today)."""
    end = as_of or datetime.now(timezone.utc).date()
    # Look back far enough to cover holidays within the window.
    lookback_start = end - timedelta(days=max(n * 4, n + 60))
    closed = fetch_closed_holiday_dates(conn, start=lookback_start, end=end)
    return iter_weekdays_excluding(end=end, n=int(n), closed=closed)


def expected_trading_days(
    conn: Any,
    *,
    start: date,
    end: date | None = None,
) -> list[date]:
    """All NYSE trading days in ``[start, end]`` (inclusive), weekday − closed holidays."""
    end_d = end or datetime.now(timezone.utc).date()
    if end_d < start:
        return []
    closed = fetch_closed_holiday_dates(conn, start=start, end=end_d)
    out: list[date] = []
    d = start
    while d <= end_d:
        if d.weekday() < 5 and d not in closed:
            out.append(d)
        d += timedelta(days=1)
    return out


_NY = ZoneInfo("America/New_York")
MARKET_OPEN_NY = time(9, 30)


def chain_session(conn: Any, now: datetime | None = None) -> date:
    """The session the vendor's live option chain currently reflects.

    A snapshot download is the chain *as it stands*. Before the next open it
    still shows the last session's close, which is what makes a Saturday
    catch-up for Friday truthful; once Monday opens, the same download shows
    Monday and can no longer be labelled Friday.
    """
    now_ny = (now or datetime.now(timezone.utc)).astimezone(_NY)
    today = now_ny.date()
    if now_ny.time() >= MARKET_OPEN_NY and is_trading_day(conn, today):
        return today
    days = fetch_recent_trading_days(conn, 2, as_of=today)
    prior = [d for d in days if d < today]
    if prior:
        return prior[-1]
    return days[-1] if days else today
