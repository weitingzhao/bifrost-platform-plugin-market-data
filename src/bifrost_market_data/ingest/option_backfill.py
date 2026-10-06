"""kind=option_backfill_plan → many ``option_daily`` jobs for one expiry window.

The vendor prices option history one contract at a time, and two years of a
liquid underlying is tens of thousands of contracts — SPY and SPX each list
over 50,000 in a two-year window. Enumerating and filtering that in an API
request would time out, so one planner job covers one underlying and one
expiry window, applies the Owner's filter (strike within ±30% of the
underlying, at most 90 days of life priced per contract) and bulk-enqueues the
aggregate jobs the workers then drain at the vendor's rate.
"""

from __future__ import annotations

import bisect
import logging
from datetime import date, timedelta
from typing import Any, Mapping

from bifrost_market_data.contracts import OPTION_WINDOW_DAYS
from bifrost_market_data.ingest._upsert import as_float, parse_date
from bifrost_market_data.ingest.contract_pages import PAGE_LIMIT, reject_truncated_catalogue
from bifrost_market_data.ingest.index_options import (
    contracts_api_underlying,
    spot_proxy_for,
    storage_underlying,
)
from bifrost_market_data.scheduler.enqueue import insert_jobs_bulk
from bifrost_market_data.worker.claim import JobRow

logger = logging.getLogger(__name__)

DEFAULT_STRIKE_PCT = 0.30
DEFAULT_DTE = 90
#: One underlying-month of expired contracts; 200 pages of 1,000 (TD-90: it was
#: 200 of 250). The largest month seen used 3 pages.
MAX_CONTRACT_PAGES = 200
INSERT_CHUNK = 500

# History must never outrank the day's own data. The planner is sometimes
# raised above the queue to get its enumeration done, and it used to pass that
# priority down: 486,000 backfill jobs sat above the EOD chain, which would
# have starved the evening's snapshot behind a night of history.
BACKFILL_MAX_PRIORITY = 2


def _today() -> date:
    return date.today()


def history_floor(today: date) -> date:
    """The oldest day the vendor still prices, on ``today``.

    Options Starter serves aggregates for a rolling two years. A contract whose
    last priced day is older than that is refused outright -- HTTP 403, "Your
    plan doesn't include this data timeframe" -- while one that straddles the
    edge is served. Measured on the queue 2026-09-28: 902 jobs whose range ended
    738 days back were all refused (CTAS and DECK, September 2024 expiries, from
    a replan of pre-split months), and 1,823,171 others succeeded with ranges
    ending up to 724 days back and starting up to 814.
    """
    return today - timedelta(days=OPTION_WINDOW_DAYS)


def _closes(conn: Any, underlying: str, start: date, end: date) -> tuple[list[date], list[float]]:
    """The underlying's daily closes over the window, for the strike filter."""
    dates: list[date] = []
    values: list[float] = []
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT bar_date, close FROM raw_market.stock_daily
                WHERE symbol = %s AND bar_date BETWEEN %s AND %s AND close IS NOT NULL
                ORDER BY bar_date
                """,
                (underlying, start, end),
            )
            for row in cur.fetchall() or []:
                d = row.get("bar_date") if isinstance(row, Mapping) else row[0]
                c = row.get("close") if isinstance(row, Mapping) else row[1]
                if isinstance(d, date) and c is not None:
                    dates.append(d)
                    values.append(float(c))
    except Exception as exc:  # noqa: BLE001 — no spot means no filter, not no backfill
        logger.warning("close lookup failed for %s: %s", underlying, exc)
        try:
            conn.rollback()
        except Exception:
            pass
    return dates, values


def _splits_after(conn: Any, underlying: str, since: date) -> list[tuple[date, float]]:
    """``(ex_date, shares after per share before)`` for splits on or after ``since``."""
    out: list[tuple[date, float]] = []
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ex_date, ratio_from, ratio_to FROM raw_market.corporate_action
                WHERE symbol = %s AND action_type = 'split' AND ex_date >= %s
                """,
                (underlying, since),
            )
            for row in cur.fetchall() or []:
                if isinstance(row, Mapping):
                    ex, r_from, r_to = row.get("ex_date"), row.get("ratio_from"), row.get("ratio_to")
                else:
                    ex, r_from, r_to = row[0], row[1], row[2]
                f, t = as_float(r_from), as_float(r_to)
                if isinstance(ex, date) and f and t and f > 0 and t > 0:
                    out.append((ex, t / f))
    except Exception as exc:  # noqa: BLE001 — no split record means the adjusted close, as before
        logger.warning("split lookup failed for %s: %s", underlying, exc)
        try:
            conn.rollback()
        except Exception:
            pass
    return out


def _unadjust_factor(splits: list[tuple[date, float]], when: date) -> float:
    """What turns an adjusted close on ``when`` into the price that traded that day.

    ``stock_daily`` is split-adjusted all the way back; strikes are as traded.
    KLAC split ten for one on 2026-06-12, so its October 2024 closes read ~$70
    against strikes near $700, and every one of the 124–198 contracts a month
    fell outside the ±30% band: two years planned, nothing enqueued.
    """
    factor = 1.0
    for ex, ratio in splits:
        if when < ex:
            factor *= ratio
    return factor


def _spots_over(
    dates: list[date], values: list[float], splits: list[tuple[date, float]], start: date, end: date
) -> list[float]:
    """As-traded closes over a contract's priced window; the last one before it if none.

    A contract is kept when its strike is near the money on **any** day it is
    priced, not only the first. Measured 2026-09-28 on CRWD: banded on the
    window's first close, June 2026 expiries kept strikes up to 572.5 while the
    stock rallied from ~424 to ~780, so 762 of the month's 1,208 contracts —
    the at-the-money ones by June — were never pulled. And a window that spans a
    split holds both scales: the vendor lists a contract open on the ex-date
    under its old ticker too (O:CRWD260717C00800000, 22 pre-split bars) as well
    as the adjusted one, and judging it on the post-split close alone dropped
    every pre-split bar of July–September expiries.
    """
    lo = bisect.bisect_left(dates, start)
    hi = bisect.bisect_right(dates, end)
    idx = range(lo, hi) if hi > lo else range(lo - 1, lo) if lo else range(0)
    return [values[i] * _unadjust_factor(splits, dates[i]) for i in idx]


async def handle_option_backfill_plan(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    """Payload: ``{"underlying", "expiry_gte", "expiry_lte", "strike_pct", "dte"}``."""
    payload = job.payload or {}
    underlying = str(payload.get("underlying") or "").strip().upper()
    if not underlying:
        raise ValueError("option_backfill_plan payload requires underlying")
    expiry_gte = parse_date(payload.get("expiry_gte"))
    expiry_lte = parse_date(payload.get("expiry_lte"))
    if expiry_gte is None or expiry_lte is None:
        raise ValueError("option_backfill_plan payload requires expiry_gte and expiry_lte")
    strike_pct = float(payload.get("strike_pct") or DEFAULT_STRIKE_PCT)
    dte = int(payload.get("dte") or DEFAULT_DTE)
    priority = min(int(job.priority or 0), BACKFILL_MAX_PRIORITY)

    storage = storage_underlying(underlying)
    data = await client.fetch_options_contracts(
        # The contracts reference endpoint takes the plain ticker; the snapshot
        # endpoint's I: form returns nothing here, which silently emptied SPX.
        underlying_ticker=contracts_api_underlying(underlying),
        expired=True,
        expiration_date_gte=expiry_gte.isoformat(),
        expiration_date_lte=expiry_lte.isoformat(),
        max_pages=MAX_CONTRACT_PAGES,
    )
    # A short list would plan a month with its tail missing and report it done.
    reject_truncated_catalogue("option_backfill_plan", storage, data, MAX_CONTRACT_PAGES)
    results = list(data.get("results") or [])
    window_start = expiry_gte - timedelta(days=dte)
    dates, values = _closes(conn, storage, window_start, expiry_lte)
    spot_source = storage
    if not dates:
        # An index level is not in stock_daily (that needs an Indices plan), so
        # fall back to its tracking ETF. Without this SPX keeps every strike:
        # 274,414 contracts across 24 months instead of the ±30% band.
        proxy = spot_proxy_for(storage)
        if proxy is not None:
            symbol, multiplier = proxy
            dates, raw = _closes(conn, symbol, window_start, expiry_lte)
            values = [v * multiplier for v in raw]
            spot_source = f"{symbol}x{multiplier:g}"
    splits = _splits_after(conn, storage, window_start)

    today = _today()
    floor = history_floor(today)
    specs: list[tuple[str, dict[str, Any], int, int]] = []
    no_spot = 0
    out_of_band = 0
    outside_window = 0
    for item in results:
        if not isinstance(item, dict):
            continue
        ticker = str(item.get("ticker") or "").strip().upper()
        expiry = parse_date(item.get("expiration_date"))
        strike = as_float(item.get("strike_price"))
        if not ticker or expiry is None:
            continue
        win_from = expiry - timedelta(days=dte)
        win_to = min(expiry, today)
        if win_to < win_from:
            continue
        if win_to < floor:
            # Every bar it could have is past the vendor's edge: queueing it
            # buys a 403 and a failed job, never a row.
            outside_window += 1
            continue
        spots =[s for s in _spots_over(dates, values, splits, win_from, win_to) if s > 0]
        if not spots:
            no_spot += 1
        elif strike is not None and all(abs(strike - s) / s > strike_pct for s in spots):
            out_of_band += 1
            continue
        specs.append(
            (
                "option_daily",
                {
                    "option_ticker": ticker,
                    # The catalogue's underlying, not the ticker's root. Without
                    # it the handler falls back to parsing, and an adjusted
                    # contract reads O:BDX1… and lands under "BDX1" as a symbol
                    # of its own. Measured 2026-09-10: 2,964,147 option_daily
                    # rows filed that way, ~150k a month through the P4 window
                    # and 425 in September — a backlog this path created and
                    # would have created again on the next backfill.
                    #
                    # option-bars has passed it since 0.21.x; this planner was
                    # missed because `storage` was already in scope, already
                    # correct, and simply never put in the payload.
                    "underlying": storage,
                    "from": win_from.isoformat(),
                    "to": win_to.isoformat(),
                },
                priority,
                3,
            )
        )

    # SPY lists thousands of contracts a month; one statement for all of them
    # times out against a queue table this busy, so insert in bounded chunks.
    ids: list[int | None] = []
    for start in range(0, len(specs), INSERT_CHUNK):
        ids.extend(insert_jobs_bulk(conn, specs[start : start + INSERT_CHUNK]))
    enqueued = sum(1 for i in ids if i is not None)
    return {
        "underlying": storage,
        "expiry_gte": expiry_gte.isoformat(),
        "expiry_lte": expiry_lte.isoformat(),
        "contracts_seen": len(results),
        "contracts_kept": len(specs),
        "out_of_strike_band": out_of_band,
        "outside_history_window": outside_window,
        "history_floor": floor.isoformat(),
        "no_spot_reference": no_spot,
        "spot_source": spot_source,
        "enqueued": enqueued,
        "deduped": len(ids) - enqueued,
        "truncated": False,
        "pages": data.get("pages"),
        "max_pages": MAX_CONTRACT_PAGES,
        "page_limit": PAGE_LIMIT,
    }
