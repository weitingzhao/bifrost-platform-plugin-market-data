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

from bifrost_market_data.ingest._upsert import as_float, parse_date
from bifrost_market_data.ingest.index_options import snapshot_api_underlying, storage_underlying
from bifrost_market_data.scheduler.enqueue import insert_jobs_bulk
from bifrost_market_data.worker.claim import JobRow

logger = logging.getLogger(__name__)

DEFAULT_STRIKE_PCT = 0.30
DEFAULT_DTE = 90
MAX_CONTRACT_PAGES = 200


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


def _close_on_or_before(dates: list[date], values: list[float], when: date) -> float | None:
    idx = bisect.bisect_right(dates, when)
    return values[idx - 1] if idx else None


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
    priority = int(job.priority or 0)

    storage = storage_underlying(underlying)
    data = await client.fetch_options_contracts(
        underlying_ticker=snapshot_api_underlying(underlying),
        expired=True,
        expiration_date_gte=expiry_gte.isoformat(),
        expiration_date_lte=expiry_lte.isoformat(),
        max_pages=MAX_CONTRACT_PAGES,
    )
    results = list(data.get("results") or [])
    dates, values = _closes(conn, storage, expiry_gte - timedelta(days=dte), expiry_lte)

    today = date.today()
    specs: list[tuple[str, dict[str, Any], int, int]] = []
    no_spot = 0
    out_of_band = 0
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
        spot = _close_on_or_before(dates, values, win_from)
        if spot is None:
            no_spot += 1
        elif strike is not None and spot > 0 and abs(strike - spot) / spot > strike_pct:
            out_of_band += 1
            continue
        specs.append(
            (
                "option_daily",
                {"option_ticker": ticker, "from": win_from.isoformat(), "to": win_to.isoformat()},
                priority,
                3,
            )
        )

    ids = insert_jobs_bulk(conn, specs)
    enqueued = sum(1 for i in ids if i is not None)
    return {
        "underlying": storage,
        "expiry_gte": expiry_gte.isoformat(),
        "expiry_lte": expiry_lte.isoformat(),
        "contracts_seen": len(results),
        "contracts_kept": len(specs),
        "out_of_strike_band": out_of_band,
        "no_spot_reference": no_spot,
        "enqueued": enqueued,
        "deduped": len(ids) - enqueued,
        "truncated": bool(data.get("truncated")),
        "pages": data.get("pages"),
    }
