"""kind=option_snapshot → market.option_snapshot (+ option_contract + option_open_interest).

One chain download serves all three tables: the snapshot rows, the contract
catalogue, and the session's open interest. A separate ``option_open_interest``
job used to re-download the whole chain for the OI field alone.

``snapshot_ts`` is the time we observed the chain — 16:00 New York of the
session for an EOD run, the actual instant for an intraday one. It used to be
the contract's last trade time, which filed a quiet contract under the day it
last traded, so a session's chain was never complete and a later fetch
overwrote older rows in place. The last trade time now lives in
``last_trade_ts``, where it describes the contract instead of keying the row.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, time, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from bifrost_market_data.ingest._upsert import (
    as_float,
    as_int,
    batch_upsert,
    daily_snapshot_anchor,
    epoch_ms_to_datetime,
    epoch_ns_to_datetime,
    parse_date,
    parse_datetime,
    parse_option_right,
    parse_option_ticker,
    session_anchor,
)
from bifrost_market_data.ingest.index_options import (
    snapshot_api_underlying,
    storage_underlying,
)
from bifrost_market_data.scheduler.enqueue import insert_job
from bifrost_market_data.trading_calendar import chain_session
from bifrost_market_data.worker.claim import JobRow

_SNAPSHOT_COLS = (
    "option_ticker",
    "underlying",
    "snapshot_ts",
    "last_trade_ts",
    "iv",
    "delta",
    "gamma",
    "theta",
    "vega",
    "open_interest",
    "day_open",
    "day_high",
    "day_low",
    "day_close",
    "day_previous_close",
    "day_change_percent",
    "day_volume",
    "day_vwap",
)

_CONTRACT_COLS = (
    "option_ticker",
    "underlying",
    "expiry",
    "strike",
    "option_right",
    "exercise_style",
    "shares_per_contract",
)

logger = logging.getLogger(__name__)

_NY = ZoneInfo("America/New_York")
_MARKET_CLOSE_NY = time(16, 0)

# One continuation covers another 500 pages, far past any listed chain
# (SPX, the largest, is 29 pages); the cap stops a bad cursor chaining forever.
MAX_CHAIN_DEPTH = 20


def _today_ny() -> date:
    return datetime.now(timezone.utc).astimezone(_NY).date()


def session_closed(now: datetime | None = None) -> bool:
    """True once the NY session has closed (16:00 New York)."""
    ny = (now or datetime.now(timezone.utc)).astimezone(_NY)
    return ny.time() >= _MARKET_CLOSE_NY


_OI_COLS = (
    "option_ticker",
    "underlying",
    "expiry",
    "strike",
    "option_right",
    "trade_date",
    "open_interest",
)


def _last_trade_ts(item: Mapping[str, Any]) -> datetime | None:
    last_trade = item.get("last_trade") if isinstance(item.get("last_trade"), dict) else {}
    day = item.get("day") if isinstance(item.get("day"), dict) else {}
    for key in ("sip_timestamp", "participant_timestamp", "timestamp"):
        raw = last_trade.get(key)
        if raw is not None:
            try:
                v = int(raw)
            except (TypeError, ValueError):
                continue
            # ns if huge, else ms
            if v > 10_000_000_000_000:  # > year ~2286 in ms → treat as ns
                return epoch_ns_to_datetime(v)
            return epoch_ms_to_datetime(v)
    lu = day.get("last_updated")
    if lu is not None:
        try:
            v = int(lu)
            if v > 10_000_000_000_000:
                return epoch_ns_to_datetime(v)
            return epoch_ms_to_datetime(v)
        except (TypeError, ValueError):
            pass
    # A contract that has never traded simply has no last trade time.
    return None


def _contract_parts(item: Mapping[str, Any], storage: str) -> dict[str, Any] | None:
    details = item.get("details") if isinstance(item.get("details"), dict) else {}
    ticker = str(details.get("ticker") or item.get("ticker") or "").strip().upper()
    if not ticker:
        return None
    expiry = parse_date(details.get("expiration_date"))
    strike = as_float(details.get("strike_price"))
    try:
        right = parse_option_right(details.get("contract_type"))
    except ValueError:
        right = None
    # Always persist canonical storage underlying (SPX not I:SPX / SPXW).
    und = storage

    if expiry is None or strike is None or right is None:
        try:
            parsed = parse_option_ticker(ticker)
            expiry = expiry or parsed["expiry"]
            strike = strike if strike is not None else parsed["strike"]
            right = right or parsed["option_right"]
        except ValueError:
            return None
    if expiry is None or strike is None or right is None:
        return None

    style = details.get("exercise_style")
    style_s = str(style).strip().lower() if style else None
    spc = as_int(details.get("shares_per_contract"))
    if spc is None:
        spc = 100
    return {
        "option_ticker": ticker,
        "underlying": und,
        "expiry": expiry,
        "strike": strike,
        "option_right": right,
        "exercise_style": style_s,
        "shares_per_contract": spc,
    }


async def handle_option_snapshot(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    payload = job.payload or {}
    underlying = str(payload.get("underlying") or "").strip().upper()
    if not underlying:
        raise ValueError("option_snapshot payload requires underlying")
    storage = storage_underlying(underlying)
    api_underlying = snapshot_api_underlying(underlying)
    expiration_date = payload.get("expiration_date")
    contract_type = payload.get("contract_type")
    trade_date = parse_date(payload.get("trade_date"))
    if trade_date is None:
        trade_date = daily_snapshot_anchor().date()
    # EOD runs key every row to the session anchor, so a catch-up upserts onto
    # the session it is healing instead of inventing one dated today. Intraday
    # runs (several observations per session) carry their own instant.
    intraday = bool(payload.get("intraday"))
    observed_at = parse_datetime(payload.get("observed_at"))
    if observed_at is None:
        observed_at = datetime.now(timezone.utc) if intraday else session_anchor(trade_date)

    # The vendor snapshot is the chain as it stands right now. Labelling it with
    # an older session is only truthful while no session has closed since — the
    # Saturday catch-up for Friday, not a Tuesday backfill of Friday. Refuse the
    # rest rather than write today's greeks under a past session's key.
    if not intraday:
        # An EOD row claims to be the session's close, so it must be observed
        # after that close. Mid-session data stamped 16:00 would be the same
        # class of lie the observation-time model exists to remove.
        if trade_date == _today_ny() and not session_closed():
            return {
                "rows_written": 0,
                "contracts_written": 0,
                "oi_rows_written": 0,
                "trade_date": trade_date.isoformat(),
                "skipped": True,
                "reason": "session_open",
                "detail": f"{trade_date.isoformat()} has not closed yet; an EOD chain must be observed after 16:00 NY",
                "underlying": storage,
            }
        current = chain_session(conn)
        if trade_date != current:
            return {
                "rows_written": 0,
                "contracts_written": 0,
                "oi_rows_written": 0,
                "trade_date": trade_date.isoformat(),
                "skipped": True,
                "reason": "stale_session",
                "detail": (
                    f"the chain now reflects {current.isoformat()}; "
                    f"{trade_date.isoformat()} can no longer be observed"
                ),
                "underlying": storage,
            }

    data = await client.fetch_options_snapshot(
        api_underlying,
        expiration_date=expiration_date,
        contract_type=contract_type,
        cursor=payload.get("cursor") or None,
    )
    results = list(data.get("results") or [])
    snap_rows: list[tuple[Any, ...]] = []
    contract_rows: list[tuple[Any, ...]] = []
    oi_rows: list[tuple[Any, ...]] = []
    seen_contracts: set[str] = set()

    for item in results:
        if not isinstance(item, dict):
            continue
        parts = _contract_parts(item, storage)
        if parts is None:
            continue
        greeks = item.get("greeks") if isinstance(item.get("greeks"), dict) else {}
        day = item.get("day") if isinstance(item.get("day"), dict) else {}
        iv = as_float(item.get("implied_volatility"))
        if iv is None:
            iv = as_float(greeks.get("implied_volatility") or greeks.get("iv"))
        snap_rows.append(
            (
                parts["option_ticker"],
                parts["underlying"],
                observed_at,
                _last_trade_ts(item),
                iv,
                as_float(greeks.get("delta")),
                as_float(greeks.get("gamma")),
                as_float(greeks.get("theta")),
                as_float(greeks.get("vega")),
                as_int(item.get("open_interest")),
                as_float(day.get("open")),
                as_float(day.get("high")),
                as_float(day.get("low")),
                as_float(day.get("close")),
                as_float(day.get("previous_close")),
                as_float(day.get("change_percent")),
                as_int(day.get("volume")),
                as_float(day.get("vwap")),
            )
        )
        ot = parts["option_ticker"]
        oi = as_int(item.get("open_interest"))
        if oi is not None:
            oi_rows.append(
                (
                    ot,
                    parts["underlying"],
                    parts["expiry"],
                    parts["strike"],
                    parts["option_right"],
                    trade_date,
                    oi,
                )
            )
        if ot not in seen_contracts:
            seen_contracts.add(ot)
            contract_rows.append(
                (
                    ot,
                    parts["underlying"],
                    parts["expiry"],
                    parts["strike"],
                    parts["option_right"],
                    parts["exercise_style"],
                    parts["shares_per_contract"],
                )
            )

    n_contracts = batch_upsert(
        conn,
        "market.option_contract",
        _CONTRACT_COLS,
        contract_rows,
        conflict_keys=("option_ticker",),
        update_cols=(
            "underlying",
            "expiry",
            "strike",
            "option_right",
            "exercise_style",
            "shares_per_contract",
        ),
        set_fetched_at=False,
        auto_commit=False,
    )
    n = batch_upsert(
        conn,
        "market.option_snapshot",
        _SNAPSHOT_COLS,
        snap_rows,
        conflict_keys=("option_ticker", "snapshot_ts"),
        update_cols=tuple(c for c in _SNAPSHOT_COLS if c not in ("option_ticker", "snapshot_ts")),
        set_fetched_at=True,
        auto_commit=False,
    )
    n_oi = batch_upsert(
        conn,
        "market.option_open_interest",
        _OI_COLS,
        oi_rows,
        conflict_keys=("option_ticker", "trade_date"),
        update_cols=("underlying", "expiry", "strike", "option_right", "open_interest"),
        set_fetched_at=True,
        auto_commit=False,
    )
    try:
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    # A chain that outran max_pages resumes from the vendor's cursor instead of
    # starting over: the remainder is queued as its own job, bounded so a
    # misbehaving cursor cannot chain forever.
    continuation: int | None = None
    next_cursor = data.get("next_cursor")
    depth = int(payload.get("chain_depth") or 0)
    if next_cursor and depth < MAX_CHAIN_DEPTH:
        continuation = insert_job(
            conn,
            kind="option_snapshot",
            payload={
                **{k: v for k, v in payload.items() if k not in ("cursor", "chain_depth")},
                "cursor": str(next_cursor),
                "chain_depth": depth + 1,
            },
            priority=int(job.priority or 0),
        )
    elif next_cursor:
        logger.warning(
            "option_snapshot %s stopped at chain_depth=%s with a cursor left over",
            storage,
            depth,
        )

    return {
        "rows_written": n,
        "contracts_written": n_contracts,
        "oi_rows_written": n_oi,
        "chain_depth": depth,
        "continuation_job_id": continuation,
        "trade_date": trade_date.isoformat() if isinstance(trade_date, date) else str(trade_date),
        "observed_at": observed_at.isoformat(),
        # The worker touches these freshness dimensions on top of the job's own.
        "freshness_extra": {"option_open_interest": n_oi},
        "underlying": storage,
        "api_underlying": api_underlying,
        "truncated": bool(data.get("truncated")),
        "pages": data.get("pages"),
    }
