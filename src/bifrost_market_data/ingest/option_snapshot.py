"""kind=option_snapshot → market.option_snapshot (+ option_contract)."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

from bifrost_market_data.ingest._upsert import (
    as_float,
    as_int,
    batch_upsert,
    daily_snapshot_anchor,
    epoch_ms_to_datetime,
    epoch_ns_to_datetime,
    parse_date,
    parse_option_right,
    parse_option_ticker,
)
from bifrost_market_data.worker.claim import JobRow

_SNAPSHOT_COLS = (
    "option_ticker",
    "underlying",
    "snapshot_ts",
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


def _snapshot_ts(item: Mapping[str, Any]) -> datetime:
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
    # Stable NY-session daily anchor so re-runs upsert the same PK row (idempotent).
    return daily_snapshot_anchor()


def _contract_parts(item: Mapping[str, Any], underlying: str) -> dict[str, Any] | None:
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
    und = str(
        (item.get("underlying_asset") or {}).get("ticker")
        if isinstance(item.get("underlying_asset"), dict)
        else underlying
    ).strip().upper() or underlying

    if expiry is None or strike is None or right is None:
        try:
            parsed = parse_option_ticker(ticker)
            expiry = expiry or parsed["expiry"]
            strike = strike if strike is not None else parsed["strike"]
            right = right or parsed["option_right"]
            und = und or parsed["underlying"]
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
    expiration_date = payload.get("expiration_date")
    contract_type = payload.get("contract_type")

    data = await client.fetch_options_snapshot(
        underlying,
        expiration_date=expiration_date,
        contract_type=contract_type,
    )
    results = list(data.get("results") or [])
    snap_rows: list[tuple[Any, ...]] = []
    contract_rows: list[tuple[Any, ...]] = []
    seen_contracts: set[str] = set()

    for item in results:
        if not isinstance(item, dict):
            continue
        parts = _contract_parts(item, underlying)
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
                _snapshot_ts(item),
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
    try:
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {
        "rows_written": n,
        "contracts_written": n_contracts,
        "underlying": underlying,
        "truncated": bool(data.get("truncated")),
        "pages": data.get("pages"),
    }
