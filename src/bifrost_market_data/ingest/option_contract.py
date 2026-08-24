"""kind=option_contract → market.option_contract (+ option_expiration)."""

from __future__ import annotations

from typing import Any, Mapping

from bifrost_market_data.ingest._upsert import (
    as_float,
    as_int,
    batch_upsert,
    parse_date,
    parse_option_right,
    physical_table_name,
)
from bifrost_market_data.ingest.index_options import (
    contracts_api_underlying,
    is_index_option_underlying,
    storage_underlying,
)
from bifrost_market_data.worker.claim import JobRow

_CONTRACT_COLS = (
    "option_ticker",
    "underlying",
    "expiry",
    "strike",
    "option_right",
    "exercise_style",
    "shares_per_contract",
)

_EXPIRY_COLS = ("underlying", "expiry")


async def handle_option_contract(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    payload = job.payload or {}
    underlying = str(payload.get("underlying") or payload.get("underlying_ticker") or "").strip().upper()
    if not underlying:
        raise ValueError("option_contract payload requires underlying")
    storage = storage_underlying(underlying)
    api_underlying = contracts_api_underlying(underlying)
    expired = payload.get("expired")
    if expired is None:
        expired = False
    # Index chains (SPX) are large — allow a higher page budget when mapped.
    max_pages = int(payload.get("max_pages") or (80 if is_index_option_underlying(storage) else 20))

    data = await client.fetch_options_contracts(
        underlying_ticker=api_underlying,
        expired=bool(expired),
        expiration_date=payload.get("expiration_date"),
        max_pages=max_pages,
    )
    results = list(data.get("results") or [])
    contract_rows: list[tuple[Any, ...]] = []
    expiries: set[Any] = set()

    for item in results:
        if not isinstance(item, dict):
            continue
        ticker = str(item.get("ticker") or "").strip().upper()
        expiry = parse_date(item.get("expiration_date"))
        strike = as_float(item.get("strike_price"))
        if not ticker or expiry is None or strike is None:
            continue
        try:
            right = parse_option_right(item.get("contract_type"))
        except ValueError:
            continue
        und = storage
        style = item.get("exercise_style")
        style_s = str(style).strip().lower() if style else None
        spc = as_int(item.get("shares_per_contract"))
        if spc is None:
            spc = 100
        contract_rows.append((ticker, und, expiry, strike, right, style_s, spc))
        expiries.add((und, expiry))

    exp_rows = sorted(expiries, key=lambda x: (x[0], x[1]))
    try:
        n = batch_upsert(
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
        if contract_rows:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {physical_table_name("market.option_contract")}
                    SET updated_at = now()
                    WHERE option_ticker = ANY(%s)
                    """,
                    ([r[0] for r in contract_rows],),
                )

        n_exp = batch_upsert(
            conn,
            "market.option_expiration",
            _EXPIRY_COLS,
            exp_rows,
            conflict_keys=("underlying", "expiry"),
            update_cols=(),
            set_fetched_at=False,
            auto_commit=False,
        )
        if exp_rows:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {physical_table_name("market.option_expiration")}
                    SET updated_at = now()
                    WHERE underlying = %s
                    """,
                    (storage,),
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return {
        "rows_written": n,
        "expirations_written": n_exp,
        "underlying": storage,
        "api_underlying": api_underlying,
        "truncated": bool(data.get("truncated")),
        "pages": data.get("pages"),
    }
