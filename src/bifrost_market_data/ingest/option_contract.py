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
from bifrost_market_data.ingest.contract_pages import (
    PAGE_LIMIT,
    contract_page_cap,
    reject_truncated_catalogue,
)
from bifrost_market_data.ingest.index_options import (
    contracts_api_underlying,
    storage_underlying,
)
from bifrost_market_data.symbol_rename import resolve_storage
from bifrost_market_data.symbol_void import (
    NO_LISTED_OPTIONS,
    clear_symbol_void,
    record_symbol_void,
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
    # SPX lists ~29k live contracts: ~30 pages of 1,000 against a 120-page cap
    # (TD-90 — at the old 250 a page it used 118 of 120).
    max_pages = int(payload.get("max_pages") or contract_page_cap(storage))

    data = await client.fetch_options_contracts(
        underlying_ticker=api_underlying,
        expired=bool(expired),
        expiration_date=payload.get("expiration_date"),
        expiration_date_gte=payload.get("expiration_date_gte"),
        expiration_date_lte=payload.get("expiration_date_lte"),
        max_pages=max_pages,
    )
    reject_truncated_catalogue("option_contract", storage, data, max_pages)
    results = list(data.get("results") or [])
    # Same rename resolution as the chain snapshot: the catalogue is keyed by
    # option_ticker and its underlying is an update column, so a stale request
    # symbol here rewrites rows the snapshot handler had already labelled.
    storage = resolve_storage(
        conn,
        storage,
        [str(i.get("ticker")) for i in results if isinstance(i, dict) and i.get("ticker")],
    )
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

    # The live catalogue, whole: the vendor's answer is the name's verdict. Empty
    # is "no listed options", remembered so option-refresh stops asking every
    # run; rows clear it. Keyed by the requested symbol, which is what the
    # scheduler lists. A dated or expired walk says nothing about today's listing.
    whole_live_walk = expired is False and not any(
        payload.get(k) for k in ("expiration_date", "expiration_date_gte", "expiration_date_lte")
    )
    if whole_live_walk:
        if contract_rows:
            clear_symbol_void(conn, underlying, NO_LISTED_OPTIONS)
        elif not results:
            record_symbol_void(conn, underlying, NO_LISTED_OPTIONS, note="no listed options")

    result: dict[str, Any] = {
        "rows_written": n,
        "expirations_written": n_exp,
        "underlying": storage,
        "api_underlying": api_underlying,
        "truncated": False,
        "pages": data.get("pages"),
        "max_pages": max_pages,
        "page_limit": PAGE_LIMIT,
    }
    # The expiry list comes off the same pages since the option_expiration kind
    # stopped being enqueued, so this handler is that dimension's writer. Without
    # it ops_jobs.ingest_freshness.option_expiration sat frozen at 2026-09-06 and
    # still listed "ok" (TD-169).
    if n_exp:
        result["freshness_extra"] = {"option_expiration": n_exp}
    return result
