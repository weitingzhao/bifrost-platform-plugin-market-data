"""URL builders for Polygon REST endpoints used by market-data ingest (P2/P4).

Each builder returns ``(path, query_params)`` relative to the REST base
(``https://api.polygon.io``). Auth ``apiKey`` is injected by ``PolygonClient``.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

DEFAULT_REST_BASE = "https://api.polygon.io"


def aggs_range_path(
    ticker: str,
    *,
    multiplier: int,
    timespan: str,
    from_value: str | int,
    to_value: str | int,
) -> str:
    """``/v2/aggs/ticker/{ticker}/range/{mult}/{timespan}/{from}/{to}``."""
    enc = quote(str(ticker).strip().upper(), safe="")
    return (
        f"/v2/aggs/ticker/{enc}/range/{int(multiplier)}/{timespan}"
        f"/{from_value}/{to_value}"
    )


def aggs_range_params(
    *,
    ticker: str | None = None,
    adjusted: bool | None = None,
    sort: str = "asc",
    limit: int = 50_000,
) -> dict[str, Any]:
    """Build aggs query params.

    Polygon **indices** (``I:…``) and **options** (``O:…``) omit ``adjusted`` —
    forcing ``adjusted=true`` can drop fields like ``vw`` on option contract bars.
    Equities default to ``adjusted=true`` unless ``adjusted`` is explicitly set.
    """
    params: dict[str, Any] = {
        "sort": sort,
        "limit": int(limit),
    }
    t = (ticker or "").strip().upper()
    if adjusted is None:
        if t.startswith("I:") or t.startswith("O:"):
            return params
        params["adjusted"] = "true"
        return params
    params["adjusted"] = "true" if adjusted else "false"
    return params


def options_contracts_path() -> str:
    return "/v3/reference/options/contracts"


def options_contracts_params(
    *,
    underlying_ticker: str | None = None,
    expiration_date: str | None = None,
    expired: bool | None = None,
    limit: int = 250,
    order: str = "asc",
    sort: str = "ticker",
    expiration_date_gte: str | None = None,
    expiration_date_lte: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "limit": min(int(limit), 1000),
        "order": order,
        "sort": sort,
    }
    if underlying_ticker:
        params["underlying_ticker"] = str(underlying_ticker).strip().upper()
    if expiration_date:
        params["expiration_date"] = expiration_date
    if expired is not None:
        params["expired"] = "true" if expired else "false"
    if expiration_date_gte:
        params["expiration_date.gte"] = expiration_date_gte
    if expiration_date_lte:
        params["expiration_date.lte"] = expiration_date_lte
    return params


def options_snapshot_path(underlying: str) -> str:
    # Keep ':' for index tickers (I:SPX); quote only unsafe path chars.
    enc = quote(str(underlying).strip().upper(), safe=":")
    return f"/v3/snapshot/options/{enc}"


def options_snapshot_params(
    *,
    strike_price: float | None = None,
    expiration_date: str | None = None,
    contract_type: str | None = None,
    limit: int = 250,
    strike_gte: float | None = None,
    strike_lte: float | None = None,
    expiration_lte: str | None = None,
) -> dict[str, Any]:
    """Chain snapshot query. The range filters are the near-the-money window:
    a whole chain is 2,000–28,000 contracts per underlying per session, and at
    575 underlyings that is ~130 GB of ninety-session retention. Filtering at
    the vendor keeps both the call and the table proportionate."""
    params: dict[str, Any] = {"limit": min(int(limit), 250)}
    if strike_price is not None:
        params["strike_price"] = strike_price
    if expiration_date:
        params["expiration_date"] = expiration_date
    if contract_type:
        params["contract_type"] = contract_type
    if strike_gte is not None:
        params["strike_price.gte"] = strike_gte
    if strike_lte is not None:
        params["strike_price.lte"] = strike_lte
    if expiration_lte:
        params["expiration_date.lte"] = expiration_lte
    return params


def reference_tickers_path() -> str:
    return "/v3/reference/tickers"


def reference_tickers_params(
    *,
    market: str = "stocks",
    active: bool = True,
    locale: str = "us",
    ticker_type: str | None = "CS",
    limit: int = 1000,
    cursor: str | None = None,
    ticker: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "market": market,
        "active": "true" if active else "false",
        "locale": locale,
        "limit": min(int(limit), 1000),
    }
    if ticker_type:
        params["type"] = ticker_type
    if cursor:
        params["cursor"] = cursor
    if ticker:
        params["ticker"] = str(ticker).strip().upper()
    return params


def ticker_details_path(ticker: str) -> str:
    enc = quote(str(ticker).strip().upper(), safe="")
    return f"/v3/reference/tickers/{enc}"


def financials_path() -> str:
    """Legacy Polygon financials endpoint (still used by ingest handlers)."""
    return "/vX/reference/financials"


def financials_params(
    *,
    ticker: str,
    limit: int = 100,
    timeframe: str | None = None,
    include_sources: bool = False,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "ticker": str(ticker).strip().upper(),
        "limit": min(int(limit), 100),
        "include_sources": "true" if include_sources else "false",
    }
    if timeframe:
        params["timeframe"] = timeframe
    return params


def splits_path() -> str:
    """Current Stocks REST splits (replaces deprecated ``/v3/reference/splits``)."""
    return "/stocks/v1/splits"


def splits_params(
    *,
    ticker: str | None = None,
    limit: int = 1000,
    execution_date_gte: str | None = None,
    execution_date_lte: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": min(int(limit), 5000)}
    if ticker:
        params["ticker"] = str(ticker).strip().upper()
    if execution_date_gte:
        params["execution_date.gte"] = execution_date_gte
    if execution_date_lte:
        params["execution_date.lte"] = execution_date_lte
    return params


def dividends_path() -> str:
    """Current Stocks REST dividends (replaces deprecated ``/v3/reference/dividends``)."""
    return "/stocks/v1/dividends"


def dividends_params(
    *,
    ticker: str | None = None,
    limit: int = 1000,
    ex_dividend_date_gte: str | None = None,
    ex_dividend_date_lte: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": min(int(limit), 5000)}
    if ticker:
        params["ticker"] = str(ticker).strip().upper()
    if ex_dividend_date_gte:
        params["ex_dividend_date.gte"] = ex_dividend_date_gte
    if ex_dividend_date_lte:
        params["ex_dividend_date.lte"] = ex_dividend_date_lte
    return params


def market_status_upcoming_path() -> str:
    return "/v1/marketstatus/upcoming"


def grouped_daily_path(
    date_str: str,
    *,
    locale: str = "us",
    market: str = "stocks",
) -> str:
    """``/v2/aggs/grouped/locale/{locale}/market/{market}/{date}``."""
    return f"/v2/aggs/grouped/locale/{locale}/market/{market}/{date_str}"


def grouped_daily_params(*, adjusted: bool = True) -> dict[str, Any]:
    return {"adjusted": "true" if adjusted else "false"}


def treasury_yields_path() -> str:
    """``/fed/v1/treasury-yields`` — daily constant-maturity yields."""
    return "/fed/v1/treasury-yields"


def treasury_yields_params(
    *, date_gte: str | None = None, date_lte: str | None = None, limit: int = 1000
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": int(limit), "sort": "date.asc"}
    if date_gte:
        params["date.gte"] = date_gte
    if date_lte:
        params["date.lte"] = date_lte
    return params


def stock_snapshot_all_path() -> str:
    """``/v2/snapshot/locale/us/markets/stocks/tickers`` (full-market)."""
    return "/v2/snapshot/locale/us/markets/stocks/tickers"


def stock_snapshot_all_params(*, include_otc: bool = False) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if include_otc:
        params["include_otc"] = "true"
    return params


def stock_snapshot_single_path(ticker: str) -> str:
    """``/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}``."""
    enc = quote(str(ticker).strip().upper(), safe="")
    return f"/v2/snapshot/locale/us/markets/stocks/tickers/{enc}"


def stock_gainers_losers_path(direction: str) -> str:
    """``/v2/snapshot/locale/us/markets/stocks/{direction}`` (gainers|losers)."""
    d = str(direction or "").strip().lower()
    if d not in ("gainers", "losers"):
        raise ValueError(f"direction must be gainers|losers, got {direction!r}")
    return f"/v2/snapshot/locale/us/markets/stocks/{d}"


def stock_gainers_losers_params(*, include_otc: bool = False) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if include_otc:
        params["include_otc"] = "true"
    return params


def open_close_path(ticker: str, date_str: str) -> str:
    enc = quote(str(ticker).strip().upper(), safe=".")
    return f"/v1/open-close/{enc}/{date_str}"


def open_close_params(*, adjusted: bool = True) -> dict[str, Any]:
    return {"adjusted": "true" if adjusted else "false"}


def prev_agg_path(ticker: str) -> str:
    enc = quote(str(ticker).strip().upper(), safe=".")
    return f"/v2/aggs/ticker/{enc}/prev"


def prev_agg_params(*, adjusted: bool = True) -> dict[str, Any]:
    return {"adjusted": "true" if adjusted else "false"}


def news_path() -> str:
    return "/v2/reference/news"


def news_params(
    *,
    ticker: str | None = None,
    published_utc_gte: str | None = None,
    published_utc_lte: str | None = None,
    limit: int = 10,
    sort: str | None = None,
    order: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": min(int(limit), 1000)}
    if ticker:
        params["ticker"] = str(ticker).strip().upper()
    if published_utc_gte:
        params["published_utc.gte"] = published_utc_gte
    if published_utc_lte:
        params["published_utc.lte"] = published_utc_lte
    if sort:
        params["sort"] = sort
    if order:
        params["order"] = order
    return params


def related_companies_path(ticker: str) -> str:
    enc = quote(str(ticker).strip().upper(), safe=".")
    return f"/v1/related-companies/{enc}"


def reference_tickers_search_params(
    *,
    search: str | None = None,
    ticker: str | None = None,
    instrument_type: str | None = None,
    market: str | None = None,
    exchange: str | None = None,
    active: bool | None = None,
    date: str | None = None,
    limit: int = 100,
    sort: str = "ticker",
    order: str = "asc",
    cursor: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "limit": min(int(limit), 1000),
        "sort": sort,
        "order": order,
    }
    if search:
        params["search"] = search
    if ticker:
        params["ticker"] = str(ticker).strip().upper()
    if instrument_type:
        params["type"] = instrument_type
    if market:
        params["market"] = market
    if exchange:
        params["exchange"] = exchange
    if active is not None:
        params["active"] = "true" if active else "false"
    if date:
        params["date"] = date
    if cursor:
        params["cursor"] = cursor
    return params


def ticker_types_path() -> str:
    return "/v3/reference/tickers/types"


def ticker_types_params(
    *,
    asset_class: str | None = None,
    locale: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if asset_class:
        params["asset_class"] = asset_class
    if locale:
        params["locale"] = locale
    return params


def ticker_detail_params(*, date: str | None = None) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if date:
        params["date"] = date
    return params


def conditions_path() -> str:
    return "/v3/reference/conditions"


def conditions_params(
    *,
    asset_class: str | None = None,
    data_type: str | None = None,
    limit: int = 1000,
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": min(int(limit), 1000)}
    if asset_class:
        params["asset_class"] = asset_class
    if data_type:
        params["data_type"] = data_type
    return params


def exchanges_path() -> str:
    return "/v3/reference/exchanges"


def exchanges_params(
    *,
    asset_class: str | None = None,
    locale: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if asset_class:
        params["asset_class"] = asset_class
    if locale:
        params["locale"] = locale
    return params


def market_status_now_path() -> str:
    return "/v1/marketstatus/now"


def financial_statement_path(kind: str) -> str:
    allowed = {
        "income-statements": "/stocks/financials/v1/income-statements",
        "balance-sheets": "/stocks/financials/v1/balance-sheets",
        "cash-flow-statements": "/stocks/financials/v1/cash-flow-statements",
    }
    path = allowed.get(kind)
    if path is None:
        raise ValueError(f"unknown financial statement kind: {kind}")
    return path


def financial_statement_params(
    *,
    ticker: str,
    timeframe: str | None = None,
    fiscal_year: int | None = None,
    fiscal_quarter: int | None = None,
    period_end: str | None = None,
    filing_date: str | None = None,
    limit: int = 10,
    sort: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "ticker": str(ticker).strip().upper(),
        "limit": min(int(limit), 1000),
    }
    if timeframe:
        params["timeframe"] = timeframe
    if fiscal_year is not None:
        params["fiscal_year"] = int(fiscal_year)
    if fiscal_quarter is not None:
        params["fiscal_quarter"] = int(fiscal_quarter)
    if period_end:
        params["period_end"] = period_end
    if filing_date:
        params["filing_date"] = filing_date
    if sort:
        params["sort"] = sort
    return params


def ratios_path() -> str:
    return "/stocks/financials/v1/ratios"


def ratios_params(
    *,
    ticker: str | None = None,
    date: str | None = None,
    limit: int = 10,
    sort: str | None = None,
) -> dict[str, Any]:
    """Per-ticker history, or — with ``date`` and no ticker — the whole market for one day."""
    params: dict[str, Any] = {"limit": min(int(limit), 1000)}
    if ticker:
        params["ticker"] = str(ticker).strip().upper()
    if date:
        params["date"] = date
    if sort:
        params["sort"] = sort
    return params


def short_interest_path() -> str:
    return "/stocks/v1/short-interest"


def short_interest_params(
    *,
    ticker: str | None = None,
    settlement_date: str | None = None,
    settlement_date_gte: str | None = None,
    limit: int = 10,
    sort: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": min(int(limit), 1000)}
    if ticker:
        params["ticker"] = str(ticker).strip().upper()
    if settlement_date:
        params["settlement_date"] = settlement_date
    if settlement_date_gte:
        params["settlement_date.gte"] = settlement_date_gte
    if sort:
        params["sort"] = sort
    return params


def short_volume_path() -> str:
    return "/stocks/v1/short-volume"


def short_volume_params(
    *,
    ticker: str | None = None,
    date: str | None = None,
    limit: int = 10,
    sort: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": min(int(limit), 1000)}
    if ticker:
        params["ticker"] = str(ticker).strip().upper()
    if date:
        params["date"] = date
    if sort:
        params["sort"] = sort
    return params


def indicator_path(indicator: str, ticker: str) -> str:
    ind = str(indicator or "").strip().lower()
    enc = quote(str(ticker).strip().upper(), safe=".")
    return f"/v1/indicators/{ind}/{enc}"


def indicator_params(
    *,
    timespan: str = "day",
    window: int = 14,
    series_type: str = "close",
    adjusted: bool = True,
    order: str = "desc",
    limit: int = 50,
    short_window: int | None = None,
    long_window: int | None = None,
    signal_window: int | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "timespan": timespan,
        "window": int(window),
        "series_type": series_type,
        "adjusted": "true" if adjusted else "false",
        "order": order,
        "limit": min(int(limit), 5000),
    }
    if short_window is not None:
        params["short_window"] = int(short_window)
    if long_window is not None:
        params["long_window"] = int(long_window)
    if signal_window is not None:
        params["signal_window"] = int(signal_window)
    return params


def option_trades_path(options_ticker: str) -> str:
    enc = quote(str(options_ticker).strip().upper(), safe=":")
    return f"/v3/trades/{enc}"


def option_ticks_params(
    *,
    timestamp_gte: str | None = None,
    timestamp_lte: str | None = None,
    limit: int = 100,
    sort: str = "timestamp",
    order: str = "asc",
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "limit": min(int(limit), 50000),
        "sort": sort,
        "order": order,
    }
    if timestamp_gte:
        params["timestamp.gte"] = timestamp_gte
    if timestamp_lte:
        params["timestamp.lte"] = timestamp_lte
    return params
