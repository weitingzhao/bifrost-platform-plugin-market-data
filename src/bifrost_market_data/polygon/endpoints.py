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
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": min(int(limit), 250)}
    if strike_price is not None:
        params["strike_price"] = strike_price
    if expiration_date:
        params["expiration_date"] = expiration_date
    if contract_type:
        params["contract_type"] = contract_type
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


def splits_params(*, ticker: str | None = None, limit: int = 1000) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": min(int(limit), 5000)}
    if ticker:
        params["ticker"] = str(ticker).strip().upper()
    return params


def dividends_path() -> str:
    """Current Stocks REST dividends (replaces deprecated ``/v3/reference/dividends``)."""
    return "/stocks/v1/dividends"


def dividends_params(*, ticker: str | None = None, limit: int = 1000) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": min(int(limit), 5000)}
    if ticker:
        params["ticker"] = str(ticker).strip().upper()
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


def ratios_params(*, ticker: str, limit: int = 10, sort: str | None = None) -> dict[str, Any]:
    params: dict[str, Any] = {
        "ticker": str(ticker).strip().upper(),
        "limit": min(int(limit), 1000),
    }
    if sort:
        params["sort"] = sort
    return params


def short_interest_path() -> str:
    return "/stocks/v1/short-interest"


def short_interest_params(
    *,
    ticker: str,
    settlement_date: str | None = None,
    limit: int = 10,
    sort: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "ticker": str(ticker).strip().upper(),
        "limit": min(int(limit), 1000),
    }
    if settlement_date:
        params["settlement_date"] = settlement_date
    if sort:
        params["sort"] = sort
    return params


def short_volume_path() -> str:
    return "/stocks/v1/short-volume"


def short_volume_params(
    *,
    ticker: str,
    date: str | None = None,
    limit: int = 10,
    sort: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "ticker": str(ticker).strip().upper(),
        "limit": min(int(limit), 1000),
    }
    if date:
        params["date"] = date
    if sort:
        params["sort"] = sort
    return params


def float_path() -> str:
    return "/stocks/v1/float"


def float_params(*, ticker: str, limit: int = 10, sort: str | None = None) -> dict[str, Any]:
    params: dict[str, Any] = {
        "ticker": str(ticker).strip().upper(),
        "limit": min(int(limit), 5000),
    }
    if sort:
        params["sort"] = sort
    return params


def _filing_date_filter_params(
    params: dict[str, Any],
    *,
    filing_date: str | None = None,
    filing_date_gt: str | None = None,
    filing_date_gte: str | None = None,
    filing_date_lt: str | None = None,
    filing_date_lte: str | None = None,
) -> None:
    if filing_date:
        params["filing_date"] = filing_date
    if filing_date_gt:
        params["filing_date.gt"] = filing_date_gt
    if filing_date_gte:
        params["filing_date.gte"] = filing_date_gte
    if filing_date_lt:
        params["filing_date.lt"] = filing_date_lt
    if filing_date_lte:
        params["filing_date.lte"] = filing_date_lte


def edgar_index_path() -> str:
    return "/stocks/filings/v1/index"


def edgar_index_params(**kwargs: Any) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": min(int(kwargs.get("limit", 100)), 50000)}
    if kwargs.get("ticker"):
        params["ticker"] = str(kwargs["ticker"]).strip().upper()
    if kwargs.get("cik"):
        params["cik"] = kwargs["cik"]
    if kwargs.get("form_type"):
        params["form_type"] = kwargs["form_type"]
    _filing_date_filter_params(
        params,
        filing_date=kwargs.get("filing_date"),
        filing_date_gt=kwargs.get("filing_date_gt"),
        filing_date_gte=kwargs.get("filing_date_gte"),
        filing_date_lt=kwargs.get("filing_date_lt"),
        filing_date_lte=kwargs.get("filing_date_lte"),
    )
    if kwargs.get("sort"):
        params["sort"] = kwargs["sort"]
    return params


def filing_10k_sections_path() -> str:
    return "/stocks/filings/10-K/v1/sections"


def filing_10k_sections_params(**kwargs: Any) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": min(int(kwargs.get("limit", 10)), 99)}
    if kwargs.get("ticker"):
        params["ticker"] = str(kwargs["ticker"]).strip().upper()
    if kwargs.get("cik"):
        params["cik"] = kwargs["cik"]
    if kwargs.get("section"):
        params["section"] = kwargs["section"]
    _filing_date_filter_params(
        params,
        filing_date=kwargs.get("filing_date"),
        filing_date_gt=kwargs.get("filing_date_gt"),
        filing_date_gte=kwargs.get("filing_date_gte"),
        filing_date_lt=kwargs.get("filing_date_lt"),
        filing_date_lte=kwargs.get("filing_date_lte"),
    )
    if kwargs.get("period_end"):
        params["period_end"] = kwargs["period_end"]
    if kwargs.get("period_end_gte"):
        params["period_end.gte"] = kwargs["period_end_gte"]
    if kwargs.get("period_end_lte"):
        params["period_end.lte"] = kwargs["period_end_lte"]
    if kwargs.get("sort"):
        params["sort"] = kwargs["sort"]
    return params


def filing_8k_text_path() -> str:
    return "/stocks/filings/8-K/v1/text"


def filing_8k_text_params(**kwargs: Any) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": min(int(kwargs.get("limit", 10)), 99)}
    if kwargs.get("ticker"):
        params["ticker"] = str(kwargs["ticker"]).strip().upper()
    if kwargs.get("cik"):
        params["cik"] = kwargs["cik"]
    if kwargs.get("form_type"):
        params["form_type"] = kwargs["form_type"]
    _filing_date_filter_params(
        params,
        filing_date=kwargs.get("filing_date"),
        filing_date_gt=kwargs.get("filing_date_gt"),
        filing_date_gte=kwargs.get("filing_date_gte"),
        filing_date_lt=kwargs.get("filing_date_lt"),
        filing_date_lte=kwargs.get("filing_date_lte"),
    )
    if kwargs.get("sort"):
        params["sort"] = kwargs["sort"]
    return params


def filing_13f_path() -> str:
    return "/stocks/filings/v1/13-F"


def filing_13f_params(**kwargs: Any) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": min(int(kwargs.get("limit", 100)), 1000)}
    if kwargs.get("filer_cik"):
        params["filer_cik"] = kwargs["filer_cik"]
    _filing_date_filter_params(
        params,
        filing_date=kwargs.get("filing_date"),
        filing_date_gt=kwargs.get("filing_date_gt"),
        filing_date_gte=kwargs.get("filing_date_gte"),
        filing_date_lt=kwargs.get("filing_date_lt"),
        filing_date_lte=kwargs.get("filing_date_lte"),
    )
    if kwargs.get("sort"):
        params["sort"] = kwargs["sort"]
    return params


def filing_risk_factors_path() -> str:
    return "/stocks/filings/v1/risk-factors"


def filing_risk_factors_params(**kwargs: Any) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": min(int(kwargs.get("limit", 100)), 49999)}
    if kwargs.get("ticker"):
        params["ticker"] = str(kwargs["ticker"]).strip().upper()
    if kwargs.get("cik"):
        params["cik"] = kwargs["cik"]
    _filing_date_filter_params(
        params,
        filing_date=kwargs.get("filing_date"),
        filing_date_gt=kwargs.get("filing_date_gt"),
        filing_date_gte=kwargs.get("filing_date_gte"),
        filing_date_lt=kwargs.get("filing_date_lt"),
        filing_date_lte=kwargs.get("filing_date_lte"),
    )
    if kwargs.get("sort"):
        params["sort"] = kwargs["sort"]
    return params


def filing_risk_categories_path() -> str:
    return "/stocks/taxonomies/v1/risk-factors"


def filing_risk_categories_params(**kwargs: Any) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": min(int(kwargs.get("limit", 200)), 999)}
    if kwargs.get("taxonomy") is not None:
        params["taxonomy"] = int(kwargs["taxonomy"])
    if kwargs.get("primary_category"):
        params["primary_category"] = kwargs["primary_category"]
    if kwargs.get("secondary_category"):
        params["secondary_category"] = kwargs["secondary_category"]
    if kwargs.get("tertiary_category"):
        params["tertiary_category"] = kwargs["tertiary_category"]
    if kwargs.get("sort"):
        params["sort"] = kwargs["sort"]
    return params


def filing_form_3_path() -> str:
    return "/stocks/filings/v1/form-3"


def filing_form_4_path() -> str:
    return "/stocks/filings/v1/form-4"


def insider_filing_params(**kwargs: Any) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": min(int(kwargs.get("limit", 100)), 10000)}
    if kwargs.get("issuer_cik"):
        params["issuer_cik"] = kwargs["issuer_cik"]
    if kwargs.get("owner_cik"):
        params["owner_cik"] = kwargs["owner_cik"]
    if kwargs.get("tickers"):
        params["tickers"] = kwargs["tickers"]
    if kwargs.get("form_type"):
        params["form_type"] = kwargs["form_type"]
    if kwargs.get("transaction_code"):
        params["transaction_code"] = kwargs["transaction_code"]
    _filing_date_filter_params(
        params,
        filing_date=kwargs.get("filing_date"),
        filing_date_gt=kwargs.get("filing_date_gt"),
        filing_date_gte=kwargs.get("filing_date_gte"),
        filing_date_lt=kwargs.get("filing_date_lt"),
        filing_date_lte=kwargs.get("filing_date_lte"),
    )
    if kwargs.get("sort"):
        params["sort"] = kwargs["sort"]
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


def last_trade_path(options_ticker: str) -> str:
    enc = quote(str(options_ticker).strip().upper(), safe=":")
    return f"/v2/last/trade/{enc}"


def option_quotes_path(options_ticker: str) -> str:
    enc = quote(str(options_ticker).strip().upper(), safe=":")
    return f"/v3/quotes/{enc}"


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
