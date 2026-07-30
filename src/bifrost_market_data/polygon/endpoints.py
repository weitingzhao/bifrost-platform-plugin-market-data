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
    enc = quote(str(underlying).strip().upper(), safe="")
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
