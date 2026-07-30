"""URL builder unit tests."""

from __future__ import annotations

from bifrost_market_data.polygon import endpoints as ep


def test_aggs_range_path_encodes_ticker() -> None:
    path = ep.aggs_range_path("aapl", multiplier=1, timespan="day", from_value="2024-01-01", to_value="2024-01-31")
    assert path == "/v2/aggs/ticker/AAPL/range/1/day/2024-01-01/2024-01-31"


def test_aggs_range_params_defaults() -> None:
    params = ep.aggs_range_params()
    assert params["adjusted"] == "true"
    assert params["limit"] == 50_000


def test_aggs_range_params_omits_adjusted_for_options_and_indices() -> None:
    opt = ep.aggs_range_params(ticker="O:AAPL250620C00150000")
    assert "adjusted" not in opt
    idx = ep.aggs_range_params(ticker="I:SPX")
    assert "adjusted" not in idx
    stock = ep.aggs_range_params(ticker="AAPL")
    assert stock["adjusted"] == "true"
    forced = ep.aggs_range_params(ticker="O:AAPL250620C00150000", adjusted=False)
    assert forced["adjusted"] == "false"


def test_options_contracts_params() -> None:
    params = ep.options_contracts_params(underlying_ticker="aapl", expired=False)
    assert params["underlying_ticker"] == "AAPL"
    assert params["expired"] == "false"
    assert params["limit"] == 250


def test_options_snapshot_path() -> None:
    assert ep.options_snapshot_path("spy") == "/v3/snapshot/options/SPY"


def test_reference_tickers_and_details() -> None:
    assert ep.reference_tickers_path() == "/v3/reference/tickers"
    params = ep.reference_tickers_params(ticker_type="CS", active=True)
    assert params["type"] == "CS"
    assert params["active"] == "true"
    assert ep.ticker_details_path("msft") == "/v3/reference/tickers/MSFT"


def test_financials_splits_dividends_calendar() -> None:
    assert ep.financials_path() == "/vX/reference/financials"
    assert ep.splits_path() == "/stocks/v1/splits"
    assert ep.dividends_path() == "/stocks/v1/dividends"
    assert ep.market_status_upcoming_path() == "/v1/marketstatus/upcoming"
    assert ep.splits_params(ticker="ibm")["ticker"] == "IBM"
