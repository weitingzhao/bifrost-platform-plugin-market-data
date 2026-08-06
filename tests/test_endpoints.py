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


def test_grouped_daily_path_and_params() -> None:
    assert (
        ep.grouped_daily_path("2024-06-20")
        == "/v2/aggs/grouped/locale/us/market/stocks/2024-06-20"
    )
    assert ep.grouped_daily_params()["adjusted"] == "true"
    assert ep.grouped_daily_params(adjusted=False)["adjusted"] == "false"


def test_stock_snapshot_paths_and_params() -> None:
    assert ep.stock_snapshot_all_path() == "/v2/snapshot/locale/us/markets/stocks/tickers"
    assert ep.stock_snapshot_all_params() == {}
    assert ep.stock_snapshot_all_params(include_otc=True)["include_otc"] == "true"
    assert (
        ep.stock_snapshot_single_path("aapl")
        == "/v2/snapshot/locale/us/markets/stocks/tickers/AAPL"
    )
    assert ep.stock_gainers_losers_path("gainers") == (
        "/v2/snapshot/locale/us/markets/stocks/gainers"
    )
    assert ep.stock_gainers_losers_path("LOSERS") == (
        "/v2/snapshot/locale/us/markets/stocks/losers"
    )
    try:
        ep.stock_gainers_losers_path("both")
        raise AssertionError("expected ValueError")
    except ValueError:
        pass
