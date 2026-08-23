"""Tests for index option dual-ticker mapping."""

from __future__ import annotations

from bifrost_market_data.ingest.index_options import (
    contracts_api_underlying,
    load_index_option_roots_from_cfg,
    snapshot_api_underlying,
    spot_api_symbol,
    storage_underlying,
)
from bifrost_market_data.polygon.endpoints import options_snapshot_path


def test_spx_storage_aliases() -> None:
    assert storage_underlying("SPX") == "SPX"
    assert storage_underlying("I:SPX") == "SPX"
    assert storage_underlying("SPXW") == "SPX"
    assert storage_underlying("SPY") == "SPY"


def test_spx_api_tickers() -> None:
    assert snapshot_api_underlying("SPX") == "I:SPX"
    assert snapshot_api_underlying("I:SPX") == "I:SPX"
    assert contracts_api_underlying("SPX") == "SPX"
    assert contracts_api_underlying("I:SPX") == "SPX"
    assert spot_api_symbol("SPX") == "I:SPX"
    assert spot_api_symbol("SPY") is None


def test_index_roots_from_benchmarks() -> None:
    roots = load_index_option_roots_from_cfg(
        {"iv_radar_benchmarks": ["SPY", "QQQ", "IWM", "SPX"]}
    )
    assert roots == ["SPX"]
    assert load_index_option_roots_from_cfg({"iv_radar_benchmarks": ["SPY"]}) == []


def test_options_snapshot_path_keeps_colon() -> None:
    assert options_snapshot_path("I:SPX") == "/v3/snapshot/options/I:SPX"
