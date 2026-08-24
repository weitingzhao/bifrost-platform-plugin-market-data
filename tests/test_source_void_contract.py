"""Contract fields shared with Trade FE SepaReadinessSummaryResponse."""

from __future__ import annotations

# Must stay in sync with bifrost-trade-frontend/src/types/stockDataReadiness.ts
SUMMARY_CONTRACT_FIELDS = frozenset(
    {
        "ok",
        "stock_day_vendor_fill_gap_count",
        "income_statements_gap_count",
        "balance_sheets_gap_count",
        "cash_flows_gap_count",
        "ratios_gap_count",
        "short_interest_gap_count",
        "short_volume_gap_count",
        "income_statements_source_void",
        "balance_sheets_source_void",
        "cash_flows_source_void",
        "ratios_source_void",
        "short_interest_source_void",
        "short_volume_source_void",
        "income_statements_acked_gap_count",
        "balance_sheets_acked_gap_count",
        "cash_flows_acked_gap_count",
        "ratios_acked_gap_count",
        "short_interest_acked_gap_count",
        "short_volume_acked_gap_count",
        "income_statements_actionable_gap_count",
        "balance_sheets_actionable_gap_count",
        "cash_flows_actionable_gap_count",
        "ratios_actionable_gap_count",
        "short_interest_actionable_gap_count",
        "short_volume_actionable_gap_count",
    }
)


def test_summary_builder_emits_contract_keys(monkeypatch) -> None:
    from bifrost_market_data.api import readiness_summary as mod

    class _Conn:
        def close(self) -> None:
            return None

    monkeypatch.setattr(mod, "_universe_count", lambda _c: 1)
    monkeypatch.setattr(
        mod, "_price_readiness", lambda _c: {"total_symbols": 1, "price_ready": 1}
    )
    monkeypatch.setattr(mod, "_fund_cache_counts", lambda _c: (1, None, True))
    monkeypatch.setattr(
        mod,
        "query_snapshot_coverage",
        lambda _c: {
            "row_count": 1,
            "last_fetched_at": None,
            "by_instrument_type": [],
        },
    )
    monkeypatch.setattr(mod, "_fundamentals_by_type_for_fe", lambda _c: None)
    monkeypatch.setattr(mod, "query_vendor_gap", lambda _c, detail=False: {"gap_count": 0})
    monkeypatch.setattr(
        mod, "query_gaps", lambda _c, report_type="", limit=0: {"count": 3}
    )
    monkeypatch.setattr(
        mod,
        "query_all_voids",
        lambda _c: {
            "income_statements": {
                "is_void": True,
                "acked_gap_count": 1,
                "note": None,
            }
        },
    )
    monkeypatch.setattr(mod, "_holidays_summary", lambda _c: None)

    out = mod.build_readiness_summary(_Conn())
    missing = SUMMARY_CONTRACT_FIELDS - set(out.keys())
    assert not missing, f"missing contract fields: {sorted(missing)}"
    assert out["income_statements_source_void"] is True
    assert out["income_statements_actionable_gap_count"] == 2
