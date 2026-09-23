"""SEC filings ingest (0.37.0): 8-K text, the vendor's classification, 10-K sections.

Fixtures are invented — tickers are real names, every accession number, date
and sentence is made up.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from bifrost_market_data.ingest import sec_filings
from bifrost_market_data.ingest.sec_filings import (
    TEN_K_TICKERS_PER_REQUEST,
    VOID_10K,
    VOID_8K,
    filings_since,
    handle_sec_filings_market,
    handle_sec_filings_symbol,
    parse_8k_items,
)
from ingest_testutil import FakeConn, make_job, mock_client


# ── parse_8k_items ────────────────────────────────────────────────────────


def test_items_are_the_sec_numbers_named_in_the_text() -> None:
    text = (
        "Item 2.02 Results of Operations and Financial Condition. On a date the "
        "company issued a press release. Item 9.01 Financial Statements and Exhibits."
    )
    assert parse_8k_items(text) == ["2.02", "9.01"]


def test_items_normalise_case_punctuation_and_repeats() -> None:
    assert parse_8k_items("ITEM 5.02. Departure of Directors … see Item 5.02 above; item 1.01") == [
        "1.01",
        "5.02",
    ]


def test_a_10k_part_number_is_not_an_8k_item() -> None:
    # "Item 2" / "Item 1A" are annual-report parts. Matching them would read a
    # stray reference as a Results-of-Operations filing.
    assert parse_8k_items("As described in Item 1A and Item 2 of the annual report") == []


def test_items_sort_numerically_not_as_text() -> None:
    assert parse_8k_items("Item 9.01 then Item 10.01 then Item 2.02") == ["2.02", "9.01", "10.01"]


def test_no_text_is_no_items() -> None:
    assert parse_8k_items(None) == []
    assert parse_8k_items("") == []


# ── filings_since ─────────────────────────────────────────────────────────


def test_backfill_start_is_pinned_to_the_first_of_the_month() -> None:
    # One payload per name per month, so a catch-up still queued from yesterday
    # is deduplicated rather than run twice.
    assert filings_since(date(2026, 9, 23), 730) == "2024-09-01"
    assert filings_since(date(2026, 9, 1), 730) == "2024-09-01"
    assert filings_since(date(2026, 9, 30), 730) == "2024-09-01"


# ── the daily whole-market job ───────────────────────────────────────────

TEXT = {
    "results": [
        {
            "accession_number": "0000000001-26-000001",
            "ticker": "nvda",
            "cik": "0000000001",
            "form_type": "8-K",
            "filing_date": "2026-08-26",
            "filing_url": "https://example.invalid/1.txt",
            "items_text": "Item 2.02 Results of Operations. Item 9.01 Exhibits.",
        },
        {
            # Not in the universe: seen, not kept.
            "accession_number": "0000000002-26-000002",
            "ticker": "ZZZZ",
            "filing_date": "2026-08-26",
            "items_text": "Item 8.01 Other Events.",
        },
        # No accession number: dropped rather than keyed on nothing.
        {"ticker": "NVDA", "filing_date": "2026-08-26", "items_text": "Item 7.01"},
    ],
    "pages": 3,
    "truncated": False,
}
DISC = {
    "results": [
        {
            "accession_number": "0000000001-26-000001",
            # A filing naming two tickers, only one of which the universe holds.
            "tickers": ["NVDA", "ZZZZ"],
            "filing_date": "2026-08-26",
            "primary_category": "financial_reporting",
            "secondary_category": "earnings",
            "tertiary_category": "quarterly_earnings",
            "supporting_text": "An invented sentence about results.",
        }
    ],
    "pages": 1,
    "truncated": False,
}
SECTIONS = {
    "results": [
        {
            "ticker": "PLTR",
            "section": "risk_factors",
            "period_end": "2025-12-31",
            "filing_date": "2026-02-17",
            "text": "Invented risk factor text.",
        }
    ],
    "pages": 1,
    "truncated": False,
}


def _patch_universe(monkeypatch: pytest.MonkeyPatch, names: set[str]) -> None:
    monkeypatch.setattr(sec_filings, "universe_symbols", lambda conn: set(names))


def _inserted(conn: FakeConn, table: str) -> list[Any]:
    for sql, params in conn.statements:
        if f"INSERT INTO raw_market.{table}" in sql:
            return params
    return []


@pytest.mark.asyncio
async def test_market_job_keeps_the_universe_and_parses_items(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_universe(monkeypatch, {"NVDA", "PLTR"})
    client = mock_client(
        fetch_sec_8k_text=TEXT, fetch_sec_8k_disclosures=DISC, fetch_sec_10k_sections=SECTIONS
    )
    conn = FakeConn()
    job = make_job("sec_filings_market", {"filing_date_gte": "2026-08-22", "filing_date_lte": "2026-08-26"})
    r = await handle_sec_filings_market(job, client, conn)

    assert r["written"] == {"sec_8k_filing": 1, "sec_8k_disclosure": 1, "sec_10k_section": 1}
    assert r["rows_written"] == 3
    assert r["seen"]["sec_8k_filing"] == 3
    filing = _inserted(conn, "sec_8k_filing")[0]
    assert filing[1] == "NVDA" and filing[6] == ["2.02", "9.01"]
    # The ZZZZ half of the shared disclosure is not this universe's to write.
    assert [row[1] for row in _inserted(conn, "sec_8k_disclosure")] == ["NVDA"]
    assert conn.committed == 1


@pytest.mark.asyncio
async def test_disclosures_replace_only_the_pairs_returned(monkeypatch: pytest.MonkeyPatch) -> None:
    # No natural key, so a reclassified filing must not keep its old category
    # beside the new one — and a filing this pull did not return is not erased.
    _patch_universe(monkeypatch, {"NVDA"})
    client = mock_client(
        fetch_sec_8k_text={"results": []},
        fetch_sec_8k_disclosures=DISC,
        fetch_sec_10k_sections={"results": []},
    )
    conn = FakeConn()
    await handle_sec_filings_market(make_job("sec_filings_market", {"filing_date_gte": "2026-08-22"}), client, conn)
    deletes = [(sql, p) for sql, p in conn.statements if "DELETE FROM raw_market.sec_8k_disclosure" in sql]
    assert len(deletes) == 1
    assert deletes[0][1] == (["0000000001-26-000001"], ["NVDA"])


@pytest.mark.asyncio
async def test_sections_are_asked_for_by_name_in_slices(monkeypatch: pytest.MonkeyPatch) -> None:
    # A whole-market week of annual reports does not fit in a worker; the
    # universe is sent in slices of TEN_K_TICKERS_PER_REQUEST.
    names = {f"S{i:03d}" for i in range(TEN_K_TICKERS_PER_REQUEST + 7)}
    _patch_universe(monkeypatch, names)
    client = mock_client(
        fetch_sec_8k_text={"results": []},
        fetch_sec_8k_disclosures={"results": []},
        fetch_sec_10k_sections={"results": []},
    )
    await handle_sec_filings_market(make_job("sec_filings_market", {"filing_date_gte": "2026-08-22"}), client, FakeConn())
    calls = client.fetch_sec_10k_sections.await_args_list
    assert [len(c.kwargs["tickers"]) for c in calls] == [TEN_K_TICKERS_PER_REQUEST, 7]
    assert set().union(*(set(c.kwargs["tickers"]) for c in calls)) == names


@pytest.mark.asyncio
async def test_an_empty_universe_is_a_failure_not_a_quiet_day(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_universe(monkeypatch, set())
    client = mock_client(fetch_sec_8k_text=TEXT, fetch_sec_8k_disclosures=DISC, fetch_sec_10k_sections=SECTIONS)
    with pytest.raises(RuntimeError, match="option_universe is empty"):
        await handle_sec_filings_market(make_job("sec_filings_market", {"filing_date_gte": "2026-08-22"}), client, FakeConn())


@pytest.mark.asyncio
async def test_a_truncated_pull_fails_the_job(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_universe(monkeypatch, {"NVDA"})
    client = mock_client(
        fetch_sec_8k_text={**TEXT, "truncated": True, "pages": 80},
        fetch_sec_8k_disclosures=DISC,
        fetch_sec_10k_sections=SECTIONS,
    )
    with pytest.raises(RuntimeError, match="page cap"):
        await handle_sec_filings_market(make_job("sec_filings_market", {"filing_date_gte": "2026-08-22"}), client, FakeConn())


@pytest.mark.asyncio
async def test_market_job_needs_a_window() -> None:
    with pytest.raises(ValueError, match="filing_date_gte"):
        await handle_sec_filings_market(make_job("sec_filings_market", {}), mock_client(), FakeConn())


# ── the per-symbol backfill job ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_symbol_job_counts_prints_and_voids_per_collection() -> None:
    client = mock_client(
        fetch_sec_8k_text=TEXT, fetch_sec_8k_disclosures=DISC, fetch_sec_10k_sections={"results": []}
    )
    conn = FakeConn()
    r = await handle_sec_filings_symbol(
        make_job("sec_filings_symbol", {"symbol": "nvda", "since": "2024-09-01"}), client, conn
    )
    assert r["symbol"] == "NVDA"
    assert r["earnings_prints"] == 1
    assert r["written"]["sec_10k_section"] == 0
    assert client.fetch_sec_10k_sections.await_args.kwargs["tickers"] == ("NVDA",)
    voids = [(sql, p) for sql, p in conn.statements if "symbol_source_void" in sql]
    # 8-K rows arrived → that void is cleared; no sections → a 10-K void is noted.
    assert any("DELETE" in sql and p == ("NVDA", VOID_8K) for sql, p in voids)
    assert any("INSERT" in sql and p[0] == "NVDA" and p[1] == VOID_10K for sql, p in voids)


@pytest.mark.asyncio
async def test_symbol_job_writes_only_its_own_name() -> None:
    client = mock_client(
        fetch_sec_8k_text=TEXT, fetch_sec_8k_disclosures=DISC, fetch_sec_10k_sections=SECTIONS
    )
    conn = FakeConn()
    await handle_sec_filings_symbol(
        make_job("sec_filings_symbol", {"symbol": "NVDA", "since": "2024-09-01"}), client, conn
    )
    assert {row[1] for row in _inserted(conn, "sec_8k_filing")} == {"NVDA"}
    assert {row[1] for row in _inserted(conn, "sec_8k_disclosure")} == {"NVDA"}
    # PLTR's section came back on an NVDA request only in this fake; it is dropped.
    assert _inserted(conn, "sec_10k_section") == []


@pytest.mark.asyncio
async def test_symbol_job_needs_symbol_and_since() -> None:
    with pytest.raises(ValueError, match="symbol and since"):
        await handle_sec_filings_symbol(make_job("sec_filings_symbol", {"symbol": "NVDA"}), mock_client(), FakeConn())


# ── registration ─────────────────────────────────────────────────────────


def test_both_kinds_run_on_the_stocks_pool_and_share_a_freshness_dimension() -> None:
    from bifrost_market_data.freshness import dimension_for_kind
    from bifrost_market_data.ingest import raw_handler_kinds
    from bifrost_market_data.worker.loop import kinds_for_pool

    for kind in ("sec_filings_market", "sec_filings_symbol"):
        assert kind in raw_handler_kinds()
        assert kind in kinds_for_pool("stocks")
        assert dimension_for_kind(kind) == "sec_filings"


def test_the_three_tables_have_contracts_on_slots_that_exist() -> None:
    from bifrost_market_data.contracts import contract_for
    from bifrost_market_data.scheduler.daily import SLOT_NAMES

    for table in ("raw_market.sec_8k_filing", "raw_market.sec_8k_disclosure", "raw_market.sec_10k_section"):
        c = contract_for(table)
        assert c.tier == "universe" and c.cadence == "filing" and c.grain == "filing"
        assert c.freshness_dimension == "sec_filings"
        assert set(c.slots) <= set(SLOT_NAMES)
    # The classification covers a minority of 8-Ks; its breadth is not a gap.
    assert contract_for("raw_market.sec_8k_disclosure").breadth_unjudged
    assert contract_for("raw_market.sec_8k_filing").void_data_type == VOID_8K
    assert contract_for("raw_market.sec_10k_section").void_data_type == VOID_10K


def test_filings_are_entitled_not_404() -> None:
    # 0.10.3 recorded the family as "unavailable, 404" after probing v1; the
    # vendor serves vX. Float is the part that really is gone.
    from bifrost_market_data.subscription import CAPABILITIES

    by_id = {c["id"]: c for c in CAPABILITIES}
    assert by_id["sec_filings"]["status"] == "entitled"
    assert "filings_float" not in by_id
    assert by_id["float"]["status"] == "unavailable"
    assert by_id["benzinga"]["status"] == "planned"


def test_the_endpoints_are_the_vx_paths() -> None:
    from bifrost_market_data.polygon import endpoints as ep

    for path in (ep.sec_8k_text_path(), ep.sec_8k_disclosures_path(), ep.sec_10k_sections_path()):
        assert "/vX/" in path and "/v1/" not in path
    # The sections endpoint answers 400 to a filing_date sort; none is sent.
    assert "sort" not in ep.sec_filing_params(ticker="NVDA", sections=("mda",))
    p = ep.sec_filing_params(ticker="NVDA", ticker_param="tickers")
    assert p["tickers"] == "NVDA" and "ticker" not in p
    assert ep.sec_filing_params(tickers_any_of=("nvda", "pltr"))["ticker.any_of"] == "NVDA,PLTR"
