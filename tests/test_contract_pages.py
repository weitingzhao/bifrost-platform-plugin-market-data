"""TD-90: contract-catalogue walks fail on truncation and keep page headroom."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

import pytest

from bifrost_market_data import doctor as doc
from bifrost_market_data.ingest import _RAW_HANDLERS
from bifrost_market_data.ingest.contract_pages import (
    CATALOGUE_KINDS,
    EQUITY_PAGE_CAP,
    INDEX_PAGE_CAP,
    PAGE_CAP_WARN_RATIO,
    PAGE_LIMIT,
    CatalogueTruncatedError,
    contract_page_cap,
)
from bifrost_market_data.polygon import endpoints as ep
from ingest_testutil import FakeConn, make_job, mock_client

#: SPX live contracts, measured 2026-10-05 (118 pages at 250 a page).
SPX_LIVE_CONTRACTS_MEASURED = 29_282

_EXPIRY = date.today() + timedelta(days=10)
_PAYLOADS: dict[str, dict[str, Any]] = {
    "option_contract": {"underlying": "SPX"},
    "option_expiration": {"underlying": "SPX"},
    "option_backfill_plan": {
        "underlying": "SPY",
        "expiry_gte": _EXPIRY.replace(day=1).isoformat(),
        "expiry_lte": _EXPIRY.isoformat(),
    },
}


def _truncated_page() -> dict[str, Any]:
    contract = {
        "ticker": "O:SPX261016C05000000",
        "expiration_date": _EXPIRY.isoformat(),
        "strike_price": 5000,
        "contract_type": "call",
    }
    return {"results": [contract], "pages": 120, "truncated": True, "next_cursor": "abc"}


def test_every_catalogue_kind_is_a_registered_handler() -> None:
    assert set(CATALOGUE_KINDS) <= set(_RAW_HANDLERS)
    assert set(_PAYLOADS) == set(CATALOGUE_KINDS)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", CATALOGUE_KINDS)
async def test_truncated_catalogue_walk_raises_and_writes_nothing(kind: str) -> None:
    """Ratchet: a handler result with truncated=True must fail the job, not report done."""
    client = mock_client(fetch_options_contracts=_truncated_page())
    conn = FakeConn()
    with pytest.raises(CatalogueTruncatedError, match="page cap"):
        await _RAW_HANDLERS[kind](make_job(kind, _PAYLOADS[kind]), client, conn)
    writes = [q for q, _ in conn.statements if any(w in q.upper() for w in ("INSERT", "UPDATE", "DELETE"))]
    assert writes == []
    assert conn.committed == 0


def test_contracts_request_uses_the_page_size_the_caps_assume() -> None:
    """Ratchet: the request's page size is the figure the cap arithmetic uses."""
    assert PAGE_LIMIT == ep.OPTIONS_CONTRACTS_PAGE_LIMIT == 1000
    assert ep.options_contracts_params(underlying_ticker="SPX")["limit"] == PAGE_LIMIT


def test_spx_uses_well_under_half_its_page_cap() -> None:
    assert contract_page_cap("SPX") == INDEX_PAGE_CAP
    assert contract_page_cap("AAPL") == EQUITY_PAGE_CAP
    pages = -(-SPX_LIVE_CONTRACTS_MEASURED // PAGE_LIMIT)
    assert pages / INDEX_PAGE_CAP < 0.5


@pytest.mark.asyncio
async def test_option_contract_sends_the_cap_and_records_it() -> None:
    client = mock_client(fetch_options_contracts={"results": [], "pages": 30, "truncated": False})
    result = await _RAW_HANDLERS["option_contract"](make_job("option_contract", {"underlying": "SPX"}), client, FakeConn())
    assert client.fetch_options_contracts.await_args.kwargs["max_pages"] == INDEX_PAGE_CAP
    assert result["max_pages"] == INDEX_PAGE_CAP
    assert result["page_limit"] == PAGE_LIMIT
    assert result["truncated"] is False


class _JobsCur:
    def __init__(self, rows: list[tuple[Any, ...]], fail: bool) -> None:
        self.rows = rows
        self.fail = fail

    def execute(self, query: str, params: Any = None) -> None:
        if self.fail:
            raise RuntimeError("statement timeout")
        assert "/* doctor: page-cap */" in query
        assert list(params[0]) == list(CATALOGUE_KINDS)

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self.rows)

    def __enter__(self) -> "_JobsCur":
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _JobsConn:
    def __init__(self, rows: list[tuple[Any, ...]], fail: bool = False) -> None:
        self.rows = rows
        self.fail = fail

    def cursor(self) -> _JobsCur:
        return _JobsCur(self.rows, self.fail)

    def rollback(self) -> None:
        return None


NOW = datetime(2026, 10, 6, 12, 0, tzinfo=timezone.utc)


def test_doctor_warns_when_an_underlying_passes_the_page_cap_ratio() -> None:
    """Ratchet: > 80% of the cap is amber before it becomes a failed job."""
    over = int(INDEX_PAGE_CAP * PAGE_CAP_WARN_RATIO) + 1
    rows = [
        ("option_contract", "SPX", over, INDEX_PAGE_CAP),
        ("option_contract", "SPY", 14, EQUITY_PAGE_CAP),
        ("option_backfill_plan", "SPY", 3, 200),
    ]
    found = {f.id: f for f in doc._page_cap_findings(_JobsConn(rows), NOW)}
    contract = found["page_cap:option_contract"]
    assert contract.severity == "warn"
    assert contract.missing_sample == ["SPX"]
    assert f"SPX {over}/{INDEX_PAGE_CAP}" in contract.detail
    assert found["page_cap:option_backfill_plan"].severity == "ok"


def test_doctor_page_cap_is_ok_with_headroom_and_silent_on_failure() -> None:
    rows = [("option_contract", "SPX", 30, INDEX_PAGE_CAP)]
    (only,) = doc._page_cap_findings(_JobsConn(rows), NOW)
    assert only.severity == "ok"
    assert only.actual.startswith("25%")
    assert doc._page_cap_findings(_JobsConn(rows, fail=True), NOW) == []
