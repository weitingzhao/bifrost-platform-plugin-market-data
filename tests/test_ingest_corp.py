"""Tests for splits / dividends corporate_action handlers."""

from __future__ import annotations

from datetime import date

import pytest

from bifrost_market_data.ingest.corporate_action import (
    handle_dividends,
    handle_dividends_market,
    handle_splits,
)
from bifrost_market_data.schema.corporate_action_identity import IDENTITY
from ingest_testutil import FakeConn, make_job, mock_client


def _deletes(conn: FakeConn) -> list[tuple[str, object]]:
    return [(q, p) for q, p in conn.statements if "DELETE FROM raw_market.corporate_action" in q]


@pytest.mark.asyncio
async def test_splits_upsert() -> None:
    client = mock_client(
        fetch_splits={
            "results": [
                {
                    "execution_date": "2020-08-31",
                    "split_from": 1,
                    "split_to": 4,
                    "adjustment_type": "split",
                }
            ],
            "pages": 1,
        }
    )
    conn = FakeConn()
    result = await handle_splits(make_job("splits", {"symbol": "AAPL"}), client, conn)
    assert result["rows_written"] == 1
    assert result["action_type"] == "split"
    row = conn.statements[0][1][0]
    assert row[0] == "AAPL"
    assert row[1] == "split"
    assert row[5] == 1.0
    assert row[6] == 4.0
    # Every new identity part is NULL for a split, so its key is what it was.
    assert row[10] is None and row[11] is None
    assert not _deletes(conn)


@pytest.mark.asyncio
async def test_dividends_upsert() -> None:
    client = mock_client(
        fetch_dividends={
            "results": [
                {
                    "ex_dividend_date": "2024-05-10",
                    "record_date": "2024-05-13",
                    "pay_date": "2024-05-16",
                    "cash_amount": 0.25,
                    "currency": "USD",
                    "distribution_type": "Recurring",
                    "frequency": 4,
                }
            ],
            "pages": 1,
        }
    )
    conn = FakeConn()
    result = await handle_dividends(make_job("dividends", {"symbol": "AAPL"}), client, conn)
    assert result["rows_written"] == 1
    assert result["action_type"] == "dividend"
    row = conn.statements[0][1][0]
    assert row[1] == "dividend"
    assert row[7] == 0.25
    assert row[8] == "USD"
    assert row[10] == "recurring"
    assert row[11] == 4


@pytest.mark.asyncio
async def test_a_special_and_a_regular_on_one_ex_date_are_two_rows() -> None:
    """MSFT 2004-11-15, as the vendor sends it: the old key kept the $0.08 and lost the $3.00."""
    client = mock_client(
        fetch_dividends={
            "results": [
                {"ex_dividend_date": "2004-11-15", "cash_amount": 3.0, "currency": "USD",
                 "distribution_type": "special", "frequency": 0},
                {"ex_dividend_date": "2004-11-15", "cash_amount": 0.08, "currency": "USD",
                 "distribution_type": "recurring", "frequency": 4},
            ],
            "pages": 1,
        }
    )
    conn = FakeConn()
    result = await handle_dividends(make_job("dividends", {"symbol": "MSFT"}), client, conn)
    sql, rows = conn.statements[0]
    assert f"ON CONFLICT ({', '.join(IDENTITY)})" in sql
    assert result["rows_written"] == 2
    assert {(r[7], r[10], r[11]) for r in rows} == {(3.0, "special", 0), (0.08, "recurring", 4)}
    # A frequency of 0 is a value, not a missing one — it is part of the key.
    assert any(r[11] == 0 for r in rows)


@pytest.mark.asyncio
async def test_a_complete_fetch_removes_what_it_no_longer_lists_in_one_transaction() -> None:
    client = mock_client(
        fetch_dividends={
            "results": [{"ex_dividend_date": "2024-05-10", "cash_amount": 0.25, "currency": "USD",
                         "distribution_type": "recurring", "frequency": 4}],
            "pages": 1,
        }
    )
    conn = FakeConn()
    await handle_dividends(make_job("dividends", {"symbol": "AAPL"}), client, conn)
    (sql, params), = _deletes(conn)
    assert params == ("AAPL",)
    assert "action_type = 'dividend'" in sql
    assert "fetched_at < now()" in sql
    # The upsert and the delete commit together, or a crash between them could
    # leave the old untyped row beside its typed replacement.
    assert conn.statements.index((sql, params)) > 0
    assert conn.committed == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "answer",
    [
        {"results": [], "pages": 1},
        {"results": [{"ex_dividend_date": "2024-05-10", "cash_amount": 0.25}], "pages": 5, "truncated": True},
    ],
    ids=["empty", "truncated"],
)
async def test_only_a_complete_non_empty_answer_may_delete(answer: dict) -> None:
    conn = FakeConn()
    await handle_dividends(make_job("dividends", {"symbol": "KO"}), mock_client(fetch_dividends=answer), conn)
    assert not _deletes(conn)


@pytest.mark.asyncio
async def test_the_market_window_removes_only_inside_its_own_dates() -> None:
    client = mock_client(
        fetch_dividends_market={
            "results": [{"ticker": "AAPL", "ex_dividend_date": "2026-09-10", "cash_amount": 0.25,
                         "currency": "USD", "distribution_type": "recurring", "frequency": 4}],
            "pages": 4,
        }
    )
    conn = FakeConn()
    result = await handle_dividends_market(make_job("dividends_market", {"from": "2026-09-08", "to": "2026-11-14"}), client, conn)
    assert result["rows_written"] == 1
    (sql, params), = _deletes(conn)
    assert "ex_date BETWEEN %s AND %s" in sql
    assert params == (date(2026, 9, 8), date(2026, 11, 14))


@pytest.mark.asyncio
async def test_splits_idempotent_sql() -> None:
    client = mock_client(fetch_splits={"results": [], "pages": 1})
    conn = FakeConn()
    r1 = await handle_splits(make_job("splits", {"symbol": "MSFT"}), client, conn)
    r2 = await handle_splits(make_job("splits", {"symbol": "MSFT"}), client, conn)
    assert r1["rows_written"] == 0
    assert r2["rows_written"] == 0
