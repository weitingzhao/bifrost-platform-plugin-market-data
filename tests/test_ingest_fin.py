"""Tests for financials ingest handler."""

from __future__ import annotations

import json

import pytest

from bifrost_market_data.ingest.financials import handle_financials
from ingest_testutil import FakeConn, make_job, mock_client


@pytest.mark.asyncio
async def test_financials_upsert_nested_statements() -> None:
    client = mock_client(
        fetch_financials={
            "results": [
                {
                    "end_date": "2024-03-31",
                    "timeframe": "quarterly",
                    "fiscal_year": 2024,
                    "fiscal_quarter": 1,
                    "financials": {
                        "income_statement": {"revenues": {"value": 100}},
                        "balance_sheet": {"assets": {"value": 200}},
                        "cash_flow_statement": {"net_cash_flow": {"value": 50}},
                    },
                }
            ],
            "pages": 1,
        }
    )
    conn = FakeConn()
    result = await handle_financials(
        make_job("financials", {"symbol": "AAPL", "timeframe": "quarterly"}),
        client,
        conn,
    )
    assert result["rows_written"] == 3
    sql = conn.statements[0][0]
    assert "market.stock_financials" in sql
    assert "::jsonb" in sql
    types = {r[1] for r in conn.statements[0][1]}
    assert types == {"income_statement", "balance_sheet", "cash_flow_statement"}
    income = next(r for r in conn.statements[0][1] if r[1] == "income_statement")
    assert json.loads(income[6])["revenues"]["value"] == 100


@pytest.mark.asyncio
async def test_financials_empty() -> None:
    client = mock_client(fetch_financials={"results": []})
    conn = FakeConn()
    result = await handle_financials(make_job("financials", {"symbol": "AAPL"}), client, conn)
    assert result["rows_written"] == 0
