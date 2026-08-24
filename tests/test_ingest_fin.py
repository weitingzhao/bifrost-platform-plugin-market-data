"""Tests for financials ingest handler."""

from __future__ import annotations

import json

import pytest
from typing import Any

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
    upsert_sqls = conn.upsert_sqls()
    assert len(upsert_sqls) == 3
    assert all("::jsonb" in sql for sql in upsert_sqls)
    assert any("income_statement" in sql for sql in upsert_sqls)
    all_rows: list[tuple[Any, ...]] = []
    for _, params in conn.statements:
        if isinstance(params, list):
            all_rows.extend(params)
    income_row = next(r for r in all_rows if "revenues" in r[5])
    assert json.loads(income_row[5])["revenues"]["value"] == 100


@pytest.mark.asyncio
async def test_financials_empty() -> None:
    client = mock_client(fetch_financials={"results": []})
    conn = FakeConn()
    result = await handle_financials(make_job("financials", {"symbol": "AAPL"}), client, conn)
    assert result["rows_written"] == 0
