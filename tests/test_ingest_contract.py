"""Tests for option_contract ingest handler."""

from __future__ import annotations

import pytest

from bifrost_market_data.ingest.option_contract import handle_option_contract
from ingest_testutil import FakeConn, make_job, mock_client


@pytest.mark.asyncio
async def test_option_contract_upsert() -> None:
    client = mock_client(
        fetch_options_contracts={
            "results": [
                {
                    "ticker": "O:AAPL250620C00150000",
                    "underlying_ticker": "AAPL",
                    "expiration_date": "2025-06-20",
                    "strike_price": 150,
                    "contract_type": "call",
                    "exercise_style": "american",
                    "shares_per_contract": 100,
                },
                {
                    "ticker": "O:AAPL250620P00150000",
                    "underlying_ticker": "AAPL",
                    "expiration_date": "2025-06-20",
                    "strike_price": 150,
                    "contract_type": "put",
                },
            ],
            "pages": 1,
            "truncated": False,
        }
    )
    conn = FakeConn()
    result = await handle_option_contract(
        make_job("option_contract", {"underlying": "aapl"}),
        client,
        conn,
    )
    assert result["rows_written"] == 2
    assert result["expirations_written"] == 1
    assert result["underlying"] == "AAPL"
    sqls = "\n".join(conn.upsert_sqls())
    assert "market.option_contract" in sqls
    assert "market.option_expiration" in sqls
