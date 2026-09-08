"""P4 handlers: treasury yields, and the option-history planner's filter."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pytest

from bifrost_market_data.ingest.option_backfill import handle_option_backfill_plan
from bifrost_market_data.ingest.treasury import handle_treasury_yields
from ingest_testutil import FakeConn, make_job, mock_client


@pytest.mark.asyncio
async def test_treasury_yields_upsert() -> None:
    client = mock_client(
        fetch_treasury_yields={
            "results": [
                {
                    "date": "2026-09-03",
                    "yield_1_month": 3.83,
                    "yield_3_month": 3.89,
                    "yield_1_year": 4.11,
                    "yield_2_year": 4.34,
                    "yield_5_year": 4.52,
                    "yield_10_year": 4.77,
                    "yield_30_year": 5.25,
                },
                {"date": None, "yield_10_year": 1.0},  # unusable row is dropped
            ],
            "pages": 1,
        }
    )
    conn = FakeConn()
    result = await handle_treasury_yields(
        make_job("treasury_yields", {"from": "2026-09-01", "to": "2026-09-04"}), client, conn
    )
    assert result["rows_written"] == 1
    assert result["from_date"] == "2026-09-01"
    row = next(st for st in conn.statements if "treasury_yield" in st[0])[1][0]
    assert row[0] == date(2026, 9, 3)
    assert row[6] == 4.77  # 10-year
    client.fetch_treasury_yields.assert_awaited_once_with(date_gte="2026-09-01", date_lte="2026-09-04")


class _CloseConn(FakeConn):
    """Serves the underlying's daily closes to the strike filter."""

    def __init__(self, closes: list[tuple[date, float]]) -> None:
        super().__init__()
        self.closes = closes

    def cursor(self) -> Any:
        return _CloseCursor(self)


class _CloseCursor:
    def __init__(self, parent: _CloseConn) -> None:
        self.parent = parent
        self._rows: list[Any] = []

    def execute(self, query: str, params: Any = None) -> None:
        self.parent.statements.append((query, params))
        self._rows = list(self.parent.closes) if "stock_daily" in query else []

    def executemany(self, query: str, params_seq: Any) -> None:
        self.parent.statements.append((query, list(params_seq)))

    def fetchall(self) -> list[Any]:
        return list(self._rows)

    def fetchone(self) -> Any:
        return (self.parent.next_job_id,)

    def __enter__(self) -> _CloseCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None


def _contract(ticker: str, expiry: str, strike: float) -> dict[str, Any]:
    return {"ticker": ticker, "expiration_date": expiry, "strike_price": strike}


@pytest.mark.asyncio
async def test_backfill_plan_keeps_only_strikes_near_spot() -> None:
    """A 100-strike band at +-30% of a 100 spot keeps 70-130 and drops the wings."""
    expiry = date.today() + timedelta(days=10)
    window_start = expiry - timedelta(days=90)
    client = mock_client(
        fetch_options_contracts={
            "results": [
                _contract("O:AAPL_A", expiry.isoformat(), 100.0),
                _contract("O:AAPL_B", expiry.isoformat(), 125.0),
                _contract("O:AAPL_C", expiry.isoformat(), 200.0),
                _contract("O:AAPL_D", expiry.isoformat(), 50.0),
            ],
            "pages": 1,
            "truncated": False,
        }
    )
    conn = _CloseConn([(window_start, 100.0)])
    result = await handle_option_backfill_plan(
        make_job(
            "option_backfill_plan",
            {
                "underlying": "AAPL",
                "expiry_gte": expiry.replace(day=1).isoformat(),
                "expiry_lte": expiry.isoformat(),
            },
        ),
        client,
        conn,
    )
    assert result["contracts_seen"] == 4
    assert result["contracts_kept"] == 2
    assert result["out_of_strike_band"] == 2
    assert result["no_spot_reference"] == 0
    queued = str(next(st for st in conn.statements if "job_ingest" in st[0])[1])
    assert "O:AAPL_A" in queued and "O:AAPL_B" in queued
    assert "O:AAPL_C" not in queued and "O:AAPL_D" not in queued
    # Each contract is priced over at most its last 90 days of life.
    assert window_start.isoformat() in queued


@pytest.mark.asyncio
async def test_backfill_plan_keeps_contracts_when_spot_is_unknown() -> None:
    """A missing close must not silently drop history from the backfill."""
    expiry = date.today() + timedelta(days=5)
    client = mock_client(
        fetch_options_contracts={
            "results": [_contract("O:AAPL_A", expiry.isoformat(), 999.0)],
            "pages": 1,
        }
    )
    conn = _CloseConn([])
    result = await handle_option_backfill_plan(
        make_job(
            "option_backfill_plan",
            {"underlying": "AAPL", "expiry_gte": expiry.isoformat(), "expiry_lte": expiry.isoformat()},
        ),
        client,
        conn,
    )
    assert result["contracts_kept"] == 1
    assert result["no_spot_reference"] == 1
    assert result["out_of_strike_band"] == 0
