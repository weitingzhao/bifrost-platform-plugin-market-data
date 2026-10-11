"""Session locks persist but no open transaction waits on vendor HTTP."""

import asyncio
import json
from datetime import date

import pytest

from bifrost_market_data.ingest.long_pilot import run_pilot


class Connection:
    closed = False
    active_transaction = False

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def execute(self, sql, args=()):
        self.sql = sql
        self.active_transaction = True

    def fetchone(self):
        if "current_user" in self.sql:
            return ("data_writer", "bifrost_golden_source")
        if "pg_try_advisory_lock" in self.sql:
            return (True,)
        return (0,)

    def fetchall(self):
        return []

    def commit(self):
        self.active_transaction = False

    def rollback(self):
        assert not self.closed
        self.active_transaction = False


def test_reference_and_aggregate_never_wait_inside_database_transaction(tmp_path):
    conn = Connection()

    class Client:
        attempts = {"total": 0, "reference": 0, "bars": 0}

        async def fetch_options_contracts(self, **kwargs):
            assert not conn.active_transaction
            return {
                "status": "OK",
                "results": [{"ticker": "O:SPY270319C00770000", "shares_per_contract": 100}],
            }

        async def fetch_stock_aggs(self, *args, **kwargs):
            assert not conn.active_transaction
            return {"results": []}

    req = {
        "symbol": "SPY",
        "option_ticker": "O:SPY270319C00770000",
        "expiry": "2027-03-19",
        "days": [date(2026, 9, 2)],
        "start": "2026-09-02",
        "end": "2026-09-02",
    }
    asyncio.run(
        run_pilot(conn, Client(), [req], receipt=tmp_path / "receipt", lock=tmp_path / "lock")
    )
    report = json.loads((tmp_path / "receipt").read_text())
    assert report["status"] == "finished_requires_coverage_acceptance"
    assert report["historical_reference_records"][0]["session"] == "2026-09-02"


def test_closed_connection_does_not_mask_stopped_receipt(tmp_path):
    conn = Connection()

    class Client:
        attempts = {"total": 1, "reference": 1, "bars": 0}

        async def fetch_options_contracts(self, **kwargs):
            conn.closed = True
            raise RuntimeError("disconnected")

    req = {
        "symbol": "SPY",
        "option_ticker": "O:SPY270319C00770000",
        "expiry": "2027-03-19",
        "days": [date(2026, 9, 2)],
        "start": "2026-09-02",
        "end": "2026-09-02",
    }
    with pytest.raises(RuntimeError, match="disconnected"):
        asyncio.run(
            run_pilot(conn, Client(), [req], receipt=tmp_path / "receipt", lock=tmp_path / "lock")
        )
    report = json.loads((tmp_path / "receipt").read_text())
    assert report["status"] == "stopped" and report["attempts"]["total"] == 1


def test_reconciled_historical_cache_is_revalidated_without_http(tmp_path):
    conn = Connection()

    class Client:
        attempts = {"total": 35, "reference": 28, "bars": 7}
        historical_reference_cache = {
            ("SPY", "2026-09-02", "2027-03-19"): {
                "status": "OK",
                "results": [{"ticker": "O:SPY270319C00770000", "shares_per_contract": 100}],
            }
        }

        async def fetch_options_contracts(self, **kwargs):
            raise AssertionError("must reuse reconciled reference")

        async def fetch_stock_aggs(self, *args, **kwargs):
            assert not conn.active_transaction
            return {"results": []}

    req = {
        "symbol": "SPY",
        "option_ticker": "O:SPY270319C00770000",
        "expiry": "2027-03-19",
        "days": [date(2026, 9, 2)],
        "start": "2026-09-02",
        "end": "2026-09-02",
    }
    asyncio.run(
        run_pilot(conn, Client(), [req], receipt=tmp_path / "receipt", lock=tmp_path / "lock")
    )
    assert json.loads((tmp_path / "receipt").read_text())["historical_reference_records"][0][
        "reused"
    ]
