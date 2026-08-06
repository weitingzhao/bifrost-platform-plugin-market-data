"""Tests for snapshot → option_open_interest extract (D4=B, D5=A)."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from bifrost_market_data.ingest.option_oi_extract import extract_oi_from_snapshots


class _ExtCursor:
    def __init__(self, parent: _ExtConn) -> None:
        self.parent = parent

    def execute(self, query: str, params: Any = None) -> None:
        self.parent.statements.append((query, params))
        q = query.lower()
        if "from market.option_snapshot" in q or "option_snapshot" in q:
            underlyings = None
            if params and len(params) >= 3 and isinstance(params[2], list):
                underlyings = {str(s).upper() for s in params[2]}
            rows = []
            for row in self.parent.snapshot_rows:
                # row: (option_ticker, underlying, oi, trade_date, expiry, strike, right, contract_und)
                und = str(row[1]).upper()
                if underlyings is not None and und not in underlyings:
                    continue
                rows.append(row)
            self.parent._fetchall = rows
        else:
            self.parent._fetchall = []

    def fetchall(self) -> list[Any]:
        return list(self.parent._fetchall)

    def executemany(self, query: str, params_seq: Any) -> None:
        self.parent.statements.append((query, list(params_seq)))
        self.parent.inserts.extend(list(params_seq))

    def __enter__(self) -> _ExtCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _ExtConn:
    def __init__(self) -> None:
        self.statements: list[tuple[str, Any]] = []
        self.inserts: list[tuple[Any, ...]] = []
        self._fetchall: list[Any] = []
        # Simulated JOIN result rows from extract SELECT
        self.snapshot_rows: list[tuple[Any, ...]] = []
        self.committed = 0

    def cursor(self) -> _ExtCursor:
        return _ExtCursor(self)

    def commit(self) -> None:
        self.committed += 1

    def rollback(self) -> None:
        return None


def test_extract_oi_from_snapshots_with_contract() -> None:
    conn = _ExtConn()
    conn.snapshot_rows = [
        (
            "O:AAPL250620C00150000",
            "AAPL",
            1234,
            date(2024, 6, 20),
            date(2025, 6, 20),
            150.0,
            "C",
            "AAPL",
        ),
    ]
    result = extract_oi_from_snapshots(
        conn,
        underlyings=["AAPL"],
        from_date=date(2024, 6, 1),
        to_date=date(2024, 6, 30),
    )
    assert result["candidates"] == 1
    assert result["rows_attempted"] == 1
    assert result["skipped"] == 0
    assert len(conn.inserts) == 1
    assert conn.inserts[0][0] == "O:AAPL250620C00150000"
    assert conn.inserts[0][5] == date(2024, 6, 20)
    assert conn.inserts[0][6] == 1234
    sql = conn.statements[-1][0]
    assert "ON CONFLICT" in sql
    assert "DO NOTHING" in sql
    assert "DO UPDATE" not in sql


def test_extract_oi_parse_ticker_fallback() -> None:
    """When option_contract JOIN is empty, parse option_ticker for expiry/strike/right."""
    conn = _ExtConn()
    conn.snapshot_rows = [
        (
            "O:AAPL250620C00150000",
            "AAPL",
            99,
            date(2024, 6, 19),
            None,
            None,
            None,
            None,
        ),
    ]
    result = extract_oi_from_snapshots(
        conn,
        underlyings=["AAPL"],
        from_date=date(2024, 6, 1),
        to_date=date(2024, 6, 30),
    )
    assert result["candidates"] == 1
    row = conn.inserts[0]
    assert row[2] == date(2025, 6, 20)
    assert row[3] == 150.0
    assert row[4] == "C"


def test_extract_oi_skips_unparseable() -> None:
    conn = _ExtConn()
    conn.snapshot_rows = [
        ("BADTICKER", "AAPL", 1, date(2024, 6, 20), None, None, None, None),
    ]
    result = extract_oi_from_snapshots(
        conn,
        underlyings=["AAPL"],
        from_date=date(2024, 6, 1),
        to_date=date(2024, 6, 30),
    )
    assert result["candidates"] == 0
    assert result["skipped"] == 1
    assert conn.inserts == []


def test_extract_oi_invalid_range() -> None:
    with pytest.raises(ValueError, match="to_date"):
        extract_oi_from_snapshots(
            _ExtConn(),
            underlyings=None,
            from_date=date(2024, 6, 21),
            to_date=date(2024, 6, 20),
        )


def test_extract_oi_filters_underlyings() -> None:
    conn = _ExtConn()
    conn.snapshot_rows = [
        (
            "O:AAPL250620C00150000",
            "AAPL",
            10,
            date(2024, 6, 20),
            date(2025, 6, 20),
            150.0,
            "C",
            "AAPL",
        ),
        (
            "O:MSFT250620C00400000",
            "MSFT",
            20,
            date(2024, 6, 20),
            date(2025, 6, 20),
            400.0,
            "C",
            "MSFT",
        ),
    ]
    result = extract_oi_from_snapshots(
        conn,
        underlyings=["AAPL"],
        from_date=date(2024, 6, 1),
        to_date=date(2024, 6, 30),
    )
    assert result["candidates"] == 1
    assert conn.inserts[0][1] == "AAPL"
