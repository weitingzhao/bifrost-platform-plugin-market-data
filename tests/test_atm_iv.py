"""Tests for ATM IV math + compute_atm_iv_for_date upsert path."""

from __future__ import annotations

from datetime import date
from typing import Any

from bifrost_market_data.analytics.atm_iv import (
    atm_iv_from_side_items,
    build_expiry_side_items,
    compute_atm_iv_for_date,
    representative_spot,
)


class _FakeCursor:
    def __init__(self, parent: _FakeConn) -> None:
        self.parent = parent

    def execute(self, query: str, params: Any = None) -> None:
        self.parent.statements.append((query, params))
        q = query.lower()
        if "v_option_snapshot_with_stock" in q:
            trade_date = params[0] if params else None
            underlyings = None
            if params and len(params) > 1:
                underlyings = set(params[1])
            rows = []
            for r in self.parent.snap_rows:
                if r.get("trade_date") != trade_date:
                    continue
                if underlyings is not None and r.get("underlying") not in underlyings:
                    continue
                rows.append(
                    (
                        r["option_ticker"],
                        r["underlying"],
                        r["iv"],
                        r["underlying_price"],
                        r["expiry"],
                        r["strike"],
                        r["option_right"],
                    )
                )
            self.parent._fetchall = rows
        else:
            self.parent._fetchall = []

    def fetchall(self) -> list[Any]:
        return list(self.parent._fetchall)

    def executemany(self, query: str, params_seq: Any) -> None:
        self.parent.statements.append((query, list(params_seq)))
        self.parent.upserts.extend(list(params_seq))

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _FakeConn:
    def __init__(self, snap_rows: list[dict[str, Any]] | None = None) -> None:
        self.snap_rows = snap_rows or []
        self.statements: list[tuple[str, Any]] = []
        self.upserts: list[tuple[Any, ...]] = []
        self._fetchall: list[Any] = []
        self.committed = 0

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def commit(self) -> None:
        self.committed += 1

    def rollback(self) -> None:
        return None


def test_known_fixture_atm_iv() -> None:
    """Spot 100; call IV 0.25 @ 100, put IV 0.27 @ 100 → atm 0.26."""
    rows = [
        {"strike": 95.0, "option_right": "C", "iv": 0.30, "underlying_price": 100.0},
        {"strike": 100.0, "option_right": "C", "iv": 0.25, "underlying_price": 100.0},
        {"strike": 105.0, "option_right": "C", "iv": 0.22, "underlying_price": 100.0},
        {"strike": 95.0, "option_right": "P", "iv": 0.28, "underlying_price": 100.0},
        {"strike": 100.0, "option_right": "P", "iv": 0.27, "underlying_price": 100.0},
        {"strike": 105.0, "option_right": "P", "iv": 0.24, "underlying_price": 100.0},
    ]
    assert representative_spot(rows) == 100.0
    items = build_expiry_side_items(rows, 100.0)
    atm, iv_c, iv_p, strike = atm_iv_from_side_items(items)
    assert strike == 100.0
    assert iv_c == 0.25
    assert iv_p == 0.27
    assert atm == 0.26
    assert 0.1 <= atm <= 2.0


def test_call_only_side() -> None:
    items = build_expiry_side_items(
        [
            {"strike": 50.0, "option_right": "C", "iv": 0.40},
            {"strike": 55.0, "option_right": "C", "iv": 0.35},
        ],
        50.0,
    )
    atm, iv_c, iv_p, strike = atm_iv_from_side_items(items)
    assert atm == 0.40
    assert iv_c == 0.40
    assert iv_p is None
    assert strike == 50.0


def test_empty_items() -> None:
    atm, iv_c, iv_p, strike = atm_iv_from_side_items([])
    assert atm is None
    assert iv_c is None
    assert iv_p is None
    assert strike is None


def test_invalid_iv_skipped() -> None:
    items = build_expiry_side_items(
        [
            {"strike": 100.0, "option_right": "C", "iv": 0.0},
            {"strike": 100.0, "option_right": "P", "iv": 15.0},
            {"strike": 100.0, "option_right": "C", "iv": 0.20},
        ],
        100.0,
    )
    atm, _, _, _ = atm_iv_from_side_items(items)
    assert atm == 0.20


def test_compute_atm_iv_upsert() -> None:
    td = date(2024, 6, 20)
    expiry = date(2025, 6, 20)
    conn = _FakeConn(
        [
            {
                "trade_date": td,
                "option_ticker": "O:AAPL250620C00100000",
                "underlying": "AAPL",
                "iv": 0.25,
                "underlying_price": 100.0,
                "expiry": expiry,
                "strike": 100.0,
                "option_right": "C",
            },
            {
                "trade_date": td,
                "option_ticker": "O:AAPL250620P00100000",
                "underlying": "AAPL",
                "iv": 0.27,
                "underlying_price": 100.0,
                "expiry": expiry,
                "strike": 100.0,
                "option_right": "P",
            },
        ]
    )
    result = compute_atm_iv_for_date(conn, trade_date=td, underlyings=["AAPL"])
    assert result["groups"] == 1
    assert result["rows_written"] == 1
    row = conn.upserts[0]
    assert row[0] == "AAPL"
    assert row[1] == td
    assert row[2] == expiry
    assert row[3] == 100.0
    assert row[4] == 0.26
    assert row[5] == 100.0
    assert row[6] == "snapshot"
    insert_sql = next(s[0] for s in conn.statements if "INSERT INTO" in s[0])
    assert "market_analytics.atm_iv_daily" in insert_sql
    assert "DO UPDATE" in insert_sql


def test_compute_atm_iv_empty() -> None:
    conn = _FakeConn([])
    result = compute_atm_iv_for_date(conn, trade_date=date(2024, 6, 20))
    assert result["groups"] == 0
    assert result["rows_written"] == 0
    assert conn.upserts == []


def test_multi_expiry_independent() -> None:
    td = date(2024, 6, 20)
    e1 = date(2025, 6, 20)
    e2 = date(2025, 7, 18)
    conn = _FakeConn(
        [
            {
                "trade_date": td,
                "option_ticker": "O:MSFT1C",
                "underlying": "MSFT",
                "iv": 0.20,
                "underlying_price": 100.0,
                "expiry": e1,
                "strike": 100.0,
                "option_right": "C",
            },
            {
                "trade_date": td,
                "option_ticker": "O:MSFT1P",
                "underlying": "MSFT",
                "iv": 0.20,
                "underlying_price": 100.0,
                "expiry": e1,
                "strike": 100.0,
                "option_right": "P",
            },
            {
                "trade_date": td,
                "option_ticker": "O:MSFT2C",
                "underlying": "MSFT",
                "iv": 0.30,
                "underlying_price": 200.0,
                "expiry": e2,
                "strike": 200.0,
                "option_right": "C",
            },
            {
                "trade_date": td,
                "option_ticker": "O:MSFT2P",
                "underlying": "MSFT",
                "iv": 0.30,
                "underlying_price": 200.0,
                "expiry": e2,
                "strike": 200.0,
                "option_right": "P",
            },
        ]
    )
    result = compute_atm_iv_for_date(conn, trade_date=td)
    assert result["groups"] == 2
    by_exp = {u[2]: u[4] for u in conn.upserts}
    assert by_exp[e1] == 0.20
    assert by_exp[e2] == 0.30
