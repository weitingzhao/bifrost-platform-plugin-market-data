"""Snapshots pinned to a session, daily bars pinned to a contract (R9 C3-P2 / P6).

Both routes used to answer a question the caller did not ask: ``snapshots`` gave
the newest session whatever date was meant, and ``daily`` ignored a misspelled
filter and returned the unfiltered chain.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import pytest
from fastapi.testclient import TestClient

from bifrost_market_data.api import option_daily as od
from bifrost_market_data.api import options as opt
from bifrost_market_data.api.app import create_app

SESSION = date(2026, 9, 10)
CLOSE = datetime(2026, 9, 10, 20, 0, tzinfo=timezone.utc)  # 16:00 New York


class _Cur:
    def __init__(self, owner: _Conn) -> None:
        self.owner = owner

    def execute(self, sql: str, params: Any = None) -> None:
        self.owner.sql.append(sql)
        self.owner.params.append(tuple(params) if params else ())

    def fetchall(self) -> list[Any]:
        return list(self.owner.rows)

    def fetchone(self) -> tuple[Any, ...] | None:
        if "information_schema" in (self.owner.sql[-1] if self.owner.sql else ""):
            return (1,) if self.owner.params[-1][0] == "raw_market" else None
        return None

    def __enter__(self) -> _Cur:
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False


class _Conn:
    def __init__(self, rows: list[Any] | None = None) -> None:
        self.sql: list[str] = []
        self.params: list[tuple[Any, ...]] = []
        self.rows = rows or []

    def cursor(self) -> _Cur:
        return _Cur(self)

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass


def _reads(conn: _Conn) -> list[tuple[str, tuple[Any, ...]]]:
    return [
        (s, p)
        for s, p in zip(conn.sql, conn.params)
        if "information_schema" not in s
    ]


SNAPSHOT_ROW = {
    "option_ticker": "O:NVDA261120C00245000",
    "underlying": "NVDA",
    "snapshot_ts": CLOSE,
    "iv": 0.37,
    "delta": 0.21,
    "gamma": 0.008,
    "theta": -0.07,
    "vega": 0.26,
    "open_interest": 11074,
    "day_volume": 581,
    "day_close": 3.85,
    "day_vwap": 3.9,
    "fetched_at": CLOSE,
}


# ── P2: snapshots for one session ─────────────────────────────────────────


def test_as_of_filters_by_the_sessions_new_york_date() -> None:
    conn = _Conn([SNAPSHOT_ROW])
    rows = opt.query_snapshots(conn, symbol="NVDA", as_of=SESSION)
    sql, params = _reads(conn)[0]
    assert "DATE(timezone('America/New_York', snapshot_ts)) = %s" in sql
    assert SESSION in params
    # Each row still carries the timestamp it was taken at, so a reader can see
    # that the anchor really is that session's close.
    assert rows[0]["snapshot_ts"] == CLOSE.isoformat()


def test_without_as_of_the_query_is_unchanged() -> None:
    conn = _Conn([SNAPSHOT_ROW])
    opt.query_snapshots(conn, symbol="NVDA")
    sql, params = _reads(conn)[0]
    assert "America/New_York" not in sql
    assert params[0] == "NVDA"


def test_a_session_with_no_rows_says_so_instead_of_showing_another_one(monkeypatch) -> None:
    monkeypatch.setattr(opt, "require_db", lambda: _Conn([]))
    client = TestClient(create_app())
    res = client.get("/market/options/snapshots", params={"symbol": "NVDA", "as_of": "2026-09-10"})
    body = res.json()
    assert res.status_code == 200
    assert body["rows"] == [] and body["count"] == 0
    assert body["as_of"] == "2026-09-10"
    assert body["note"] == "no EOD snapshot for session 2026-09-10"


def test_a_session_that_is_not_a_date_is_the_callers_mistake(monkeypatch) -> None:
    monkeypatch.setattr(opt, "require_db", lambda: pytest.fail("must not open a connection"))
    client = TestClient(create_app())
    res = client.get("/market/options/snapshots", params={"symbol": "NVDA", "as_of": "last-friday"})
    assert res.status_code == 422 and "as_of must be a date" in res.json()["detail"]


@pytest.mark.parametrize("wrong,right", [("date", "as_of"), ("trade_date", "as_of"), ("expiry", "expiration")])
def test_snapshots_names_the_parameter_it_actually_has(monkeypatch, wrong: str, right: str) -> None:
    monkeypatch.setattr(opt, "require_db", lambda: pytest.fail("must not open a connection"))
    client = TestClient(create_app())
    res = client.get("/market/options/snapshots", params={"symbol": "NVDA", wrong: "2026-09-10"})
    assert res.status_code == 422
    detail = res.json()["detail"]
    assert wrong in detail and right in detail


# ── P6: daily bars for one contract and one range ─────────────────────────


DAILY_ROW = (
    "O:DDOG260731C00222500", "DDOG", date(2026, 7, 31), 222.5, "C",
    date(2026, 7, 24), 1.0, 1.2, 0.9, 1.1, 42,
)


def test_option_ticker_filters_to_one_contract() -> None:
    conn = _Conn([DAILY_ROW])
    out = od.query_option_daily(conn, symbol="DDOG", option_ticker="o:ddog260731c00222500")
    sql, params = _reads(conn)[0]
    assert "UPPER(TRIM(option_ticker)) = %s" in sql
    assert "O:DDOG260731C00222500" in params
    assert out["contract"]["option_ticker"] == "O:DDOG260731C00222500"
    assert out["count"] == 1


def test_strike_and_right_filter_to_one_contract_too() -> None:
    conn = _Conn([DAILY_ROW])
    od.query_option_daily(conn, symbol="DDOG", expiry="2026-07-31", strike=222.5, right="C")
    sql, params = _reads(conn)[0]
    # A strike the caller typed is compared with a tolerance, not by float equality.
    assert "abs(strike - %s) < 1e-4" in sql
    assert "UPPER(TRIM(option_right)) = %s" in sql
    assert 222.5 in params and "C" in params


def test_a_range_replaces_the_lookback_and_says_which_it_used() -> None:
    conn = _Conn([DAILY_ROW])
    out = od.query_option_daily(
        conn, symbol="DDOG", date_from=date(2026, 7, 1), date_to=date(2026, 7, 31)
    )
    sql, params = _reads(conn)[0]
    assert "bar_date >= %s" in sql and "bar_date <= %s" in sql
    assert "CURRENT_DATE" not in sql
    assert out["window"] == {"from": "2026-07-01", "to": "2026-07-31", "basis": "explicit range"}

    plain = _Conn([DAILY_ROW])
    out2 = od.query_option_daily(plain, symbol="DDOG", days=30)
    assert "CURRENT_DATE" in _reads(plain)[0][0]
    assert out2["window"]["basis"] == "last 30 days"


def test_daily_names_the_parameter_it_actually_has(monkeypatch) -> None:
    monkeypatch.setattr(od, "require_db", lambda: pytest.fail("must not open a connection"))
    client = TestClient(create_app())
    for wrong, right in (("expiration", "expiry"), ("trade_date", "from / to"), ("ticker", "option_ticker")):
        res = client.get("/market/options/daily", params={"symbol": "DDOG", wrong: "x"})
        assert res.status_code == 422, wrong
        assert wrong in res.json()["detail"] and right in res.json()["detail"]


def test_a_contract_says_its_own_underlying() -> None:
    """The pinned list is 12 tickers; making the caller restate DDOG is a trap."""
    conn = _Conn([DAILY_ROW])
    out = od.query_option_daily(conn, option_ticker="O:DDOG260731C00222500")
    _sql, params = _reads(conn)[0]
    assert out["symbol"] == "DDOG" and "DDOG" in params
    assert od.underlying_of_ticker("O:BDX1261016P00350000") == "BDX"
    assert od.underlying_of_ticker("NVDA") is None


def test_neither_a_symbol_nor_a_contract_is_a_question_with_no_subject(monkeypatch) -> None:
    monkeypatch.setattr(od, "require_db", lambda: pytest.fail("must not open a connection"))
    client = TestClient(create_app())
    res = client.get("/market/options/daily")
    assert res.status_code == 422 and "symbol or option_ticker" in res.json()["detail"]


def test_a_right_that_is_not_a_right_is_the_callers_mistake(monkeypatch) -> None:
    monkeypatch.setattr(od, "require_db", lambda: pytest.fail("must not open a connection"))
    client = TestClient(create_app())
    res = client.get("/market/options/daily", params={"symbol": "DDOG", "right": "X"})
    assert res.status_code == 422 and "right must be C or P" in res.json()["detail"]
    assert od._norm_right("put") == "P" and od._norm_right("CALL") == "C"
