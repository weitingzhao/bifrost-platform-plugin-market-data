"""Tests for Max Pain math + compute_max_pain_for_date upsert path."""

from __future__ import annotations

from datetime import date
from typing import Any

from bifrost_market_data.analytics.max_pain import (
    compute_max_pain_curve,
    compute_max_pain_for_date,
    strike_map_for_expiry,
)


class _FakeCursor:
    def __init__(self, parent: _FakeConn) -> None:
        self.parent = parent

    def execute(self, query: str, params: Any = None) -> None:
        self.parent.statements.append((query, params))
        q = query.lower()
        if "from market.option_open_interest" in q:
            trade_date = params[0] if params else None
            underlyings = None
            if params and len(params) > 1:
                underlyings = set(params[1])
            rows = []
            for r in self.parent.oi_rows:
                if r["trade_date"] != trade_date:
                    continue
                if underlyings is not None and r["underlying"] not in underlyings:
                    continue
                rows.append(
                    (
                        r["underlying"],
                        r["expiry"],
                        r["strike"],
                        r["option_right"],
                        r["open_interest"],
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
    def __init__(self, oi_rows: list[dict[str, Any]] | None = None) -> None:
        self.oi_rows = oi_rows or []
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


def test_strike_map_and_known_max_pain() -> None:
    """Classic fixture: heavy call OI above spot → max pain pulled upward.

    Strikes 90/100/110. Calls concentrated at 90, puts at 110 → pain minimized near 100.
    """
    expiry = date(2025, 6, 20)
    rows = [
        {"expiry": expiry, "strike": 90.0, "option_right": "C", "open_interest": 10},
        {"expiry": expiry, "strike": 100.0, "option_right": "C", "open_interest": 5},
        {"expiry": expiry, "strike": 110.0, "option_right": "C", "open_interest": 1},
        {"expiry": expiry, "strike": 90.0, "option_right": "P", "open_interest": 1},
        {"expiry": expiry, "strike": 100.0, "option_right": "P", "open_interest": 5},
        {"expiry": expiry, "strike": 110.0, "option_right": "P", "open_interest": 10},
    ]
    skmap = strike_map_for_expiry(rows, expiry)
    assert skmap[90.0] == (10, 1)
    assert skmap[100.0] == (5, 5)
    assert skmap[110.0] == (1, 10)

    max_pain, min_pain, points, total_oi = compute_max_pain_curve(skmap)
    assert total_oi == 32
    assert max_pain == 100.0
    assert min_pain > 0
    # Pure-function vs recomputed pain at best strike must match (<0.1%)
    best_point = next(p for p in points if p["strike"] == max_pain)
    assert abs(best_point["pain"] - min_pain) / max(min_pain, 1.0) < 0.001


def test_empty_oi_returns_zeros() -> None:
    strike, pain, points, total_oi = compute_max_pain_curve({})
    assert strike == 0.0
    assert pain == 0.0
    assert points == []
    assert total_oi == 0


def test_multi_expiry_independent() -> None:
    """Two expiries produce independent max-pain strikes."""
    e1 = date(2025, 6, 20)
    e2 = date(2025, 7, 18)
    # Expiry 1: only strike 100 has OI → max pain 100
    # Expiry 2: only strike 200 has OI → max pain 200
    rows_e1 = [
        {"expiry": e1, "strike": 100.0, "option_right": "C", "open_interest": 50},
        {"expiry": e1, "strike": 100.0, "option_right": "P", "open_interest": 50},
    ]
    rows_e2 = [
        {"expiry": e2, "strike": 200.0, "option_right": "C", "open_interest": 30},
        {"expiry": e2, "strike": 200.0, "option_right": "P", "open_interest": 30},
    ]
    mp1, _, _, _ = compute_max_pain_curve(strike_map_for_expiry(rows_e1, e1))
    mp2, _, _, _ = compute_max_pain_curve(strike_map_for_expiry(rows_e2, e2))
    assert mp1 == 100.0
    assert mp2 == 200.0


def test_compute_max_pain_for_date_upserts() -> None:
    td = date(2024, 6, 20)
    expiry = date(2025, 6, 20)
    conn = _FakeConn(
        [
            {
                "trade_date": td,
                "underlying": "AAPL",
                "expiry": expiry,
                "strike": 90.0,
                "option_right": "C",
                "open_interest": 10,
            },
            {
                "trade_date": td,
                "underlying": "AAPL",
                "expiry": expiry,
                "strike": 100.0,
                "option_right": "C",
                "open_interest": 5,
            },
            {
                "trade_date": td,
                "underlying": "AAPL",
                "expiry": expiry,
                "strike": 110.0,
                "option_right": "C",
                "open_interest": 1,
            },
            {
                "trade_date": td,
                "underlying": "AAPL",
                "expiry": expiry,
                "strike": 90.0,
                "option_right": "P",
                "open_interest": 1,
            },
            {
                "trade_date": td,
                "underlying": "AAPL",
                "expiry": expiry,
                "strike": 100.0,
                "option_right": "P",
                "open_interest": 5,
            },
            {
                "trade_date": td,
                "underlying": "AAPL",
                "expiry": expiry,
                "strike": 110.0,
                "option_right": "P",
                "open_interest": 10,
            },
        ]
    )
    result = compute_max_pain_for_date(conn, trade_date=td, underlyings=["AAPL"])
    assert result["groups"] == 1
    assert result["rows_written"] == 1
    assert result["symbols"] == 1
    assert conn.committed >= 1
    assert len(conn.upserts) == 1
    row = conn.upserts[0]
    assert row[0] == "AAPL"
    assert row[1] == td
    assert row[2] == expiry
    assert row[3] == 100.0  # max_pain_strike
    assert row[4] == 32  # total_oi
    assert row[5] > 0  # total_pain_at_strike
    # DO UPDATE path (recompute refreshes), not DO NOTHING
    insert_sql = next(s[0] for s in conn.statements if "INSERT INTO" in s[0])
    assert "market_analytics.max_pain_daily" in insert_sql
    assert "DO UPDATE" in insert_sql
    assert "fetched_at" not in insert_sql


def test_compute_max_pain_empty_oi() -> None:
    conn = _FakeConn([])
    result = compute_max_pain_for_date(conn, trade_date=date(2024, 6, 20))
    assert result["groups"] == 0
    assert result["rows_written"] == 0
    assert conn.upserts == []


def test_compute_max_pain_multi_expiry_upsert() -> None:
    td = date(2024, 6, 20)
    e1 = date(2025, 6, 20)
    e2 = date(2025, 7, 18)
    conn = _FakeConn(
        [
            {
                "trade_date": td,
                "underlying": "MSFT",
                "expiry": e1,
                "strike": 100.0,
                "option_right": "C",
                "open_interest": 10,
            },
            {
                "trade_date": td,
                "underlying": "MSFT",
                "expiry": e1,
                "strike": 100.0,
                "option_right": "P",
                "open_interest": 10,
            },
            {
                "trade_date": td,
                "underlying": "MSFT",
                "expiry": e2,
                "strike": 200.0,
                "option_right": "C",
                "open_interest": 8,
            },
            {
                "trade_date": td,
                "underlying": "MSFT",
                "expiry": e2,
                "strike": 200.0,
                "option_right": "P",
                "open_interest": 8,
            },
        ]
    )
    result = compute_max_pain_for_date(conn, trade_date=td)
    assert result["groups"] == 2
    assert result["rows_written"] == 2
    strikes = {u[2]: u[3] for u in conn.upserts}
    assert strikes[e1] == 100.0
    assert strikes[e2] == 200.0


def test_pure_vs_persisted_divergence_under_threshold() -> None:
    """Acceptance: CronJob upsert values match pure curve (<0.1% pain divergence)."""
    expiry = date(2025, 1, 17)
    td = date(2024, 12, 20)
    rows = [
        {"expiry": expiry, "strike": 50.0, "option_right": "C", "open_interest": 100},
        {"expiry": expiry, "strike": 55.0, "option_right": "C", "open_interest": 80},
        {"expiry": expiry, "strike": 60.0, "option_right": "C", "open_interest": 20},
        {"expiry": expiry, "strike": 50.0, "option_right": "P", "open_interest": 20},
        {"expiry": expiry, "strike": 55.0, "option_right": "P", "open_interest": 80},
        {"expiry": expiry, "strike": 60.0, "option_right": "P", "open_interest": 100},
    ]
    mp, pain, _, toi = compute_max_pain_curve(strike_map_for_expiry(rows, expiry))
    conn = _FakeConn(
        [
            {
                "trade_date": td,
                "underlying": "XYZ",
                "expiry": expiry,
                "strike": r["strike"],
                "option_right": r["option_right"],
                "open_interest": r["open_interest"],
            }
            for r in rows
        ]
    )
    compute_max_pain_for_date(conn, trade_date=td, underlyings=["XYZ"])
    stored = conn.upserts[0]
    assert stored[3] == mp
    assert stored[4] == toi
    assert abs(stored[5] - pain) / max(pain, 1.0) < 0.001
