"""Tests for PCR compute + upsert."""

from __future__ import annotations

from datetime import date
from typing import Any

from bifrost_market_data.analytics.pcr import compute_pcr_for_date, safe_pcr


class _FakeCursor:
    def __init__(self, parent: _FakeConn) -> None:
        self.parent = parent

    def execute(self, query: str, params: Any = None) -> None:
        self.parent.statements.append((query, params))
        q = query.lower()
        if "from market.option_open_interest" in q and "sum(open_interest)" in q:
            trade_date = params[0] if params else None
            underlyings = set(params[1]) if params and len(params) > 1 else None
            # Aggregate like SQL GROUP BY
            buckets: dict[tuple[str, str], int] = {}
            for r in self.parent.oi_rows:
                if r.get("trade_date") != trade_date:
                    continue
                und = r["underlying"]
                if underlyings is not None and und not in underlyings:
                    continue
                key = (und, r["option_right"])
                buckets[key] = buckets.get(key, 0) + int(r.get("open_interest") or 0)
            self.parent._fetchall = [
                (und, right, total) for (und, right), total in sorted(buckets.items())
            ]
        elif "from market.option_snapshot" in q:
            trade_date = params[0] if params else None
            underlyings = set(params[1]) if params and len(params) > 1 else None
            rows = []
            for r in self.parent.vol_rows:
                if r.get("trade_date") != trade_date:
                    continue
                if underlyings is not None and r.get("underlying") not in underlyings:
                    continue
                rows.append((r["underlying"], r["option_right"], r["day_volume"]))
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
    def __init__(
        self,
        oi_rows: list[dict[str, Any]] | None = None,
        vol_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.oi_rows = oi_rows or []
        self.vol_rows = vol_rows or []
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


def test_safe_pcr() -> None:
    assert safe_pcr(100, 50) == 2.0
    assert safe_pcr(0, 50) == 0.0
    assert safe_pcr(10, 0) is None


def test_compute_pcr_fixture() -> None:
    """Manual spot-check: put_oi=200 call_oi=100 → pcr_oi=2; vol 80/40 → 2."""
    td = date(2024, 6, 20)
    conn = _FakeConn(
        oi_rows=[
            {
                "trade_date": td,
                "underlying": "AAPL",
                "option_right": "P",
                "open_interest": 200,
            },
            {
                "trade_date": td,
                "underlying": "AAPL",
                "option_right": "C",
                "open_interest": 100,
            },
        ],
        vol_rows=[
            {
                "trade_date": td,
                "underlying": "AAPL",
                "option_right": "P",
                "day_volume": 80,
            },
            {
                "trade_date": td,
                "underlying": "AAPL",
                "option_right": "C",
                "day_volume": 40,
            },
        ],
    )
    result = compute_pcr_for_date(conn, trade_date=td, underlyings=["AAPL"])
    assert result["groups"] == 1
    row = conn.upserts[0]
    assert row[0] == "AAPL"
    assert row[2] == 2.0  # pcr_oi
    assert row[3] == 2.0  # pcr_volume
    assert row[4] == 200
    assert row[5] == 100
    assert row[6] == 80
    assert row[7] == 40
    insert_sql = next(s[0] for s in conn.statements if "INSERT INTO" in s[0])
    assert "market_analytics.pcr_daily" in insert_sql
    assert "DO UPDATE" in insert_sql


def test_compute_pcr_call_oi_zero() -> None:
    td = date(2024, 6, 20)
    conn = _FakeConn(
        oi_rows=[
            {
                "trade_date": td,
                "underlying": "XYZ",
                "option_right": "P",
                "open_interest": 50,
            },
        ],
        vol_rows=[],
    )
    result = compute_pcr_for_date(conn, trade_date=td)
    assert result["groups"] == 1
    row = conn.upserts[0]
    assert row[2] is None  # pcr_oi
    assert row[4] == 50
    assert row[5] == 0


def test_compute_pcr_empty() -> None:
    conn = _FakeConn()
    result = compute_pcr_for_date(conn, trade_date=date(2024, 6, 20))
    assert result["groups"] == 0
    assert conn.upserts == []
