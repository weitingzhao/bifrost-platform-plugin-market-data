"""Undo the 1,484,987 rows 0.31.6 filed under an underlying that was not theirs."""

from __future__ import annotations

from typing import Any

from bifrost_market_data.schema.ctid_damage_restore import (
    DAMAGED,
    _restore_sql,
    restore_ctid_damage,
)


class _Cur:
    def __init__(self, conn: "_Conn") -> None:
        self.conn = conn
        self.rowcount = 0

    def __enter__(self) -> "_Cur":
        return self

    def __exit__(self, *_a: Any) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        q = " ".join(str(sql).split())
        self.conn.statements.append((q, params))
        if q.startswith("SET LOCAL"):
            self.rowcount = 0
            return
        left = self.conn.left.get(params[0], 0)
        n = min(left, 50_000)
        self.conn.left[params[0]] = left - n
        self.rowcount = n


class _Conn:
    def __init__(self, left: dict[str, int]) -> None:
        self.left = dict(left)
        self.statements: list[tuple[str, Any]] = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _Cur:
        return _Cur(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def test_the_outer_where_repeats_every_predicate() -> None:
    """The ctid list narrows a batch; it must never be the only thing selecting
    rows. That was the bug — option_daily is partitioned by bar_date and a ctid
    is unique only within a partition, so filtering on it alone matched the same
    slot in every other partition."""
    sql = " ".join(_restore_sql("option_daily", ("O:SPX",)).split())
    assert "d.underlying = %s" in sql
    # `%%` is the literal percent psycopg unescapes at execute time.
    assert "d.option_ticker NOT LIKE 'O:SPX%%'" in sql
    assert sql.index("d.underlying") < sql.index("d.ctid")


def test_a_ticker_that_legitimately_carries_the_underlying_is_left_alone() -> None:
    """O:SPXW… really does belong to SPX — those 440,315 rows were moved
    correctly and must not be moved back."""
    sql = _restore_sql("option_daily", ("O:SPX",))
    assert "NOT LIKE 'O:SPX%%'" in " ".join(sql.split())  # covers O:SPX… and O:SPXW…


def test_brk_b_accepts_both_spellings() -> None:
    assert DAMAGED["BRK.B"] == ("O:BRKB", "O:BRK.B")
    sql = " ".join(_restore_sql("option_daily", DAMAGED["BRK.B"]).split())
    assert "NOT LIKE 'O:BRKB%%'" in sql and "NOT LIKE 'O:BRK.B%%'" in sql


def test_it_restores_the_root_the_ticker_spells() -> None:
    """option_ticker was never touched, so the value before the damage is
    recoverable from it — every damaged row came from a backfill job whose
    payload had no underlying, so its old value was exactly the parsed root."""
    sql = " ".join(_restore_sql("option_daily", ("O:SPX",)).split())
    assert "SET underlying = upper(btrim(substring(option_ticker FROM 3" in sql


def test_it_commits_each_chunk_so_the_work_survives_the_budget() -> None:
    conn = _Conn({"SPX": 120_000, "BRK.B": 0})
    out = restore_ctid_damage(conn)
    assert out == {"SPX": 120_000}
    # 50k + 50k + 20k, then one that answers zero.
    assert conn.commits == 4 + 1  # plus the single empty probe for BRK.B


def test_a_failure_stops_that_underlying_and_not_the_deploy() -> None:
    class _Boom(_Conn):
        def cursor(self) -> Any:
            raise RuntimeError("canceling statement due to statement timeout")

    conn = _Boom({"SPX": 100})
    assert restore_ctid_damage(conn) == {}
    assert conn.rollbacks >= 1


def test_the_budget_stops_it_between_chunks() -> None:
    ticks = iter([0.0, 0.0, 999.0, 999.0, 999.0, 999.0])
    conn = _Conn({"SPX": 500_000, "BRK.B": 500_000})
    out = restore_ctid_damage(conn, budget_sec=240, now=lambda: next(ticks))
    assert out == {"SPX": 50_000}  # one chunk landed and stayed landed
