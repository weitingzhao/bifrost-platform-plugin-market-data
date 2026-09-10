"""Rows filed under an adjusted root belong to the underlying the catalogue names.

0.21.0 taught the parser to accept O:BDX1260918C00085000; nothing taught it that
the contract belongs to BDX. option_snapshot has always stored the request's
underlying, so the same corporate-action family sat under two different symbols.
"""

from __future__ import annotations

from typing import Any, Sequence

from bifrost_market_data.schema.adjusted_root_repair import (
    BAR_TABLES,
    mismatched_roots,
    repair_adjusted_underlyings,
)


class _Cur:
    """Answers the roots query from a catalogue, and counts what updates match."""

    def __init__(self, catalogue: Sequence[tuple[str, str]], hits: dict[tuple[str, str], int]):
        self.catalogue = catalogue
        self.hits = hits
        self.rowcount = 0
        self.statements: list[tuple[str, Any]] = []
        self._rows: list[tuple[Any, ...]] = []

    def execute(self, sql: str, params: Any = None) -> None:
        q = " ".join(str(sql).split())
        self.statements.append((q, params))
        if q.startswith("SET LOCAL"):
            self.rowcount = 0
            return
        if "FROM raw_market.option_contract" in q and q.startswith("SELECT DISTINCT"):
            self._rows = [
                (
                    t[2 : len(t) - 15],
                    u,
                )
                for t, u in self.catalogue
                if len(t) > 17 and t[2 : len(t) - 15] != u
            ]
            self.rowcount = len(self._rows)
            return
        if q.startswith("UPDATE raw_market."):
            table = q.split("UPDATE raw_market.")[1].split(" ")[0]
            canonical, root, _ = params
            self.rowcount = self.hits.get((table, root), 0)
            return
        self._rows = []
        self.rowcount = 0

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows

    def __enter__(self) -> "_Cur":
        return self

    def __exit__(self, *_a: Any) -> None:
        return None


class _Conn:
    """One shared cursor, and a count of how often the repair committed.

    The commit count is the test's whole point now: the first version ran every
    rewrite inside the migration's single transaction, so a timeout on the
    fortieth rolled back the thirty-nine before it — and against a real backlog
    of 2,964,147 rows that meant it could never finish while reporting, on the
    next run, that there was nothing to do.
    """

    def __init__(self, cur: _Cur) -> None:
        self._cur = cur
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _Cur:
        return self._cur

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


CATALOGUE = [
    ("O:BDX1260918C00085000", "BDX"),
    ("O:BDX1260918P00085000", "BDX"),
    ("O:SPGI1250117P00037500", "SPGI"),
    ("O:AAPL250620C00150000", "AAPL"),  # not adjusted — root equals underlying
    ("O:BRK.B250117C00500000", "BRK.B"),  # a dot in the root is not a suffix
]


def test_only_adjusted_families_are_listed() -> None:
    cur = _Cur(CATALOGUE, {})
    assert mismatched_roots(cur) == [("BDX1", "BDX"), ("SPGI1", "SPGI")]


def test_a_plain_root_is_never_rewritten() -> None:
    """AAPL and BRK.B must not appear in a single UPDATE."""
    cur = _Cur(CATALOGUE, {("option_daily", "BDX1"): 4200})
    repair_adjusted_underlyings(_Conn(cur))
    updated_roots = {p[1] for q, p in cur.statements if q.startswith("UPDATE") and p}
    assert updated_roots == {"BDX1", "SPGI1"}


def test_it_counts_what_it_rewrote_per_table() -> None:
    cur = _Cur(
        CATALOGUE,
        {("option_daily", "BDX1"): 4200, ("option_daily", "SPGI1"): 910,
         ("option_minute", "BDX1"): 80},
    )
    out = repair_adjusted_underlyings(_Conn(cur))
    assert out == {"option_daily": 5110, "option_minute": 80}


def test_a_clean_catalogue_issues_no_update_at_all() -> None:
    """Idempotent: the second run finds nothing and must not touch the table."""
    cur = _Cur([("O:AAPL250620C00150000", "AAPL")], {})
    assert repair_adjusted_underlyings(_Conn(cur)) == {t: 0 for t in BAR_TABLES}
    assert not [q for q, _ in cur.statements if q.startswith("UPDATE")]


def test_the_rewrite_is_guarded_by_the_catalogue() -> None:
    """A root that is some other instrument's real symbol must survive.

    The EXISTS ties the rewrite to the exact contract, not to the string.
    """
    cur = _Cur(CATALOGUE, {("option_daily", "BDX1"): 1})
    repair_adjusted_underlyings(_Conn(cur))
    update = next(q for q, _ in cur.statements if q.startswith("UPDATE"))
    assert "EXISTS" in update
    assert "c.option_ticker = d.option_ticker" in update
    assert "c.underlying = %s" in update


def test_snapshot_tables_are_not_touched() -> None:
    """They store the request's underlying and were never wrong."""
    assert "option_snapshot" not in BAR_TABLES
    assert "option_open_interest" not in BAR_TABLES


# ── it has to make progress, not roll it all back ─────────────────────────


def test_each_root_is_committed_as_it_goes() -> None:
    """The backlog is 2,964,147 rows across 36 families and two dozen monthly
    partitions. No pass rewrites that inside any sensible budget while the queue
    is ingesting, so the run has to leave behind what it managed."""
    cur = _Cur(CATALOGUE, {("option_daily", "BDX1"): 4200})
    conn = _Conn(cur)
    repair_adjusted_underlyings(conn)
    # Two families × two tables.
    assert conn.commits == 4


def test_one_slow_family_costs_that_family_and_not_the_run() -> None:
    """A statement timeout used to abort the transaction and take every rewrite
    before it with it — which is also why the next run looked idempotent."""

    class _Flaky(_Cur):
        def execute(self, sql: str, params: Any = None) -> None:
            q = " ".join(str(sql).split())
            if q.startswith("UPDATE") and params and params[1] == "BDX1":
                raise RuntimeError("canceling statement due to statement timeout")
            super().execute(sql, params)

    cur = _Flaky(CATALOGUE, {("option_daily", "SPGI1"): 910})
    conn = _Conn(cur)
    out = repair_adjusted_underlyings(conn)
    # SPGI1 still got done in both tables; BDX1 is left for the next deploy.
    assert out["option_daily"] == 910
    assert conn.rollbacks == 2  # one per table for the family that failed


def test_the_budget_stops_it_rather_than_the_deploy() -> None:
    """It rides a schema migration. A deploy that waits ten minutes on a data
    fix is a deploy nobody runs."""
    # First call sets the deadline; each loop turn checks it. So: start,
    # one family inside the budget, then past it.
    ticks = iter([0.0, 0.0, 999.0, 999.0, 999.0, 999.0])
    cur = _Cur(CATALOGUE, {("option_daily", "BDX1"): 4200})
    conn = _Conn(cur)
    out = repair_adjusted_underlyings(conn, budget_sec=90, now=lambda: next(ticks))
    # It did the first family and stopped; nothing raised.
    assert out["option_daily"] == 4200
    assert len([q for q, _ in cur.statements if q.startswith("UPDATE")]) == 1
