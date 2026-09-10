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
    repair_adjusted_underlyings(cur)
    updated_roots = {p[1] for q, p in cur.statements if q.startswith("UPDATE") and p}
    assert updated_roots == {"BDX1", "SPGI1"}


def test_it_counts_what_it_rewrote_per_table() -> None:
    cur = _Cur(
        CATALOGUE,
        {("option_daily", "BDX1"): 4200, ("option_daily", "SPGI1"): 910,
         ("option_minute", "BDX1"): 80},
    )
    out = repair_adjusted_underlyings(cur)
    assert out == {"option_daily": 5110, "option_minute": 80}


def test_a_clean_catalogue_issues_no_update_at_all() -> None:
    """Idempotent: the second run finds nothing and must not touch the table."""
    cur = _Cur([("O:AAPL250620C00150000", "AAPL")], {})
    assert repair_adjusted_underlyings(cur) == {t: 0 for t in BAR_TABLES}
    assert not [q for q, _ in cur.statements if q.startswith("UPDATE")]


def test_the_rewrite_is_guarded_by_the_catalogue() -> None:
    """A root that is some other instrument's real symbol must survive.

    The EXISTS ties the rewrite to the exact contract, not to the string.
    """
    cur = _Cur(CATALOGUE, {("option_daily", "BDX1"): 1})
    repair_adjusted_underlyings(cur)
    update = next(q for q, _ in cur.statements if q.startswith("UPDATE"))
    assert "EXISTS" in update
    assert "c.option_ticker = d.option_ticker" in update
    assert "c.underlying = %s" in update


def test_snapshot_tables_are_not_touched() -> None:
    """They store the request's underlying and were never wrong."""
    assert "option_snapshot" not in BAR_TABLES
    assert "option_open_interest" not in BAR_TABLES
