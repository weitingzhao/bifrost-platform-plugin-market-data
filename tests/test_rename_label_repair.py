"""Moving rows already filed under a symbol the vendor has renamed.

The forward fix stops new rows arriving that way; 24,238 were already written
when it landed, and a chain split across two labels is a chain no reader can ask
for. These pin the two discovery routes — one of the three renames is invisible
to each of them — and the shape of the update that moves the rows.
"""

from __future__ import annotations

from typing import Any, Sequence

from bifrost_market_data.schema.rename_label_repair import (
    RENAME_TABLES,
    rename_pairs,
    repair_renamed_labels,
)


class _Cur:
    def __init__(self, parent: _Conn) -> None:
        self.parent = parent
        self.rowcount = 0
        self._rows: list[tuple[Any, ...]] = []

    def execute(self, sql: str, params: Any = None) -> None:
        q = " ".join(str(sql).split())
        self.parent.statements.append((q, params))
        if "FROM raw_market.ticker dead" in q:
            self._rows = list(self.parent.ticker_pairs)
            return
        if "FROM raw_market.option_contract" in q and q.startswith("WITH r AS"):
            self._rows = list(self.parent.catalogue_pairs)
            return
        if q.startswith("UPDATE raw_market."):
            table = q.split("UPDATE raw_market.")[1].split(" ")[0]
            if len(params) == 4:
                # The history move: root spells the *retired* symbol, and the
                # fourth parameter is that symbol again, for the date bound.
                alive, dead, root, dated = params
                assert root == dead, "pre-rename rows are the ones still rooted the old way"
                assert dated == dead, "the bound is the retired symbol's own last bar"
                assert "FROM raw_market.stock_daily WHERE symbol = %s" in q, (
                    "an unbounded history move would sweep up a stale writer's rows"
                )
                self.rowcount = self.parent.history_hits.get((table, dead), 0)
                return
            alive, dead, root = params
            assert alive == root, "the label moves to the root, so those two are one value"
            self.rowcount = self.parent.hits.get((table, dead), 0)
            return
        self._rows = []

    def fetchall(self) -> Sequence[Any]:
        return self._rows

    def __enter__(self) -> _Cur:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _Conn:
    def __init__(
        self,
        *,
        ticker_pairs: Sequence[tuple[str, str]] = (),
        catalogue_pairs: Sequence[tuple[str, str]] = (),
        hits: dict[tuple[str, str], int] | None = None,
        history_hits: dict[tuple[str, str], int] | None = None,
    ) -> None:
        self.ticker_pairs = list(ticker_pairs)
        self.catalogue_pairs = list(catalogue_pairs)
        self.hits = hits or {}
        #: Rows under the dead label that are rooted the dead way — the company's
        #: own history from before the rename, which `_MOVE` correctly leaves.
        self.history_hits = history_hits or {}
        self.statements: list[tuple[str, Any]] = []
        self.commits = 0

    def cursor(self) -> _Cur:
        return _Cur(self)

    def commit(self) -> None:
        self.commits += 1


# ── discovery ─────────────────────────────────────────────────────────────


def test_both_routes_are_needed_and_neither_is_enough() -> None:
    """Measured 2026-09-26: the reference table proves ISSC → IA and EQR → VMRK
    and cannot see SATS → ECHO, because SATS has no row. The catalogue shows
    SATS → ECHO and ISSC → IA and cannot see EQR → VMRK, because
    option_contract's underlying is an update column and a later VMRK job had
    already rewritten it."""
    conn = _Conn(
        ticker_pairs=[("ISSC", "IA"), ("EQR", "VMRK")],
        catalogue_pairs=[("SATS", "ECHO"), ("ISSC", "IA")],
    )
    with conn.cursor() as cur:
        assert rename_pairs(cur) == [("EQR", "VMRK"), ("ISSC", "IA"), ("SATS", "ECHO")]


def test_a_pair_that_is_not_a_move_is_dropped() -> None:
    conn = _Conn(ticker_pairs=[("IA", "IA"), ("", "IA"), ("ISSC", None)])
    with conn.cursor() as cur:
        assert rename_pairs(cur) == []


def test_pairs_are_deduplicated_across_routes() -> None:
    conn = _Conn(ticker_pairs=[("ISSC", "IA")], catalogue_pairs=[("issc", "ia")])
    with conn.cursor() as cur:
        assert rename_pairs(cur) == [("ISSC", "IA")]


# ── the move ──────────────────────────────────────────────────────────────


def test_every_option_table_is_visited_for_every_pair() -> None:
    conn = _Conn(ticker_pairs=[("ISSC", "IA")])
    repair_renamed_labels(conn)
    updated = [q for q, _ in conn.statements if q.startswith("UPDATE raw_market.")]
    assert {q.split("UPDATE raw_market.")[1].split(" ")[0] for q in updated} == set(RENAME_TABLES)
    # One mislabelled-move statement per table, plus a history move for each table
    # that stamps a date. option_contract stamps none, so it gets only the first.
    from bifrost_market_data.schema.rename_label_repair import _HISTORY_DATE_COLUMN

    assert len(updated) == len(RENAME_TABLES) + len(_HISTORY_DATE_COLUMN)


def test_the_update_only_moves_rows_whose_root_spells_the_successor() -> None:
    """438 of EQR's rows are rooted VMRK1 and 26 of SATS's ECHO1 — adjusted
    roots, not tickers. They stay for adjusted_root_repair, whose direction is
    root → catalogue underlying rather than the reverse."""
    conn = _Conn(ticker_pairs=[("EQR", "VMRK")])
    repair_renamed_labels(conn)
    update = next(q for q, _ in conn.statements if q.startswith("UPDATE raw_market.option_snapshot"))
    assert "substring(option_ticker FROM 3 FOR length(option_ticker) - 17) = %s" in update
    assert "WHERE underlying = %s" in update, "equality, so the label index carries it"


def test_rows_moved_are_reported_per_table() -> None:
    conn = _Conn(
        ticker_pairs=[("ISSC", "IA")],
        catalogue_pairs=[("SATS", "ECHO")],
        hits={
            ("option_snapshot", "SATS"): 8012,
            ("option_snapshot", "ISSC"): 1672,
            ("option_daily", "SATS"): 1562,
        },
    )
    moved = repair_renamed_labels(conn)
    assert moved == {"option_snapshot": 9684, "option_daily": 1562}


def test_nothing_to_move_is_an_empty_answer_and_no_updates() -> None:
    conn = _Conn()
    assert repair_renamed_labels(conn) == {}
    assert not [q for q, _ in conn.statements if q.startswith("UPDATE")]


def test_each_statement_commits_so_a_budget_keeps_what_it_moved() -> None:
    """The adjusted-root repair learned this the hard way: one transaction for
    every rewrite meant a timeout rolled back the lot, and the run after it
    printed "nothing written", which reads exactly like nothing left to do."""
    conn = _Conn(ticker_pairs=[("ISSC", "IA")])
    repair_renamed_labels(conn)
    updates = len([q for q, _ in conn.statements if q.startswith("UPDATE")])
    assert conn.commits >= updates


def test_a_spent_budget_returns_what_it_did_rather_than_raising() -> None:
    conn = _Conn(
        ticker_pairs=[("ISSC", "IA")],
        hits={("option_snapshot", "ISSC"): 5},
    )
    moved = repair_renamed_labels(conn, budget_sec=-1.0)
    assert moved == {}
    assert not [q for q, _ in conn.statements if q.startswith("UPDATE")]


# ── the listings have to abut ──────────────────────────────────────────────


def _routes() -> dict[str, str]:
    from bifrost_market_data.schema import rename_label_repair as m

    return {"ticker": m._PAIRS_FROM_TICKER, "catalogue": m._PAIRS_FROM_CATALOGUE}


def test_both_routes_require_the_listings_to_abut() -> None:
    """A rename hands the price series over within days.

    Sharing a CIK and a FIGI across a gap of years is a registrant reusing an
    identifier, not one company changing its symbol, and it must not move option
    rows. Measured 2026-09-27 this filters nothing: of the twenty pairs the
    reference route finds, sixteen abut at one day and four at three, none
    further. It was added expecting to drop ADIGW/ADIG and BBBY/NXH and does not,
    because those are real symbol changes — ADIG carries ADIGW's company name and
    is instrument_type CS, not a warrant. So it bounds what the rule can do later
    rather than correcting what it does now, and the measurement is the reason to
    keep it honest about which of those it is.

    It matters most on the catalogue route, which has no CIK to link through: a
    live root over a dead label is the whole of its evidence there.
    """
    from bifrost_market_data.schema.rename_label_repair import _RENAME_MAX_GAP_DAYS

    for name, sql in _routes().items():
        flat = " ".join(sql.split())
        assert f"<= {_RENAME_MAX_GAP_DAYS}" in flat, f"{name} route does not bound the gap"
        assert "next_bar - " in flat and "last_bar" in flat, f"{name} route compares no dates"


def test_the_successor_is_judged_on_the_handover_not_its_earlier_life() -> None:
    """ECHO held the symbol before, so its first bar ever is the wrong date.

    ``stock_daily`` has ECHO bars from 2021-09-09 to 2021-11-22 for Echo Global
    Logistics, then nothing until EchoStar took the symbol on 2026-06-24 — the
    session after SATS's last bar on 2026-06-23. Read as "the successor's first
    bar" the gap is 4.6 years and SATS → ECHO, the pair this repair exists for,
    is rejected. Read as "its first bar after the retired symbol's last" it is one
    session. Simplifying the subquery to an unconditional ``min`` is the mistake
    this test exists to catch.
    """
    for name, sql in _routes().items():
        flat = " ".join(sql.split())
        assert "min(bar_date) AS next_bar" in flat, f"{name} route takes no successor date"
        head, _, tail = flat.partition("min(bar_date) AS next_bar")
        assert "bar_date >" in tail.split(")")[0] + tail.split(")")[1], (
            f"{name} route takes the successor's first bar outright, not the handover"
        )


def test_a_missing_price_series_on_either_side_moves_nothing() -> None:
    """No dates is no evidence, and the write is an UPDATE nobody can undo.

    A symbol with no bars cannot be shown to abut anything, so it fails closed.
    That does cost a rename of a name that never printed a stock bar; moving rows
    on no time evidence at all costs more.
    """
    for name, sql in _routes().items():
        flat = " ".join(sql.split())
        assert "last_bar IS NOT NULL" in flat, f"{name} route acts without the dead date"
        assert "next_bar IS NOT NULL" in flat, f"{name} route acts without the live date"


# ── the history before the rename ──────────────────────────────────────────


def test_the_companys_own_history_moves_too() -> None:
    """OCC renames the contracts, so the series is split by ticker as well as label.

    ECHO's option_daily begins at the handover and the two years before it sit
    under `O:SATS…` labelled SATS. Those rows are not mislabelled — the label
    agrees with the root — so `_MOVE` leaves them, and the company's series stays
    torn on the time axis. Research reads option_daily by (bar_date, underlying),
    so asking ECHO for a date before 2026-06-24 returns nothing and says nothing.
    Measured 2026-09-27: 39,449 rows, option_daily SATS 32,263, EQR 5,571,
    ISSC 1,615, and none in any other table.
    """
    conn = _Conn(
        catalogue_pairs=[("SATS", "ECHO")],
        history_hits={("option_daily", "SATS"): 32263},
    )
    assert repair_renamed_labels(conn) == {"option_daily": 32263}


def test_the_history_move_is_bounded_by_the_handover() -> None:
    """A row dated after the handover is a stale writer, not old history.

    That is a different fault and must not be swept up by a repair aimed at this
    one. Unlike `_MOVE`, this statement's root check matches the ordinary case, so
    it is not self-limiting: every row under the dead label qualifies and the date
    bound is the only structural limit there is.
    """
    conn = _Conn(catalogue_pairs=[("SATS", "ECHO")])
    repair_renamed_labels(conn)
    hist = [
        (q, p)
        for q, p in conn.statements
        if q.startswith("UPDATE raw_market.option_daily") and len(p or ()) == 4
    ]
    assert len(hist) == 1, "one history statement for option_daily"
    q, _p = hist[0]
    assert "bar_date <= ( SELECT max(bar_date) FROM raw_market.stock_daily" in q
    # And the mislabelled move is still a separate statement, still root=successor.
    plain = [
        p
        for q2, p in conn.statements
        if q2.startswith("UPDATE raw_market.option_daily") and len(p or ()) == 3
    ]
    assert plain and plain[0][0] == plain[0][2] == "ECHO"


def test_a_table_with_no_date_gets_no_history_move() -> None:
    """option_contract stamps no date, so there is nothing to bound by."""
    from bifrost_market_data.schema.rename_label_repair import _HISTORY_DATE_COLUMN

    assert "option_contract" not in _HISTORY_DATE_COLUMN
    conn = _Conn(catalogue_pairs=[("SATS", "ECHO")])
    repair_renamed_labels(conn)
    contract = [
        p
        for q, p in conn.statements
        if q.startswith("UPDATE raw_market.option_contract")
    ]
    assert all(len(p or ()) == 3 for p in contract), "no unbounded rewrite of the catalogue"


def test_the_two_moves_commit_separately() -> None:
    """The history move reaches wider, so a timeout on it must not cost the other."""
    conn = _Conn(
        catalogue_pairs=[("SATS", "ECHO")],
        hits={("option_daily", "SATS"): 1562},
        history_hits={("option_daily", "SATS"): 32263},
    )
    moved = repair_renamed_labels(conn)
    assert moved["option_daily"] == 1562 + 32263
    updates = len([q for q, _ in conn.statements if q.startswith("UPDATE")])
    assert conn.commits >= updates
