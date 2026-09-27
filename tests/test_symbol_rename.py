"""The rename rule: two conditions, and the cases that must not trip it.

Every row here is a real reading of `raw_market.ticker` from 2026-09-26, because
the rule's whole job is to tell three pairs apart that look identical from a
distance — a rename, a merger, and two live listings whose option roots differ.
"""

from __future__ import annotations

from typing import Any

import pytest

from bifrost_market_data import symbol_rename as sr


class FakeCursor:
    def __init__(self, parent: FakeConn) -> None:
        self.parent = parent

    def execute(self, query: str, params: Any = None) -> None:
        self.parent.queries.append((query, params))
        if self.parent.raises:
            raise RuntimeError("catalogue unreadable")

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.parent.rows

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class FakeConn:
    def __init__(self, rows: list[tuple[Any, ...]] | None = None, raises: bool = False) -> None:
        self.rows = rows if rows is not None else []
        self.raises = raises
        self.queries: list[tuple[str, Any]] = []
        self.rolled_back = 0

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def rollback(self) -> None:
        self.rolled_back += 1


def pair(dead: tuple[Any, Any, Any], alive: tuple[Any, Any, Any]) -> FakeConn:
    """One row shaped like the two-row-by-primary-key lookup."""
    return FakeConn([tuple(dead) + tuple(alive)])


ISSC = (False, "0000836690", "BBG000BF1Z04")
IA = (True, "0000836690", "BBG000BF1Z04")
EQR = (False, "0000906107", "BBG000BG8M31")
VMRK = (True, "0000906107", "BBG000BG8M31")
AVB = (False, "0000915912", "BBG000C2FJB1")
ABSENT = (None, None, None)


# ── the two renames and the merger ────────────────────────────────────────


def test_a_rename_with_both_rows_needs_the_same_cik_and_figi() -> None:
    """ISSC → IA, 2026-08-18: one registrant, one instrument, one of them retired."""
    assert sr.is_rename(pair(ISSC, IA), "ISSC", "IA") is True


def test_a_rename_is_recognised_when_the_old_symbol_has_no_row_at_all() -> None:
    """SATS → ECHO. The walk inserts what the vendor lists as active, and SATS
    was delisted on 2026-06-24 — before raw_market.ticker was first written. A
    predecessor link has nothing to link to, so the active half carries it."""
    assert sr.is_rename(pair(ABSENT, (True, "0001415404", "BBG000TGLV00")), "SATS", "ECHO") is True


def test_the_surviving_registrant_renaming_itself_is_a_rename() -> None:
    """EQR → VMRK: Equity Residential is the registrant that became Vivmark."""
    assert sr.is_rename(pair(EQR, VMRK), "EQR", "VMRK") is True


def test_a_merger_is_not_a_rename_even_though_both_symbols_moved() -> None:
    """AVB was merged into EQR's registrant, so its CIK changes and holders got
    2.793 VMRK a share. An option chain does not carry a conversion."""
    assert sr.is_rename(pair(AVB, VMRK), "AVB", "VMRK") is False


# ── the cases that must not trip ──────────────────────────────────────────


def test_two_live_listings_are_never_a_rename() -> None:
    """HONA/HON and Q/DD both read like this: the root belongs to the other one."""
    assert sr.is_rename(pair((True, "1", "F1"), (True, "2", "F2")), "HONA", "HON") is False


def test_a_root_that_is_not_a_ticker_is_never_a_rename() -> None:
    """SPXW and BRKB are option roots, not listings — no row, so no rename."""
    assert sr.is_rename(pair((True, "1", "F1"), ABSENT), "SPX", "SPXW") is False
    assert sr.is_rename(pair((True, "1", "F1"), ABSENT), "BRK.B", "BRKB") is False


def test_a_shared_cik_with_a_different_figi_is_not_enough() -> None:
    assert sr.is_rename(pair((False, "1", "F1"), (True, "1", "F2")), "OLD", "NEW") is False


def test_a_blank_cik_never_matches_itself() -> None:
    assert sr.is_rename(pair((False, "", ""), (True, "", "")), "OLD", "NEW") is False


def test_the_same_symbol_asks_nothing() -> None:
    conn = pair(ISSC, IA)
    assert sr.is_rename(conn, "IA", "IA") is False
    assert conn.queries == [], "a root equal to the label is not a question"


def test_an_unreadable_catalogue_answers_no_and_rolls_back() -> None:
    """The label question must not fail the job, and must not guess either."""
    conn = FakeConn(raises=True)
    assert sr.is_rename(conn, "SATS", "ECHO") is False
    assert conn.rolled_back == 1


def test_no_row_back_is_not_a_rename() -> None:
    assert sr.is_rename(FakeConn([]), "SATS", "ECHO") is False


# ── roots ─────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "ticker,expected",
    [
        ("O:ECHO261016C00055000", "ECHO"),
        ("O:IA261016C00002500", "IA"),
        ("O:SPXW260925P07700000", "SPXW"),
        ("O:BRKB270115C00500000", "BRKB"),
        ("O:VMRK1261016C00120000", "VMRK1"),
        ("ECHO", None),
        ("", None),
        ("O:261016C00055000", None),
    ],
)
def test_option_root(ticker: str, expected: str | None) -> None:
    assert sr.option_root(ticker) == expected


# ── resolution over a response ────────────────────────────────────────────


def test_resolve_moves_the_label_to_the_root_the_contracts_spell() -> None:
    conn = pair(ABSENT, (True, "0001415404", "BBG000TGLV00"))
    assert sr.resolve_storage(conn, "SATS", ["O:ECHO261016C00055000"]) == "ECHO"


def test_resolve_prefers_the_plain_root_over_the_adjusted_one() -> None:
    """A chain that mixes ECHO with ECHO1 files both under ECHO, which is where
    an adjusted series belongs. ECHO1 is not a ticker, so it cannot resolve."""
    conn = pair(ABSENT, (True, "0001415404", "BBG000TGLV00"))
    out = sr.resolve_storage(conn, "SATS", ["O:ECHO1261016C00055000", "O:ECHO261016C00055000"])
    assert out == "ECHO"


def test_resolve_leaves_an_index_root_alone() -> None:
    conn = pair((True, "1", "F1"), ABSENT)
    assert sr.resolve_storage(conn, "SPX", ["O:SPXW260925P07700000"]) == "SPX"


def test_resolve_asks_nothing_when_every_root_matches_the_label() -> None:
    conn = pair(ISSC, IA)
    assert sr.resolve_storage(conn, "AAPL", ["O:AAPL261016C00200000"]) == "AAPL"
    assert conn.queries == []


def test_resolve_on_an_empty_response_is_the_label() -> None:
    conn = pair(ABSENT, (True, "1", "F1"))
    assert sr.resolve_storage(conn, "SATS", []) == "SATS"


def test_resolve_caps_how_many_roots_it_will_ask_about() -> None:
    conn = FakeConn([])
    tickers = [f"O:R{i}261016C00055000" for i in range(12)]
    sr.resolve_storage(conn, "SATS", tickers, max_roots=3)
    assert len(conn.queries) == 3
