"""The matrix's memory: a row per change, never a row per compute."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from bifrost_market_data import coverage_history as ch

T0 = datetime(2026, 9, 10, 22, 0, tzinfo=timezone.utc)


class _Cur:
    """Just enough of ops_jobs.coverage_sample to exercise the write path."""

    def __init__(self, db: "_Conn") -> None:
        self.db = db
        self.rows: list[tuple[Any, ...]] = []
        self.rowcount = 0

    def __enter__(self) -> "_Cur":
        return self

    def __exit__(self, *_a: Any) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        s = " ".join(sql.split())
        self.db.sql.append(s)
        if self.db.boom:
            raise RuntimeError("boom")
        if s.startswith("SELECT coverage_sample_id, first_seen_at, last_seen_at, digest"):
            self.rows = [
                (r["id"], r["first"], r["last"], r["digest"], r["verdicts"])
                for r in self.db.newest_first()[:2]
            ]
        elif s.startswith("SELECT coverage_sample_id, first_seen_at, last_seen_at, verdicts"):
            self.rows = [
                (r["id"], r["first"], r["last"], r["verdicts"]) for r in self.db.newest_first()
            ]
        elif s.startswith("UPDATE"):
            for r in self.db.table:
                if r["id"] == params[0]:
                    r["last"] = self.db.now
            self.rows = []
        elif s.startswith("INSERT"):
            self.db.seq += 1
            self.db.table.append(
                {
                    "id": self.db.seq,
                    "first": self.db.now,
                    "last": self.db.now,
                    "digest": params[0],
                    "verdicts": params[1],
                }
            )
            self.rows = [(self.db.now,)]
        elif s.startswith("SELECT count(*)"):
            self.rows = [(len(self.db.table),)]
        elif s.startswith("DELETE"):
            head = self.db.newest_first()[0]["id"] if self.db.table else None
            cutoff = self.db.now - timedelta(days=int(params[0]))
            keep = [r for r in self.db.table if r["last"] >= cutoff or r["id"] == head]
            self.rowcount = len(self.db.table) - len(keep)
            self.db.table = keep
            self.rows = []

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows


class _Conn:
    def __init__(self) -> None:
        self.table: list[dict[str, Any]] = []
        self.seq = 0
        self.now = T0
        self.sql: list[str] = []
        self.commits = 0
        self.rollbacks = 0
        self.boom = False

    def newest_first(self) -> list[dict[str, Any]]:
        return sorted(self.table, key=lambda r: (r["first"], r["id"]), reverse=True)

    def cursor(self) -> _Cur:
        return _Cur(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


MAP_A = {"raw_market.stock_daily": {"breadth": "ok", "depth": "ok"}}
MAP_B = {"raw_market.stock_daily": {"breadth": "thin", "depth": "ok"}}


def test_the_first_sample_reports_no_changes_but_says_it_is_the_first() -> None:
    """"Nothing changed" and "nothing to compare against" are different claims."""
    conn = _Conn()
    out = ch.record(conn, MAP_A)
    assert out["recorded"] is True
    assert out["changes"] == []
    assert out["previous_at"] is None
    assert out["changed_at"] == T0.isoformat()
    assert len(conn.table) == 1


def test_an_unchanged_compute_writes_no_row() -> None:
    """The page recomputes on a timer; "nothing happened" is not history."""
    conn = _Conn()
    ch.record(conn, MAP_A)
    conn.now = T0 + timedelta(hours=1)
    out = ch.record(conn, MAP_A)
    assert len(conn.table) == 1
    assert conn.table[0]["last"] == T0 + timedelta(hours=1)  # seen again
    assert out["changes"] == []


def test_a_changed_verdict_writes_a_row_and_names_what_moved() -> None:
    conn = _Conn()
    ch.record(conn, MAP_A)
    conn.now = T0 + timedelta(days=1)
    out = ch.record(conn, MAP_B)
    assert len(conn.table) == 2
    assert out["changes"] == [
        {
            "dataset": "raw_market.stock_daily",
            "axis": "breadth",
            "from": "ok",
            "to": "thin",
            "direction": "regressed",
        }
    ]
    assert out["changed_at"] == (T0 + timedelta(days=1)).isoformat()
    assert out["previous_at"] == T0.isoformat()


def test_the_change_stays_visible_after_the_verdicts_settle() -> None:
    """A regression must not vanish from the page on the next recompute.

    The diff is always between the two newest *distinct* maps, so it keeps
    describing when today's state began — the whole reason a still frame could
    not say "this got worse".
    """
    conn = _Conn()
    ch.record(conn, MAP_A)
    conn.now = T0 + timedelta(days=1)
    ch.record(conn, MAP_B)
    conn.now = T0 + timedelta(days=1, hours=6)
    out = ch.record(conn, MAP_B)
    assert len(conn.table) == 2
    assert [c["axis"] for c in out["changes"]] == ["breadth"]
    assert out["changed_at"] == (T0 + timedelta(days=1)).isoformat()


def test_key_order_is_not_a_change() -> None:
    conn = _Conn()
    ch.record(conn, {"a": {"breadth": "ok", "depth": "ok"}})
    ch.record(conn, {"a": {"depth": "ok", "breadth": "ok"}})
    assert len(conn.table) == 1


def test_a_failed_write_says_so_rather_than_showing_an_empty_diff() -> None:
    """An empty `changes` on a broken write would read as "nothing changed"."""
    conn = _Conn()
    conn.boom = True
    out = ch.record(conn, MAP_A)
    assert out["recorded"] is False
    assert conn.rollbacks == 1


def test_an_empty_map_is_never_recorded() -> None:
    """A compute that read nothing must not overwrite the last real reading."""
    conn = _Conn()
    assert ch.record(conn, {})["recorded"] is False
    assert conn.table == []


def test_trim_keeps_the_newest_row_however_quiet_the_quarter() -> None:
    """Retention bounds the history, not the present."""
    conn = _Conn()
    ch.record(conn, MAP_A)
    conn.now = T0 + timedelta(days=400)
    dropped = ch.trim_samples(conn, keep_days=180)
    assert dropped == 0
    assert len(conn.table) == 1


def test_trim_drops_changes_past_the_window() -> None:
    conn = _Conn()
    ch.record(conn, MAP_A)
    conn.now = T0 + timedelta(days=300)
    ch.record(conn, MAP_B)
    assert ch.trim_samples(conn, keep_days=180) == 1
    assert [r["digest"] for r in conn.table] == [ch.digest(MAP_B)]


def test_history_reads_back_what_was_stored() -> None:
    conn = _Conn()
    ch.record(conn, MAP_A)
    rows = ch.history(conn)
    assert len(rows) == 1
    assert rows[0]["verdicts"] == MAP_A


def test_history_survives_a_json_string_column() -> None:
    """psycopg may hand back text where jsonb was written."""
    conn = _Conn()
    ch.record(conn, MAP_A)
    conn.table[0]["verdicts"] = json.dumps(MAP_A)
    assert ch.history(conn)[0]["verdicts"] == MAP_A


MAP_ERR = {"raw_market.stock_daily": {"breadth": "unknown", "depth": "unknown"}}


def test_a_dataset_that_could_not_be_read_keeps_its_last_verdicts() -> None:
    """A failed read is not a reading.

    short_volume timed out on the first recorded compute and came back unknown
    on all four axes — nothing about the data had moved. Recorded as-is, a
    dataset that times out now and then writes two rows per flap and reports
    "1 changed" on a page whose job is to make a real regression stand out.
    """
    conn = _Conn()
    ch.record(conn, MAP_A)
    conn.now = T0 + timedelta(hours=1)
    out = ch.record(conn, MAP_ERR, unread={"raw_market.stock_daily"})
    assert len(conn.table) == 1  # no new row: the fingerprint did not move
    assert out["changes"] == []
    assert out["carried_forward"] == ["raw_market.stock_daily"]
    assert ch.history(conn)[0]["verdicts"] == MAP_A


def test_a_real_change_still_lands_while_another_dataset_is_unread() -> None:
    conn = _Conn()
    ch.record(conn, {**MAP_A, "b": {"breadth": "ok"}})
    conn.now = T0 + timedelta(hours=1)
    out = ch.record(
        conn,
        {**MAP_ERR, "b": {"breadth": "thin"}},
        unread={"raw_market.stock_daily"},
    )
    assert len(conn.table) == 2
    assert [(c["dataset"], c["from"], c["to"]) for c in out["changes"]] == [("b", "ok", "thin")]


def test_the_first_sample_records_the_unknowns_it_has() -> None:
    """There is no last-known verdict to stand in for them."""
    conn = _Conn()
    out = ch.record(conn, MAP_ERR, unread={"raw_market.stock_daily"})
    assert ch.history(conn)[0]["verdicts"] == MAP_ERR
    assert out["carried_forward"] == ["raw_market.stock_daily"]
