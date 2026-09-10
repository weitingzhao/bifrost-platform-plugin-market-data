"""Three axes over the contract table — the endpoint the blueprint exists for.

What matters is not the SQL but four properties: every dataset is measured
against the denominator its contract declares (never one invented here), a
plan boundary is stated rather than reported as a gap, one unreadable dataset
does not sink the page, and the two breadth ratios stay separate.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from bifrost_market_data.api import coverage_dimensions as mod
from bifrost_market_data.contracts import CONTRACTS, BY_DATASET


class _Cur:
    def __init__(self, conn: "_Conn") -> None:
        self.conn = conn
        self._rows: list[tuple[Any, ...]] = []

    def __enter__(self) -> "_Cur":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        q = " ".join(str(sql).split())
        if q.startswith("SET LOCAL"):
            self.conn.timeouts += 1
            return
        self.conn.queries.append(q)
        if "raw_market.ticker WHERE active" in q:
            self._rows = [(s,) for s in self.conn.active]
        elif "research.option_universe" in q:
            self._rows = [(s,) for s in self.conn.universe]
        elif self.conn.raise_on and self.conn.raise_on in q:
            raise RuntimeError("statement timeout")
        elif "count(*)::bigint AS n" in q:
            # Continuity asks for rows per day, not rows per symbol.
            self._rows = list(self.conn.per_day)
        elif q.startswith("SELECT max("):
            self._rows = [(self.conn.newest,)]
        elif q.startswith("SELECT DISTINCT"):
            self._rows = [(s,) for s, _d in self.conn.per_symbol]
        else:
            self._rows = list(self.conn.per_symbol)

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows


class _Conn:
    def __init__(
        self,
        per_symbol: list[tuple[str, date]],
        newest: date,
        raise_on: str | None = None,
        active: list[str] | None = None,
        universe: list[str] | None = None,
        per_day: list[tuple[date, int]] | None = None,
    ) -> None:
        self.per_day = per_day if per_day is not None else [
            (date(2026, 9, 1), 100), (date(2026, 9, 2), 100), (date(2026, 9, 3), 100)
        ]
        self.per_symbol = per_symbol
        self.active = active if active is not None else ["AAPL", "MSFT"]
        self.universe = universe if universe is not None else ["AAPL", "XYZ"]
        self.newest = newest
        self.raise_on = raise_on
        self.queries: list[str] = []
        self.timeouts = 0

    def cursor(self) -> _Cur:
        return _Cur(self)

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


TODAY = date(2026, 9, 9)


@pytest.fixture()
def wired(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    state: dict[str, Any] = {
        "per_symbol": [("AAPL", date(2021, 9, 9)), ("MSFT", date(2026, 6, 9))],
        "newest": date(2026, 9, 8),
        "raise_on": None,
        "universe": [{"symbol": "AAPL", "tier": "resident"}, {"symbol": "XYZ", "tier": "core"}],
        "active": ["AAPL", "MSFT"],
    }
    # The scope helpers are shared now; the fake connection answers them.
    monkeypatch.setattr(mod, "benchmark_scope", lambda conn, benchmarks, **kw: set(benchmarks))

    def fake_connect(**_kw: Any) -> _Conn:
        return _Conn(
            state["per_symbol"],
            state["newest"],
            state["raise_on"],
            state["active"],
            [u["symbol"] for u in state["universe"]],
        )

    monkeypatch.setattr(mod, "connect_db", fake_connect)
    monkeypatch.setattr(mod, "load_research_universe", lambda conn: state["universe"])
    monkeypatch.setattr(mod, "_benchmarks", lambda cfg: ["SPY", "QQQ", "IWM"])
    # The benchmark scope needs the real scheduler block to reach the watchlist;
    # the test supplies it instead of letting the loader read schedule.yaml.
    monkeypatch.setattr(mod, "resolve_scheduler_cfg", lambda: {"iv_radar_benchmarks": ["SPY"]})
    monkeypatch.setattr(mod, "_today", lambda: TODAY)
    mod.CACHE.clear()
    return state


def test_every_contract_is_reported_against_its_declared_denominator(wired: dict[str, Any]) -> None:
    body = mod.get_dimensions(tier=None, refresh=True)["data"]

    assert {d["dataset"] for d in body["datasets"]} == {c.dataset for c in CONTRACTS}
    assert body["denominators"]["whole-market"] == 2
    assert "scopes" not in body["denominators"]  # symbol sets stay server-side
    assert body["denominators"]["universe"]["total"] == 2
    assert body["denominators"]["benchmark-only"] == 3
    by_ds = {d["dataset"]: d for d in body["datasets"]}
    # whole-market measures against the entitlement, universe against the rule.
    assert by_ds["raw_market.stock_daily"]["breadth"]["of"] == 2
    assert by_ds["raw_market.option_daily"]["breadth"]["of"] == 2


def test_the_two_breadth_ratios_stay_apart(wired: dict[str, Any]) -> None:
    body = mod.get_dimensions(tier=None, refresh=True)["data"]
    by_ds = {d["dataset"]: d for d in body["datasets"]}

    universe = by_ds["raw_market.option_daily"]["breadth"]
    # AAPL is in the universe, MSFT is not: held-in-scope is 1 of 2, and the
    # symbol outside the scope is reported rather than inflating the ratio.
    assert universe["held"] == 1 and universe["of"] == 2 and universe["pct"] == 50.0
    assert universe["held_total"] == 2 and universe["outside_scope"] == 1
    # Entitlement utilisation: what the rule asked for ÷ what the plan allows.
    assert universe["entitlement_pct"] == round(100.0 * 2 / 2, 1)
    # A whole-market dataset already is the entitlement; the second ratio is meaningless there.
    assert by_ds["raw_market.stock_daily"]["breadth"]["entitlement_pct"] is None


def test_depth_counts_symbols_at_target_and_names_the_shallowest(wired: dict[str, Any]) -> None:
    body = mod.get_dimensions(tier="whole-market", refresh=True)["data"]
    depth = {d["dataset"]: d["depth"] for d in body["datasets"]}["raw_market.stock_daily"]

    assert depth["measured"] is True
    assert depth["need_days"] == 5 * 365
    # AAPL reaches 2021-09-09 (1,826 days), MSFT only 92.
    assert depth["at_target"] == 1 and depth["of"] == 2
    assert depth["shallowest"] == {"symbol": "MSFT", "days": 92}


def test_a_plan_boundary_is_stated_not_reported_as_a_gap(wired: dict[str, Any]) -> None:
    body = mod.get_dimensions(tier=None, refresh=True)["data"]
    depth = {d["dataset"]: d["depth"] for d in body["datasets"]}

    ratios = depth["raw_market.ratios"]
    assert ratios["measured"] is False
    assert ratios["target"]["kind"] == "forward_only"
    assert "ignores ?date" in ratios["target"]["why"]
    assert depth["raw_market.stock_snapshot"]["target"]["kind"] == "current_only"
    assert depth["raw_market.ticker"]["target"]["kind"] == "catalogue"


def test_one_unreadable_dataset_does_not_sink_the_page(wired: dict[str, Any]) -> None:
    wired["raise_on"] = "raw_market.option_daily"

    body = mod.get_dimensions(tier="universe", refresh=True)["data"]

    by_ds = {d["dataset"]: d for d in body["datasets"]}
    assert by_ds["raw_market.option_daily"]["error"]
    assert by_ds["raw_market.option_daily"]["breadth"]["held"] == 0
    assert by_ds["raw_market.option_snapshot"]["breadth"]["held"] == 1  # its neighbour is fine


def test_the_query_shape_follows_the_cardinality(wired: dict[str, Any]) -> None:
    """A skip scan probes once per distinct value; it wins only when those are
    few. Measured 2026-09-09: 0.85s for option_daily's 60 underlyings against
    152s for stock_daily's 20,695 symbols, where a grouped scan is 24s."""
    assert BY_DATASET["raw_market.option_daily"].low_cardinality is True
    assert BY_DATASET["raw_market.stock_daily"].low_cardinality is False

    conn = _Conn([("AAPL", date(2024, 1, 1))], date(2026, 9, 8))
    mod._per_symbol_oldest(conn, BY_DATASET["raw_market.option_daily"])
    assert "WITH RECURSIVE" in conn.queries[0]

    conn2 = _Conn([("AAPL", date(2024, 1, 1))], date(2026, 9, 8))
    mod._per_symbol_oldest(conn2, BY_DATASET["raw_market.stock_daily"])
    assert "GROUP BY" in conn2.queries[0] and "RECURSIVE" not in conn2.queries[0]


def test_the_answer_is_cached_and_says_how_old_it_is(wired: dict[str, Any]) -> None:
    first = mod.get_dimensions(tier="global", refresh=True)["data"]
    second = mod.get_dimensions(tier="global", refresh=False)["data"]

    assert first["age_sec"] == 0.0
    assert second["generated_at"] == first["generated_at"]
    assert "age_sec" in second
    assert (
        mod.get_dimensions(tier="global", refresh=True)["data"]["generated_at"]
        != first["generated_at"]
    )


def test_an_unknown_tier_is_a_400(wired: dict[str, Any]) -> None:
    with pytest.raises(mod.HTTPException) as err:
        mod.get_dimensions(tier="nope", refresh=True)
    assert err.value.status_code == 400


def test_a_boundary_dataset_is_never_scanned_per_symbol(wired: dict[str, Any]) -> None:
    """Measuring depth only to discard it was most of the cost: short_interest
    alone is 22,932 symbols, and its history can only accumulate forward."""
    conn = _Conn([("AAPL", date(2024, 1, 1))], date(2026, 9, 8))
    scopes = {"whole-market": {"AAPL"}, "universe": set(), "benchmark-only": set(), "global": set()}
    mod._one(BY_DATASET["raw_market.ratios"], {"whole-market": 1, "scopes": scopes}, TODAY, conn)

    # The claim is about the per-symbol scan, not about grouping in general:
    # continuity groups these same rows by day, which is cheap and is the point.
    # The per-symbol scan is the one that takes a min() date per group.
    assert not any("min(" in q and "GROUP BY" in q for q in conn.queries)
    assert any(q.startswith("SELECT DISTINCT") for q in conn.queries)
    assert any("count(*)::bigint AS n" in q for q in conn.queries), "continuity still ran"


def test_a_cold_read_answers_at_once_and_refreshes_behind_itself(
    wired: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Scanning every dataset took 154s against the real tables; the gateway
    gives up at 60. A reader must never wait for that."""
    started: list[str] = []
    monkeypatch.setattr(
        mod.CACHE, "start_refresh", lambda key, compute: (started.append(key), True)[1]
    )
    mod.CACHE.clear()

    body = mod.get_dimensions(tier=None, refresh=False)["data"]

    assert body["computing"] is True
    assert body["datasets"] == []
    assert body["age_sec"] is None
    assert started == ["all"]


def test_a_stale_answer_is_served_while_the_refresh_runs(
    wired: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(mod.CACHE, "start_refresh", lambda key, compute: True)
    fresh = mod.get_dimensions(tier="global", refresh=True)["data"]
    # Age the cache past its TTL without waiting for it.
    at, payload = mod.CACHE._cache["global"]
    mod.CACHE._cache["global"] = (at - mod.TTL_SEC - 1, payload)

    stale = mod.get_dimensions(tier="global", refresh=False)["data"]

    assert stale["generated_at"] == fresh["generated_at"]  # the last good answer, not an empty page
    assert stale["computing"] is True
    assert stale["age_sec"] > mod.TTL_SEC


def test_the_doctor_and_the_contracts_agree_on_every_deadline() -> None:
    """A deadline written in two places is a dataset that reads healthy on one
    panel and stale on the next — the doctor's staleness table is derived."""
    from bifrost_market_data.contracts import deadline_for_dimension, staleness_by_slot
    from bifrost_market_data.doctor import POLICED_SLOTS, STALENESS

    assert set(STALENESS) == set(POLICED_SLOTS)
    for slot, (dimension, hours) in STALENESS.items():
        assert staleness_by_slot()[slot] == (dimension, hours), slot
        assert deadline_for_dimension(dimension) == hours, dimension


def test_every_dataset_declares_the_dimension_that_evidences_it() -> None:
    for c in CONTRACTS:
        assert c.freshness_dimension, c.dataset


def test_freshness_reads_the_cadence_it_was_given() -> None:
    """A deadline in hours only means something for a feed published every session.

    short_interest settles twice a month and FINRA publishes about ten days
    after; measured 2026-09-10 the axis called it 27 days behind a 30-hour
    deadline while the database held every settlement the vendor had released —
    2026-08-14 / 07-31 / 07-15 / 06-30 / 06-15, no gap. The fourth axis learned
    this when cadence was declared; this one had not.
    """
    si = BY_DATASET["raw_market.short_interest"]
    assert si.cadence == "settlement"

    routine = mod._freshness(si, date(2026, 8, 14), date(2026, 9, 10), interval_days=15)
    assert routine["days_behind"] == 27
    assert routine["expected_interval_days"] == 15
    assert routine["overdue"] is False, "27 days is one interval plus the publication lag"

    missed = mod._freshness(si, date(2026, 8, 14), date(2026, 10, 1), interval_days=15)
    assert missed["overdue"] is True, "a whole settlement has now gone missing"


def test_freshness_leaves_a_session_feed_to_its_deadline() -> None:
    """The doctor owns the session verdict; this axis must not answer differently."""
    sd = BY_DATASET["raw_market.stock_daily"]
    assert sd.cadence == "session"
    out = mod._freshness(sd, date(2026, 9, 9), date(2026, 9, 10), interval_days=1)
    assert out["days_behind"] == 1
    assert out["cadence"] == "session"
    assert out["overdue"] is None
    assert out["expected_interval_days"] is None


def test_freshness_without_an_observed_interval_does_not_guess() -> None:
    """A dataset too sparse to measure an interval is not therefore late."""
    si = BY_DATASET["raw_market.short_interest"]
    out = mod._freshness(si, date(2026, 8, 14), date(2026, 12, 1), interval_days=None)
    assert out["overdue"] is None


def test_every_contract_declares_a_grain() -> None:
    """Tier says which instruments; grain says what one row is.

    The console arranges the estate by both, and a hand-kept mapping there would
    drift from this file the first time a dataset is added.
    """
    allowed = {"catalogue", "daily", "snapshot", "minute", "filing"}
    for c in CONTRACTS:
        assert c.grain in allowed, f"{c.dataset} has grain {c.grain!r}"


def test_grain_and_depth_agree_about_catalogues() -> None:
    """A catalogue has no observation date, and nothing else claims to be one."""
    for c in CONTRACTS:
        if c.depth.kind == "catalogue":
            assert c.grain == "catalogue", c.dataset
        if c.grain == "minute":
            assert c.date_column == "bar_time", c.dataset


def test_the_two_axes_are_independent() -> None:
    """Neither is derivable from the other — which is why both are declared.

    option_snapshot and option_daily share a tier and differ in grain;
    stock_daily and option_daily share a grain and differ in tier.
    """
    by = {c.dataset: c for c in CONTRACTS}
    snap, odaily = by["raw_market.option_snapshot"], by["raw_market.option_daily"]
    assert snap.tier == odaily.tier and snap.grain != odaily.grain
    sdaily = by["raw_market.stock_daily"]
    assert sdaily.grain == odaily.grain and sdaily.tier != odaily.tier


def test_a_chain_snapshot_declares_a_boundary_not_a_target() -> None:
    """90 sessions is what trim keeps, not a depth the chain can be made to hold.

    A chain download only ever returns the current session — which is why
    2026-08-11 is permanently absent from option_snapshot — so this depth
    accrues forward and can never be bought. Read as `sessions/90` it measured
    0 of 570 at target with a median of two days and rendered red, which is a
    ramp being reported as a fault.
    """
    for name in ("raw_market.option_snapshot", "raw_market.option_open_interest"):
        c = BY_DATASET[name]
        assert c.depth.kind == "forward_only", name
        assert c.depth.kind in mod.BOUNDARY_KINDS, name
        assert "90" in c.depth.why, f"{name} must still say what trim keeps"


def test_the_boundary_does_not_cost_the_accumulation_its_visibility() -> None:
    """Depth stops measuring them; continuity does not, so a ramp stays legible.

    That is what makes the trade acceptable: the per-symbol median goes, but
    "did the chain land every session" is the question those two datasets are
    actually judged on, and the fourth axis still answers it.
    """
    from bifrost_market_data.continuity import CONTINUITY_KINDS, has_continuity

    assert "forward_only" in CONTINUITY_KINDS
    for name in ("raw_market.option_snapshot", "raw_market.option_open_interest"):
        assert has_continuity(BY_DATASET[name]), name


def test_a_boundary_skips_the_per_symbol_scan() -> None:
    """The scan exists to grade a target. With no target there is nothing to grade.

    option_snapshot alone is 570 underlyings; the dimensions read paid for that
    pass only to discard it.
    """
    for name in ("raw_market.option_snapshot", "raw_market.option_open_interest"):
        c = BY_DATASET[name]
        assert c.symbol_column and c.date_column
        assert c.depth.kind in mod.BOUNDARY_KINDS, "so _one() skips _per_symbol_oldest"


def test_breadth_is_bounded_by_the_session_not_by_max_date() -> None:
    """Two slots write option_snapshot at different scopes on the same day.

    The EOD pipeline covers all 575 underlyings at 22:00 UTC; the intraday chain
    covers the 26-name benchmark union at 14:30. Reading max(date) made breadth
    divide the intraday numerator by the universe denominator for seven and a
    half hours a day — measured 2026-09-10, the same endpoint on unchanged data
    reported 99.1% at 05:22 and 4.5% at 16:14.
    """
    c = BY_DATASET["raw_market.option_snapshot"]
    assert c.breadth_window == "session"

    seen: list[str] = []

    class _Cur:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return None

        def execute(self, sql, params=None):
            q = " ".join(str(sql).split())
            if not q.startswith("SET LOCAL"):
                seen.append(q)

        def fetchall(self):
            return []

    class _Conn:
        def cursor(self):
            return _Cur()

    mod._held_symbols(_Conn(), c, date(2026, 9, 9))
    q = seen[-1]
    assert "<= %s" in q, "the newest delivery no later than the session"
    assert "max(" in q, "still the newest one, not every row on that day"


def test_a_dataset_that_is_behind_keeps_its_breadth() -> None:
    """Lateness is freshness's answer, not breadth's.

    treasury_yield was two days back on 2026-09-10. Filtering *to* the session
    rather than bounding *by* it would read nothing held and paint red for
    something freshness already says. The shape of the predicate is what
    guarantees it: max(date <= session), never date = session.
    """
    c = BY_DATASET["raw_market.option_daily"]
    seen: list[str] = []

    class _Cur:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return None

        def execute(self, sql, params=None):
            q = " ".join(str(sql).split())
            if not q.startswith("SET LOCAL"):
                seen.append(q)

        def fetchall(self):
            return []

    class _Conn:
        def cursor(self):
            return _Cur()

    mod._held_symbols(_Conn(), c, date(2026, 9, 9))
    assert "= (SELECT max(" in seen[-1], "bounded, not filtered"


def test_an_unresolved_session_falls_back_rather_than_reporting_nothing() -> None:
    """None means "do not bound", not "hold nothing"."""
    c = BY_DATASET["raw_market.option_snapshot"]
    seen: list[str] = []

    class _Cur:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return None

        def execute(self, sql, params=None):
            q = " ".join(str(sql).split())
            if not q.startswith("SET LOCAL"):
                seen.append(q)

        def fetchall(self):
            return []

    class _Conn:
        def cursor(self):
            return _Cur()

    mod._held_symbols(_Conn(), c, None)
    assert "<= %s" not in seen[-1]
    assert "max(" in seen[-1]
