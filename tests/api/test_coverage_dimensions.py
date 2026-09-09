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
            self._rows = [(5317,)]
        elif q.startswith("SELECT max("):
            self._rows = [(self.conn.newest,)]
        elif "count(DISTINCT" in q:
            self._rows = [(len(self.conn.per_symbol),)]
        elif self.conn.raise_on and self.conn.raise_on in q:
            raise RuntimeError("statement timeout")
        else:
            self._rows = list(self.conn.per_symbol)

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows


class _Conn:
    def __init__(
        self, per_symbol: list[tuple[str, date]], newest: date, raise_on: str | None = None
    ) -> None:
        self.per_symbol = per_symbol
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
    }

    def fake_connect(**_kw: Any) -> _Conn:
        return _Conn(state["per_symbol"], state["newest"], state["raise_on"])

    monkeypatch.setattr(mod, "connect_db", fake_connect)
    monkeypatch.setattr(mod, "load_research_universe", lambda conn: state["universe"])
    monkeypatch.setattr(mod, "_benchmarks", lambda: ["SPY", "QQQ", "IWM"])
    monkeypatch.setattr(mod, "_today", lambda: TODAY)
    mod._CACHE.clear()
    return state


def test_every_contract_is_reported_against_its_declared_denominator(wired: dict[str, Any]) -> None:
    body = mod.get_dimensions(tier=None, refresh=True)["data"]

    assert {d["dataset"] for d in body["datasets"]} == {c.dataset for c in CONTRACTS}
    assert body["denominators"]["whole-market"] == 5317
    assert body["denominators"]["universe"]["total"] == 2
    assert body["denominators"]["benchmark-only"] == 3
    by_ds = {d["dataset"]: d for d in body["datasets"]}
    # whole-market measures against the entitlement, universe against the rule.
    assert by_ds["raw_market.stock_daily"]["breadth"]["of"] == 5317
    assert by_ds["raw_market.option_daily"]["breadth"]["of"] == 2


def test_the_two_breadth_ratios_stay_apart(wired: dict[str, Any]) -> None:
    body = mod.get_dimensions(tier=None, refresh=True)["data"]
    by_ds = {d["dataset"]: d for d in body["datasets"]}

    universe = by_ds["raw_market.option_daily"]["breadth"]
    # Intent fulfilment: held ÷ what the rule asked for.
    assert universe["pct"] == 100.0
    # Entitlement utilisation: what the rule asked for ÷ what the plan allows.
    assert universe["entitlement_pct"] == round(100.0 * 2 / 5317, 1)
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
    assert by_ds["raw_market.option_snapshot"]["breadth"]["held"] == 2  # its neighbour is fine


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
