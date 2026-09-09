"""The dataset contract table — the blueprint's core artifact, as code.

Every dataset declares its three targets here, and everything downstream reads
them from here. That single sentence is the whole point: the calibration of
2026-09-09 found four different answers to "how many symbols should we have",
four definitions of "the current session", and four staleness thresholds — one
dataset could be healthy on one panel and stale on another. A denominator that
lives in a panel is a denominator nobody agreed to.

Breadth is tiered by collection cost, which is what the system already did
without saying so: endpoints that return the whole market in one call pull the
entitlement; endpoints that must be called per symbol follow the Research
universe rule.

See ``docs/MASSIVE_BLUEPRINT.md`` §3. Contracts C-B1, C-D1, C-F2, C-G1.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Tier = Literal["whole-market", "universe", "benchmark-only", "global"]
#: "session" — instruments present in the newest observation, which is what a
#: daily feed should cover. "ever" — instruments ever seen, which is the honest
#: numerator for a dataset that accumulates (a company files quarterly, not
#: daily). Getting this wrong reads as 389% coverage: five years of symbols,
#: including delisted ones, over a denominator of today's active tickers.
BreadthWindow = Literal["session", "ever"]
DepthKind = Literal[
    "rolling_days", "since", "sessions", "current_only", "forward_only", "catalogue"
]


@dataclass(frozen=True)
class DepthTarget:
    """How far back this dataset should reach, and why that is the number.

    ``forward_only`` and ``catalogue`` are not weaker forms of depth — they are
    the plan boundaries C-D3 requires to be stated rather than displayed as a
    gap. The ratios endpoint ignores its ``date`` parameter and always answers
    with the latest values, so that history can only accumulate forward; a
    catalogue holds what is live, not a series.
    """

    kind: DepthKind
    #: rolling_days → days; sessions → sessions; since → ISO date. Unused otherwise.
    value: int | str | None = None
    why: str = ""


@dataclass(frozen=True)
class DatasetContract:
    dataset: str
    tier: Tier
    depth: DepthTarget
    #: Hours after the session's close by which this dataset should have landed.
    freshness_hours: float
    slots: tuple[str, ...]
    #: Column naming the instrument, or None for a single global series.
    symbol_column: str | None
    #: Column carrying the observation date, or None for a catalogue.
    date_column: str | None
    #: Distinct instruments are few enough that a per-instrument probe beats a
    #: scan. Measured 2026-09-09: option_daily (60 distinct) 0.85s by skip scan
    #: against 152s for stock_daily (20,695 distinct), where a plain GROUP BY
    #: is 24s. The shape follows the cardinality, not a preference.
    low_cardinality: bool = False
    breadth_window: BreadthWindow = "session"


# Rolling windows the subscriptions allow (subscription.py SUBSCRIPTIONS).
STOCK_WINDOW_DAYS = 5 * 365
OPTION_WINDOW_DAYS = 2 * 365
FINANCIALS_SINCE = "2009-01-01"

# The Research universe's per-tier history requirement (research.option_universe).
UNIVERSE_MONTHS = {"resident": 24, "core": 24, "edge": 12}

CONTRACTS: tuple[DatasetContract, ...] = (
    # ── whole-market: one call covers everyone, so pull the entitlement ──
    DatasetContract(
        "raw_market.stock_daily",
        "whole-market",
        DepthTarget("rolling_days", STOCK_WINDOW_DAYS, "Stocks Starter: rolling 5 years"),
        2.0,
        ("universe-daily", "stock-eod"),
        "symbol",
        "bar_date",
    ),
    DatasetContract(
        "raw_market.stock_snapshot",
        "whole-market",
        DepthTarget("current_only", why="point-in-time; the vendor has no history for it"),
        1.0,
        ("stock-snapshot",),
        "symbol",
        "session_date",
    ),
    DatasetContract(
        "raw_market.stock_movers",
        "whole-market",
        DepthTarget("current_only", why="point-in-time"),
        1.0,
        ("stock-movers",),
        "symbol",
        "session_date",
    ),
    DatasetContract(
        "raw_market.ticker",
        "whole-market",
        DepthTarget("catalogue", why="what is listed now, with active flags"),
        48.0,
        ("reference",),
        "symbol",
        None,
        breadth_window="ever",
    ),
    DatasetContract(
        "raw_market.corporate_action",
        "whole-market",
        DepthTarget("catalogue", why="the slot's window looks forward (-7 / +60 days); it is not a history"),
        48.0,
        ("corporate",),
        "symbol",
        "ex_date",
        breadth_window="ever",
    ),
    DatasetContract(
        "raw_market.income_statement",
        "whole-market",
        DepthTarget("since", FINANCIALS_SINCE, "Financials & Ratios: statements from 2009"),
        48.0,
        ("fundamentals-rotate",),
        "symbol",
        "period_date",
        breadth_window="ever",
    ),
    DatasetContract(
        "raw_market.balance_sheet",
        "whole-market",
        DepthTarget("since", FINANCIALS_SINCE, "Financials & Ratios: statements from 2009"),
        48.0,
        ("fundamentals-rotate",),
        "symbol",
        "period_date",
        breadth_window="ever",
    ),
    DatasetContract(
        "raw_market.cash_flow",
        "whole-market",
        DepthTarget("since", FINANCIALS_SINCE, "Financials & Ratios: statements from 2009"),
        48.0,
        ("fundamentals-rotate",),
        "symbol",
        "period_date",
        breadth_window="ever",
    ),
    DatasetContract(
        "raw_market.ratios",
        "whole-market",
        DepthTarget(
            "forward_only",
            why="the vendor ignores ?date and returns the latest; history only accumulates",
        ),
        30.0,
        ("fundamentals-market",),
        "symbol",
        "period_date",
    ),
    DatasetContract(
        "raw_market.short_volume",
        "whole-market",
        DepthTarget("forward_only", why="published the morning after the session"),
        30.0,
        ("fundamentals-market",),
        "symbol",
        "period_date",
    ),
    DatasetContract(
        "raw_market.short_interest",
        "whole-market",
        DepthTarget("forward_only", why="published per settlement; the lookback is 45 days, not a history"),
        30.0,
        ("fundamentals-market",),
        "symbol",
        "period_date",
        breadth_window="ever",
    ),
    # ── global: one series, no instruments ──
    DatasetContract(
        "raw_market.treasury_yield",
        "global",
        DepthTarget("rolling_days", 30, "the slot's lookback"),
        24.0,
        ("treasury",),
        None,
        "yield_date",
    ),
    DatasetContract(
        "raw_market.us_market_holiday",
        "global",
        DepthTarget("catalogue", why="the vendor's forward calendar"),
        48.0,
        ("calendar",),
        None,
        None,
        breadth_window="ever",
    ),
    # ── universe: per-symbol calls, so follow the Research rule ──
    DatasetContract(
        "raw_market.option_snapshot",
        "universe",
        DepthTarget("sessions", 90, "trim keeps 90 sessions of EOD chains"),
        2.0,
        ("eod-pipeline", "intraday-chain"),
        "underlying",
        "snapshot_ts",
        low_cardinality=True,
    ),
    DatasetContract(
        "raw_market.option_contract",
        "universe",
        DepthTarget("catalogue", why="contracts alive now, plus expired ones the vendor still lists"),
        12.0,
        ("option-refresh",),
        "underlying",
        None,
        low_cardinality=True,
        breadth_window="ever",
    ),
    DatasetContract(
        "raw_market.option_open_interest",
        "universe",
        DepthTarget("sessions", 90, "derived from the chain snapshot, same retention"),
        2.0,
        ("eod-pipeline",),
        "underlying",
        "trade_date",
    ),
    DatasetContract(
        "raw_market.option_daily",
        "universe",
        DepthTarget(
            "rolling_days",
            OPTION_WINDOW_DAYS,
            "Options Starter: rolling 2 years; resident/core 24 months, edge 12",
        ),
        24.0,
        ("option-bars", "option-backfill"),
        "underlying",
        "bar_date",
        low_cardinality=True,
    ),
    # ── benchmark-only: too big to be worth more than a few names ──
    DatasetContract(
        "raw_market.stock_minute",
        "benchmark-only",
        DepthTarget("rolling_days", 365, "12 months of intraday for the benchmarks"),
        24.0,
        ("minute-bars",),
        "symbol",
        "bar_time",
        low_cardinality=True,
    ),
    DatasetContract(
        "raw_market.option_minute",
        "benchmark-only",
        DepthTarget("rolling_days", 365, "12 months of intraday for the benchmarks"),
        24.0,
        ("minute-bars",),
        "underlying",
        "bar_time",
        low_cardinality=True,
    ),
)

BY_DATASET: dict[str, DatasetContract] = {c.dataset: c for c in CONTRACTS}


def contract_for(dataset: str) -> DatasetContract:
    """The contract, or KeyError — a dataset with no contract should not be collected."""
    return BY_DATASET[dataset]


def datasets_in_tier(tier: Tier) -> tuple[DatasetContract, ...]:
    return tuple(c for c in CONTRACTS if c.tier == tier)


__all__ = [
    "CONTRACTS",
    "BY_DATASET",
    "DatasetContract",
    "DepthTarget",
    "Tier",
    "UNIVERSE_MONTHS",
    "contract_for",
    "datasets_in_tier",
]
