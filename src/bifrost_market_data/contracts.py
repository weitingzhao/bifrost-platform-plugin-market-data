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
    #: How often a row set is published. "session" means every trading day, and
    #: only those are compared against the trading calendar: short_interest
    #: settles twice a month, so measured against sessions it read as 56 missing
    #: days on the first live read of the continuity axis. A cadence is declared,
    #: never inferred from the data it is meant to judge.
    cadence: str = "session"

    #: The ops_jobs.ingest_freshness dimension that evidences this dataset,
    #: declared here so the doctor's staleness table and the quality gate stop
    #: keeping their own copies of the mapping.
    freshness_dimension: str | None = None


# Rolling windows the subscriptions allow (subscription.py SUBSCRIPTIONS).
STOCK_WINDOW_DAYS = 5 * 365
OPTION_WINDOW_DAYS = 2 * 365
FINANCIALS_SINCE = "2009-01-01"

# The Research universe's per-tier history requirement (research.option_universe).
UNIVERSE_MONTHS = {"resident": 24, "core": 24, "edge": 12}

#: How far back the vendor serves whole-market short volume by date. Measured
#: 2026-09-09: 2024-09-16 returns that day's rows, 2023-09-15 returns none.
SHORT_VOLUME_WINDOW_DAYS = 2 * 365

#: Below this many symbols, the whole-market grouped pull did not happen for the
#: session — whatever the job status says. One number because there were two:
#: the doctor asked for 12,000 rows and the quality gate for 4,000 symbols, of
#: the same session-scoped count, so the same table could pass one and fail the
#: other. Measured on 19 sessions from 2026-08-12 to 2026-09-08: 12,396 low,
#: 12,576 high, and a failed session on 2026-08-11 wrote 18.
STOCK_DAILY_MIN_SESSION_SYMBOLS = 12000

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
        freshness_dimension="stock_daily",
    ),
    DatasetContract(
        "raw_market.stock_snapshot",
        "whole-market",
        DepthTarget("current_only", why="point-in-time; the vendor has no history for it"),
        1.0,
        ("stock-snapshot",),
        "symbol",
        "session_date",
        freshness_dimension="stock_snapshot",
    ),
    DatasetContract(
        "raw_market.stock_movers",
        "whole-market",
        DepthTarget("current_only", why="point-in-time"),
        1.0,
        ("stock-movers",),
        "symbol",
        "session_date",
        freshness_dimension="stock_movers",
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
        freshness_dimension="ticker_sync",
    ),
    DatasetContract(
        "raw_market.corporate_action",
        "whole-market",
        DepthTarget(
            "catalogue", why="the slot's window looks forward (-7 / +60 days); it is not a history"
        ),
        # Dividends and splits are sparse: a week without one is normal, and the
        # doctor has always allowed that. 48h would flag the calendar, not the feed.
        168.0,
        ("corporate",),
        "symbol",
        "ex_date",
        breadth_window="ever",
        freshness_dimension="dividends",
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
        freshness_dimension="financials",
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
        freshness_dimension="financials",
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
        freshness_dimension="financials",
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
        freshness_dimension="ratios",
    ),
    DatasetContract(
        "raw_market.short_volume",
        "whole-market",
        # Not forward_only. That is ratios' constraint — its endpoint ignores
        # ?date and always returns the latest — and it was copied here without
        # being checked. Measured 2026-09-09: short volume for 2026-06-15 comes
        # back dated 2026-06-15, so its history is bought, not merely accrued.
        DepthTarget(
            "rolling_days",
            SHORT_VOLUME_WINDOW_DAYS,
            "vendor serves ?date about two years back; 2023-09-15 returns nothing",
        ),
        30.0,
        ("fundamentals-market",),
        "symbol",
        "period_date",
        freshness_dimension="short_volume",
    ),
    DatasetContract(
        "raw_market.short_interest",
        "whole-market",
        DepthTarget(
            "forward_only", why="published per settlement; the lookback is 45 days, not a history"
        ),
        30.0,
        ("fundamentals-market",),
        "symbol",
        "period_date",
        breadth_window="ever",
        # Twice a month, on settlement dates. Measured 2026-09-09: median gap
        # 15 days over the last 120, against 1 day for ratios and short_volume.
        cadence="settlement",
        freshness_dimension="short_interest",
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
        freshness_dimension="treasury_yields",
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
        freshness_dimension="calendar",
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
        freshness_dimension="option_snapshot",
    ),
    DatasetContract(
        "raw_market.option_contract",
        "universe",
        DepthTarget(
            "catalogue", why="contracts alive now, plus expired ones the vendor still lists"
        ),
        12.0,
        ("option-refresh",),
        "underlying",
        None,
        low_cardinality=True,
        breadth_window="ever",
        freshness_dimension="option_contract",
    ),
    DatasetContract(
        "raw_market.option_open_interest",
        "universe",
        DepthTarget("sessions", 90, "derived from the chain snapshot, same retention"),
        2.0,
        ("eod-pipeline",),
        "underlying",
        "trade_date",
        freshness_dimension="option_open_interest",
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
        freshness_dimension="option_daily",
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
        freshness_dimension="stock_minute",
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
        freshness_dimension="option_minute",
    ),
)

BY_DATASET: dict[str, DatasetContract] = {c.dataset: c for c in CONTRACTS}


def contract_for(dataset: str) -> DatasetContract:
    """The contract, or KeyError — a dataset with no contract should not be collected."""
    return BY_DATASET[dataset]


def datasets_in_tier(tier: Tier) -> tuple[DatasetContract, ...]:
    return tuple(c for c in CONTRACTS if c.tier == tier)


def deadline_for_dimension(dimension: str) -> float | None:
    """Hours after the session close by which this freshness dimension is due.

    The tightest contract wins when several datasets share a dimension — the
    three statement tables all land from one financials job, and a lag that is
    late for any of them is late.
    """
    hours = [c.freshness_hours for c in CONTRACTS if c.freshness_dimension == dimension]
    return min(hours) if hours else None


def staleness_by_slot() -> dict[str, tuple[str, float]]:
    """slot → (freshness dimension, deadline hours), derived rather than kept.

    The doctor used to hold its own copy of this table; a dataset's deadline
    now has one home.
    """
    out: dict[str, tuple[str, float]] = {}
    for c in CONTRACTS:
        if not c.freshness_dimension:
            continue
        for slot in c.slots:
            current = out.get(slot)
            if current is None or c.freshness_hours < current[1]:
                out[slot] = (c.freshness_dimension, c.freshness_hours)
    return out


__all__ = [
    "CONTRACTS",
    "BY_DATASET",
    "DatasetContract",
    "DepthTarget",
    "Tier",
    "UNIVERSE_MONTHS",
    "contract_for",
    "datasets_in_tier",
    "deadline_for_dimension",
    "staleness_by_slot",
]
