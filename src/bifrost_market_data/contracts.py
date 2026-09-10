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

from dataclasses import dataclass, field
from typing import Literal

Tier = Literal["whole-market", "universe", "benchmark-only", "global"]
#: "session" — instruments present in the newest observation, which is what a
#: daily feed should cover. "ever" — instruments ever seen, which is the honest
#: numerator for a dataset that accumulates (a company files quarterly, not
#: daily). Getting this wrong reads as 389% coverage: five years of symbols,
#: including delisted ones, over a denominator of today's active tickers.
BreadthWindow = Literal["session", "ever"]

#: What one row of a dataset represents. See DatasetContract.grain.
Grain = Literal["catalogue", "daily", "snapshot", "minute", "filing"]

#: How often a row set is published. See DatasetContract.cadence.
Cadence = Literal["session", "settlement", "filing"]
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
    #: Where a forward-only depth is climbing to, when anything caps it. A chain
    #: snapshot cannot be backfilled, so its depth is a boundary — but it is
    #: still accruing toward the ninety sessions trim keeps, and "36 of 90" is
    #: worth more than a blank square. None means it accrues without a ceiling.
    accrues_to_sessions: int | None = None


@dataclass(frozen=True)
class Refill:
    """How a missing session is repaired, or why it needs no repairing.

    ``backfill_slot`` answered this with a slot name or None, and None was doing
    three jobs at once: gone for good, repairs itself, and not a session series.
    The console's agent brief read the first meaning for all three and told a
    reader that treasury_yield's missed sessions were unrecoverable when the
    slot re-pulls thirty days on its next run.

    ``slot``           enqueue that schedule slot for the date
    ``kind``           enqueue that one job kind — the slot would do more
    ``lookback``       nothing to do; the slot's own window repairs it
    ``unrecoverable``  the vendor cannot serve that date again
    """

    how: Literal["slot", "kind", "lookback", "unrecoverable"]
    #: Slot or job kind, depending on ``how``. Unused for the other two.
    target: str | None = None
    #: How far the slot reaches back on each run, where that is what repairs it.
    lookback_days: int | None = None
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
    #: ``filing`` is the third: a company files when it files, and there is no
    #: interval to measure either — period_date follows each company's own
    #: fiscal calendar, so the dataset's newest row only tells you who filed
    #: most recently. Judged against a 48-hour deadline it read 39 days late
    #: while nothing was wrong. Whether the collector is still running is the
    #: ingest_freshness question, not this one.
    cadence: Cadence = "session"

    #: Why breadth against this tier's scope is not a fair question. Absence
    #: means it is. A top-N list is not partial coverage of the market, and a
    #: catalogue of events that happened is not partial coverage of the
    #: instruments they could have happened to — both rendered red at 0.4% and
    #: 14.2% on 2026-09-10 with nothing wrong.
    breadth_unjudged: str | None = None

    #: The ops_jobs.ingest_freshness dimension that evidences this dataset,
    #: declared here so the doctor's staleness table and the quality gate stop
    #: keeping their own copies of the mapping.
    freshness_dimension: str | None = None

    #: What one row *is*. Declared, not derived: the console groups the contract
    #: table by tier and grain so a reader can see the whole estate at a glance,
    #: and a hand-kept mapping in the console would drift from this file the
    #: first time a dataset is added.
    #:
    #: ``catalogue``  a list of things that exist; no observation date
    #: ``daily``      one row per instrument per session
    #: ``snapshot``   the state of something at one instant
    #: ``minute``     intraday bars
    #: ``filing``     published per report or settlement, not per session
    grain: Grain = "daily"

    #: How a missed session is repaired — declared, because the three reasons
    #: there might be nothing to prescribe are not the same reason and a reader
    #: acts differently on each. See ``Refill``.
    refill: Refill = field(default_factory=lambda: Refill("unrecoverable"))


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
        # The grouped whole-market pull, not the watchlist one: a blank session
        # is blank for all 5,317 names, and seven of them went unnoticed for
        # ninety days before the fourth axis existed.
        grain="daily",
        refill=Refill("slot", "universe-daily", why="the grouped whole-market pull; a blank session is blank for all 5,317"),
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
        grain="snapshot",
        refill=Refill("unrecoverable", why="an all-tickers snapshot is the market as it is now; yesterday's cannot be asked for"),
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
        grain="snapshot",
        refill=Refill("unrecoverable", why="a top-N list of the session that is running; not servable for a past date"),
        breadth_unjudged="a top-N list of the session's biggest moves; holding 22 of them is the whole list, not 0.4% of the market",
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
        grain="catalogue",
        refill=Refill("lookback", "reference", why="the whole-market ticker sync runs nightly; a catalogue has no session to miss"),
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
        grain="catalogue",
        refill=Refill("lookback", "corporate", lookback_days=7, why="the slot re-pulls a 7-day window on every run"),
        breadth_unjudged="events that happened, not instruments to cover; most tickers have no split or dividend in the window",
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
        grain="filing",
        refill=Refill("lookback", "fundamentals-rotate", why="the slot walks the whole CS universe every day, so a missed filing lands on the next run"),
        cadence="filing",
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
        grain="filing",
        refill=Refill("lookback", "fundamentals-rotate", why="the slot walks the whole CS universe every day, so a missed filing lands on the next run"),
        cadence="filing",
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
        grain="filing",
        refill=Refill("lookback", "fundamentals-rotate", why="the slot walks the whole CS universe every day, so a missed filing lands on the next run"),
        cadence="filing",
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
        grain="daily",
        refill=Refill("unrecoverable", why="the endpoint ignores ?date and always answers with the latest values, so this history only accrues"),
        # The vendor's ratio population and our active-ticker list are two
        # different sets that happen to overlap. Measured 2026-09-10: each pull
        # takes 6 pages and 5,010 rows with truncated=False — everything the
        # vendor gives — landing 4,791 distinct symbols, of which 816 are not on
        # our active list at all, while 1,342 of ours get no ratio computed. The
        # program doc says the same in words: the shortfall is micro caps and
        # recent listings the vendor computes no ratios for, not a bug.
        breadth_unjudged=(
            "the vendor computes ratios for its own ~5,000-symbol set, which "
            "overlaps this tier rather than covering it; every row it offers is "
            "collected"
        ),
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
        # The slot also fires ratios_market (whose endpoint ignores the date)
        # and short_interest_market (whose 45-day lookback already covers it).
        # Two wasted jobs per repaired session, knowingly: holes are rare, and
        # the alternative is a second prescription vocabulary.
        grain="daily",
        refill=Refill("kind", "short_volume_market", why="the slot would also fire ratios_market, whose endpoint ignores the date, and short_interest_market, whose 45-day lookback already covers it"),
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
        grain="filing",
        refill=Refill("lookback", "fundamentals-market", lookback_days=45, why="the slot re-pulls 45 days of settlements on every run"),
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
        grain="daily",
        refill=Refill("lookback", "treasury", lookback_days=30, why="the slot re-pulls 30 days on every run"),
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
        grain="catalogue",
        refill=Refill("lookback", "calendar", why="the calendar is rewritten nightly; there is no session to miss"),
    ),
    # ── universe: per-symbol calls, so follow the Research rule ──
    DatasetContract(
        "raw_market.option_snapshot",
        "universe",
        # A plan boundary, not a target being missed. A chain download only ever
        # returns the current session — that is why 2026-08-11 is permanently
        # absent — so this depth accrues forward and can never be bought. Read as
        # `sessions/90` it measured 0 of 570 at target with a median of two days
        # and painted red on a matrix built to lower the barrier to
        # understanding, when what it was showing was a ramp.
        DepthTarget(
            "forward_only",
            why="a chain download only returns the current session, so depth accrues "
            "and cannot be backfilled; trim then keeps 90 of them",
            accrues_to_sessions=90,
        ),
        2.0,
        ("eod-pipeline", "intraday-chain"),
        "underlying",
        "snapshot_ts",
        low_cardinality=True,
        freshness_dimension="option_snapshot",
        grain="snapshot",
        refill=Refill("unrecoverable", why="a chain download only returns the current session — 2026-08-11 is permanently absent for this reason"),
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
        grain="catalogue",
        refill=Refill("lookback", "option-refresh", why="the catalogue is re-enumerated on rotation; a stale name is refreshed when its turn comes"),
    ),
    DatasetContract(
        "raw_market.option_open_interest",
        "universe",
        # Derived from the same chain response as option_snapshot, so the same
        # boundary applies for the same reason.
        DepthTarget(
            "forward_only",
            why="derived from the chain snapshot, which only returns the current "
            "session; same 90-session retention",
            accrues_to_sessions=90,
        ),
        2.0,
        ("eod-pipeline",),
        "underlying",
        "trade_date",
        freshness_dimension="option_open_interest",
        grain="snapshot",
        refill=Refill("unrecoverable", why="derived from the same chain response, so it shares the chain's boundary"),
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
        grain="daily",
        refill=Refill("slot", "option-bars", why="near-spot contracts for the named session, at the universe scope"),
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
        grain="minute",
        refill=Refill("slot", "minute-bars"),
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
        grain="minute",
        refill=Refill("slot", "minute-bars", why="a rotation, so one run fills a bounded batch rather than the whole benchmark set"),
        # A rotation, so "this session" is the wrong window. minute-bars picks a
        # bounded batch of 80 near-spot contracts out of the watchlist's chains,
        # which reaches about three underlyings a day and the whole set over
        # roughly ten. Measured per session it read 3 of 26 and rendered red for
        # working exactly as designed; whether the rotation is turning is the
        # continuity axis's question, not breadth's.
        breadth_window="ever",
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
