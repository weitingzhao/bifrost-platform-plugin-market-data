# Option history backfill program — Wave LO-5

> **Superseded 2026-09-08 by `docs/SUBSCRIPTION_FOCUS_PROGRAM.md` Phase P4, which
> is implemented and running.** This file is kept for the decision history; the
> tables below describe a world with a 5-requests-per-minute free tier that the
> Owner never had.

## How the backfill actually works now

`worker/backfill.py`'s stub (it passed the underlying where an option ticker
belonged) is retired in favour of a worker job kind, because the enumeration is
far too big for an API request: SPY and SPX each list more than 50,000
contracts in a two-year window.

- **`option_backfill_plan`** — one job per underlying per expiry month. It
  enumerates that month's contracts (`expired=true`), keeps those whose strike
  is within ±30% of the underlying's close at the start of the contract's
  pricing window, and bulk-enqueues one `option_daily` job per surviving
  contract covering at most that contract's last 90 days of life. Measured:
  NVDA's August 2026 expiries are 1,752 contracts, of which 1,008 survive the
  strike filter.
- **Order matters.** The strike filter reads `raw_market.stock_daily`, so the
  five-year grouped-daily stock backfill has to land first. A month planned
  before its spot prices exist keeps every strike instead of the band — more
  data than intended, not less, but it costs vendor calls.
- **Fire it** with `POST /market/ingest/enqueue-slot {"slot": "option-backfill",
  "force": true}`. The slot has no cron: it is an Owner-run one-off, and the
  ingest dashboard does not score it for adherence.

## Historical decision record (superseded)

## Options

| Path | Cost | Scope |
|------|------|-------|
| A — Polygon developer tier | Paid | Full option backtest surface |
| B — Starter narrow slice | Free (slow) | 2–3 symbols × ~90 days |
| C — Stock-leg only | Free | Skip option validate (LO-3b) |

## Engineering (after A or B)

1. Unsuspend `market-data-option-backfill` CronJob
2. Monitor `raw_market.option_daily` span via Console Massive Ingest Daily volume
3. Enable LO-3b in research `validate_hook` when `_option_coverage_available()` passes

## Verify

```bash
psql -d bifrost_golden_source -c \
  "SELECT min(trade_date), max(trade_date), count(*) FROM raw_market.option_daily"
make verify-market-data
```

Spine: `D-Market-Option-History`
