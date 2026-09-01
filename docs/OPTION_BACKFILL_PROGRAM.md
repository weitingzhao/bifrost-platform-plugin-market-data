# Option history backfill program — Wave LO-5

Owner decision required before unsuspending `k8s/cronjob-option-backfill.yaml`.

See also [`bifrost-research/docs/BACKTEST_DATA_COVERAGE.md`](../../bifrost-research/docs/BACKTEST_DATA_COVERAGE.md).

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
