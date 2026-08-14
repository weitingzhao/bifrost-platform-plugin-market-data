# Market Data Analytics

Derived metrics written by the Market Data Plugin CronJobs into `market_analytics.*`.

## Schedules (UTC)

| Slot | Cron | Writes | Notes |
|------|------|--------|-------|
| `max-pain` | `45 22 * * *` | `max_pain_daily` | From `option_open_interest` |
| `atm-iv-pcr` | `0 23 * * *` | `atm_iv_daily` + `pcr_daily` | D12=A merged slot |
| `iv-percentile` | `15 23 * * *` | `iv_percentile_daily` | After ATM IV; D12=A |

Each slot runs over `lookback_days` recent trading days (default 3) for holiday/gap heal.

## Algorithms

### Max Pain

Per `(symbol, expiry)` on a trade date:

`pain(K) = Σ [ OI_call(s)·max(0,K−s)·100 + OI_put(s)·max(0,s−K)·100 ]`

`max_pain_strike = argmin_K(pain(K))`

Source: `market.option_open_interest` only (no Polygon).

### ATM IV (D10=A)

1. Load last snapshot of the NY calendar day from `market.v_option_snapshot_with_stock`
   (already JOINs `stock_daily.close` as `underlying_price`).
2. JOIN `market.option_contract` for `expiry` / `strike` / `option_right`.
3. Group by `(underlying, expiry)`; spot = median `underlying_price`.
4. Nearest strikes to spot; take first available call IV and put IV (possibly different strikes).
5. `atm_iv` = average of available sides; require `0 < iv < 10` (decimal IV).
6. `iv_source = 'snapshot'`.

### Put/Call Ratio (D11=A)

- **OI**: sum put/call `open_interest` from `market.option_open_interest` for `trade_date`.
- **Volume**: last snapshot per ticker on the NY day; sum `day_volume` by put/call via `option_contract`.
- `pcr_oi = put_oi / call_oi` (None if `call_oi == 0`); same for volume.

### IV Percentile / Rank

**Current IV** for `symbol` + `trade_date` = **median** of `atm_iv` across expiries that day
(robust when near-term expiries are sparse or noisy).

Universe for ATM IV / IV Percentile Cron: optionable watchlist ∪ Wave A Benchmarks (`SPY`, `QQQ`, `IWM`).
`eod-pipeline` and `option-refresh` also union those three so option snapshots exist for ATM IV.

History: up to `percentile_window` (default 252) daily representative IVs ending on `trade_date`
(inclusive).

- **IV Percentile** = `#(hist ≤ current) / n × 100`
- **IV Rank** = `(current − min) / (max − min) × 100`; when `max == min` → **50.0**

## Black-box caveat

ATM IV uses **Polygon precomputed `iv`** stored on `market.option_snapshot`.
This plugin selects the ATM strike and averages call/put sides, but **cannot independently
verify Polygon’s IV model** (no local Black–Scholes reverse solve from bid/ask).
Treat absolute IV levels as vendor-dependent; relative day-over-day changes are more robust.

## Read API

Plugin routes (prefix `/market`):

| Method | Path | Table / source |
|--------|------|----------------|
| GET | `/market/analytics/max-pain` | `max_pain_daily` (persisted) |
| GET | `/market/analytics/max-pain/compute` | live from `market.option_open_interest` |
| GET | `/market/analytics/max-pain/compute/history` | live OI series |
| GET | `/market/analytics/atm-iv` | `atm_iv_daily` (persisted) |
| GET | `/market/analytics/atm-iv/term` | term structure from `atm_iv_daily` |
| GET | `/market/analytics/pcr` | `pcr_daily` |
| GET | `/market/analytics/iv-percentile` | `iv_percentile_daily` |

Query params: `symbol`, `trade_date`, `lookback_days`; ATM IV / Max Pain also accept `expiry`.
Compute routes require `symbol` + `expiry`.
