# Market Data Schema (`market.*` + `market_analytics.*` + `data_ops.*`)

Owner review deliverable for program **market-data-subcontractor** Phase **P1**,
extended by **market-data-expand** Wave **0-A** (`market_analytics`).

Physical database: shared PostgreSQL (`bifrost_dev` / `bifrost_prod`).  
Logical isolation: schemas `market` (public market data), `market_analytics`
(derived daily analytics), and `data_ops` (ingest jobs / ops metadata).  
Single vendor: **Polygon.io** — no `source` column, no IB legacy fields.

Apply DDL:

```bash
make db-init          # uses MARKET_DATA_CONFIG / POSTGRES_* / config/market-data.yaml(.example)
make db-init-dry      # print target only
# then as superuser (optional):
psql -f scripts/create_roles.sql
```

---

## Design principles

| Principle | Choice |
|-----------|--------|
| Time | Calendar `date` for daily bars (NY trade date semantics at ingest); `timestamptz` UTC for intraday / snapshots |
| Identity | `symbol` uppercase TRIM at write time; options keyed by Polygon `option_ticker` (`O:AAPL250620C00150000`) |
| Partitioning | Year for `stock_daily`; month for minute / option daily / snapshot / analytics; **no partition** for `stock_snapshot` / `stock_movers` (daily upsert by session_date) |
| Fundamentals | One jsonb table (`stock_financials`) instead of six flat tables |
| Jobs | `data_ops.job_ingest` is the broker (`SELECT FOR UPDATE SKIP LOCKED` in P3) |
| Analytics | Derived daily metrics in `market_analytics.*` (computed from `market.*`; no live vendor calls in DDL) |

---

## `market` tables

### `market.stock_daily`

Replaces `public.stock_day`.

| Column | Type | Notes |
|--------|------|-------|
| symbol | text | PK part |
| bar_date | date | PK part; RANGE partition key (by year) |
| open/high/low/close | double precision | OHLC |
| volume | bigint | |
| vwap | double precision | |
| trade_count | bigint | |
| fetched_at | timestamptz | ingest wall clock |

**PK:** `(symbol, bar_date)`  
**Indexes:** `(symbol, bar_date DESC)`  
**Partitions:** `stock_daily_yYYYY` + `stock_daily_default` via `data_ops.ensure_year_partitions`

### `market.stock_minute`

Replaces `public.stock_min`.

| Column | Type | Notes |
|--------|------|-------|
| symbol | text | PK part |
| period | text | e.g. `1`, `5`, `15` |
| bar_time | timestamptz | UTC; RANGE partition key (by month) |
| OHLCV / vwap / trade_count / fetched_at | | same pattern as daily |

**PK:** `(symbol, period, bar_time)`

### `market.stock_snapshot`

Full-market (or single-ticker) Polygon stock snapshot, daily upsert.
**Non-partitioned.** Trades/Quotes tick persistence is intentionally deferred (D1=A).

| Column | Type | Notes |
|--------|------|-------|
| symbol | text | PK part |
| session_date | date | PK part; NY calendar session day |
| open/high/low/close | double precision | from snapshot `day` |
| volume | bigint | |
| vwap | double precision | |
| prev_close | double precision | from `prevDay.c` |
| change | double precision | `todaysChange` |
| change_pct | double precision | `todaysChangePerc` |
| fetched_at | timestamptz | ingest wall clock |

**PK:** `(symbol, session_date)`  
**Index:** `(session_date DESC, symbol)`  
**Job kind:** `stock_snapshot` · slot `stock-snapshot` (~21:05 UTC)

### `market.stock_movers`

Daily gainers / losers snapshot rows. **Non-partitioned.**

| Column | Type | Notes |
|--------|------|-------|
| direction | text | `gainers` or `losers` |
| symbol | text | PK part |
| session_date | date | PK part; NY calendar session day |
| change_pct | double precision | `todaysChangePerc` |
| price | double precision | day close |
| volume | bigint | |
| fetched_at | timestamptz | |

**PK:** `(direction, symbol, session_date)`  
**Index:** `(session_date DESC, direction)`  
**Job kind:** `stock_movers` · slot `stock-movers` (~21:10 UTC)

### `market.option_daily`

Replaces `public.option_day`.

| Column | Type | Notes |
|--------|------|-------|
| option_ticker | text | Polygon native key; PK part |
| underlying | text | |
| expiry | date | |
| strike | double precision | |
| option_right | char(1) | `C` / `P` |
| bar_date | date | PK part; monthly partitions |
| OHLCV / vwap / trade_count / fetched_at | | |

**PK:** `(option_ticker, bar_date)`

### `market.option_minute`

Replaces `public.option_min`. Same contract identity as option_daily plus `period` + `bar_time`.

**PK:** `(option_ticker, period, bar_time)`

### `market.option_contract`

Replaces `public.option_contracts` (drops self-built `contract_key` / serial id).

| Column | Type | Notes |
|--------|------|-------|
| option_ticker | text | PRIMARY KEY |
| underlying / expiry / strike / option_right | | |
| exercise_style | text | |
| shares_per_contract | integer | default 100 |
| first_seen_at / updated_at | timestamptz | |

### `market.option_snapshot`

Replaces `public.option_snapshots`.

| Column | Type | Notes |
|--------|------|-------|
| option_ticker | text | PK part |
| underlying | text | |
| snapshot_ts | timestamptz | PK part; monthly partitions |
| iv / delta / gamma / theta / vega | double precision | |
| open_interest | integer | |
| day_* | | session day stats from snapshot payload |
| fetched_at | timestamptz | |

**PK:** `(option_ticker, snapshot_ts)`

### `market.option_expiration`

Replaces `public.option_expiration_cache`.

**PK:** `(underlying, expiry)`

### `market.option_open_interest`

Replaces `public.option_open_interest_daily`.

**PK:** `(option_ticker, trade_date)`

**Two write paths (P2):**

| Path | When | Behavior |
|------|------|----------|
| Live ingest (`kind=option_open_interest`) | Daily `eod-pipeline` CronJob + API backfill enqueue | Fetches current Polygon options snapshot OI → upsert (updates existing rows). Polygon has **no historical OI API**. |
| Snapshot extract (`extract_oi_from_snapshots`) | `scripts/backfill_oi.py`, weekly `oi-gap-heal` slot (Sat 04:00 UTC) | DB-to-DB: for each `(option_ticker, NY calendar day)` take `MAX(snapshot_ts)` where `open_interest IS NOT NULL`. **`ON CONFLICT DO NOTHING`** — never overwrites live ingest rows; only fills gaps (D4=B, D5=A, D6=B). |

Coverage check: `quality.check_option_oi_coverage` requires ≥1 OI row per watchlist underlying × recent trading day.

### `market.ticker`

Merges `public.tickers` + `public.ticker_overview` into one row per symbol (no `tickers_id` FK).

**PK:** `symbol`

### `market.stock_financials`

Replaces six flat fundamentals tables (`stock_income_statements`, `stock_balance_sheets`, `stock_cash_flows`, `stock_ratios`, `stock_short_interest`, `stock_short_volume`) with jsonb payload.

| Column | Type | Notes |
|--------|------|-------|
| symbol | text | PK part |
| report_type | text | `income` / `balance` / `cashflow` / `ratios` / `short_interest` / `short_volume` |
| period_date | date | PK part |
| period_type | text | `quarterly` / `annual` / `ttm` / `settlement` / `''` |
| fiscal_year / fiscal_quarter | integer | optional |
| data | jsonb | vendor field bag |
| fetched_at | timestamptz | |

**PK:** `(symbol, report_type, period_date, period_type)`  
(`period_type` defaults to `''` so NULL is not required in PK.)

### `market.corporate_action`

Replaces `public.massive_corporate_action`.

**Unique:** `(symbol, action_type, ex_date)`

---

## `market_analytics` tables

Derived daily analytics written by compute jobs (Wave 0-B+). All four tables are
`PARTITION BY RANGE (trade_date)` with monthly partitions via
`data_ops.ensure_month_partitions('market_analytics', …, 12, 4)`.

### `market_analytics.max_pain_daily`

| Column | Type | Notes |
|--------|------|-------|
| symbol | text | PK part |
| trade_date | date | PK part; RANGE partition key |
| expiry | date | PK part |
| max_pain_strike | double precision | strike minimizing total pain |
| total_oi | integer | |
| total_pain_at_strike | double precision | pain at max-pain strike |
| computed_at | timestamptz | default `now()` |

**PK:** `(symbol, trade_date, expiry)`  
**Index:** `(symbol, trade_date DESC)`

**Computation path (P3 / D7=A, D8=B):**

1. Source: `market.option_open_interest` for each trading day in lookback (`lookback_days=3`).
2. Group by `(underlying, expiry)`; build strike → (call_oi, put_oi).
3. `pain(K) = Σ [ OI_call(s)·max(0,K−s)·100 + OI_put(s)·max(0,s−K)·100 ]`; `max_pain_strike = argmin_K(pain(K))`.
4. Upsert into this table (`ON CONFLICT DO UPDATE` refreshes `computed_at`).
5. Scheduler slot `max-pain` (CronJob `45 22 * * *` UTC) runs inline DB compute — no Polygon.
6. Read API: `GET /market/analytics/max-pain` (D9=A).

### `market_analytics.atm_iv_daily`

| Column | Type | Notes |
|--------|------|-------|
| symbol | text | PK part |
| trade_date | date | PK part; RANGE partition key |
| expiry | date | PK part |
| atm_strike | double precision | |
| atm_iv | double precision | |
| underlying_price | double precision | |
| iv_source | text | e.g. `snapshot` |
| computed_at | timestamptz | default `now()` |

**PK:** `(symbol, trade_date, expiry)`  
**Index:** `(symbol, trade_date DESC)`

**Computation path (P4 / D10=A):**

1. Source: `market.v_option_snapshot_with_stock` (last snap per ticker on NY day) JOIN `option_contract`.
2. Group by `(underlying, expiry)`; spot = median `underlying_price`; nearest strike call/put IV avg.
3. Upsert (`ON CONFLICT DO UPDATE`); `iv_source='snapshot'`.
4. Scheduler slot `atm-iv-pcr` (`0 23 * * *` UTC) with PCR.
5. Read API: `GET /market/analytics/atm-iv`.
6. **Black-box:** `iv` is Polygon precomputed — see `docs/ANALYTICS.md`.

### `market_analytics.pcr_daily`

| Column | Type | Notes |
|--------|------|-------|
| symbol | text | PK part |
| trade_date | date | PK part; RANGE partition key |
| pcr_oi | double precision | put/call open-interest ratio |
| pcr_volume | double precision | put/call volume ratio |
| total_put_oi / total_call_oi | integer | |
| total_put_volume / total_call_volume | bigint | |
| computed_at | timestamptz | default `now()` |

**PK:** `(symbol, trade_date)`  
**Index:** `(symbol, trade_date DESC)`

**Computation path (P4 / D11=A):**

1. OI totals from `market.option_open_interest` for `trade_date`.
2. Volume totals from last `option_snapshot.day_volume` per ticker (NY day) + `option_contract` right.
3. Upsert via slot `atm-iv-pcr`; read `GET /market/analytics/pcr`.

### `market_analytics.iv_percentile_daily`

| Column | Type | Notes |
|--------|------|-------|
| symbol | text | PK part |
| trade_date | date | PK part; RANGE partition key |
| iv_current | double precision | median atm_iv across expiries that day |
| iv_percentile_1y | double precision | |
| iv_rank_1y | double precision | |
| lookback_days | integer | samples used (≤ percentile_window) |
| computed_at | timestamptz | default `now()` |

**PK:** `(symbol, trade_date)`  
**Index:** `(symbol, trade_date DESC)`

**Computation path (P4 / D12=A):**

1. Source: `market_analytics.atm_iv_daily` history (~252 trading days).
2. Current IV = median of per-expiry `atm_iv` on `trade_date`.
3. Percentile / rank vs lookback window; slot `iv-percentile` at `15 23 * * *` UTC.
4. Read API: `GET /market/analytics/iv-percentile`.

---

## `market` views

### `market.v_us_equity_universe`

Active US common stocks from `market.ticker` (`locale=us`, `market=stocks`, `instrument_type=cs`).

### `market.v_option_chain_latest`

`DISTINCT ON (option_ticker)` latest row from `market.option_snapshot` (convenience; may be heavy on large datasets).

### `market.v_option_snapshot_with_stock`

Option snapshot joined to same-day `market.stock_daily` close (NY calendar date).

---

## `data_ops` tables

### `data_ops.job_ingest`

Replaces `public.job_massive_backfill` as the PG-as-broker queue.

| Column | Type | Notes |
|--------|------|-------|
| id | bigserial | PK |
| kind | text | handler routing key |
| payload | jsonb | |
| payload_hash | text | dedup |
| priority | smallint | 0 standard, higher = preferred |
| status | text | `pending` / `running` / `done` / `failed` |
| result | jsonb | |
| attempts / max_attempts | smallint | |
| created_at / updated_at / started_at / finished_at | timestamptz | |

**Partial unique index:** `(kind, payload_hash)` WHERE `status IN ('pending','running') AND payload_hash IS NOT NULL`  
**Claim index:** `(status, priority DESC, created_at)`

### `data_ops.ingest_freshness`

Per-dimension freshness for Platform probes (`dimension` PK).

### `data_ops.us_trading_calendar`

Replaces `public.reference_us_holidays` with explicit trading-day flags.

---

## Partition helpers

| Function | Use |
|----------|-----|
| `data_ops.ensure_year_partitions(schema, table, years_back, years_forward)` | `stock_daily` |
| `data_ops.ensure_month_partitions(schema, table, months_back, months_forward)` | minute / option daily / snapshot / analytics |

`apply_ddl()` calls these after table creation:

| Table | Granularity | Back / Forward |
|-------|-------------|----------------|
| stock_daily | year | 5 / 2 |
| stock_minute | month | 12 / 4 |
| option_daily | month | 12 / 4 |
| option_minute | month | 12 / 4 |
| option_snapshot | month | 12 / 4 |
| max_pain_daily | month | 12 / 4 |
| atm_iv_daily | month | 12 / 4 |
| pcr_daily | month | 12 / 4 |
| iv_percentile_daily | month | 12 / 4 |

Each partitioned parent also gets a `*_default` partition for out-of-range values.

---

## Roles (`scripts/create_roles.sql`)

| Role | Access |
|------|--------|
| `data_writer` | USAGE/CREATE + ALL on `market`, `market_analytics`, and `data_ops` |
| `market_reader` | SELECT on `market.*` + `market_analytics.*`; SELECT on selected `data_ops` status tables |

Passwords in the SQL file are placeholders (`CHANGE_ME_*`).  
Apply DDL (`make db-init`) before `scripts/create_roles.sql` so schemas exist.

---

## Plugin REST API (P5 — port 8790)

FastAPI app: `src/bifrost_market_data/api/app.py`. OpenAPI at `/docs`.

| Group | Prefix | Notes |
|-------|--------|-------|
| Health | `/health` | DB probe; always HTTP 200 |
| Status | `/market/status` | Plugin + DB + key-configured bool |
| Analytics | `/market/analytics/*` | Table reads + max-pain live compute |
| Ingest | `/market/ingest/*` | `POST enqueue` → `data_ops.job_ingest` (D15=A) |
| Options | `/market/options/*` | DB expirations / snapshots / OI |
| Stocks / fundamentals / filings | `/market/stocks/*` | Polygon pass-through (D14=A) |
| Market ops | `/market/market-ops/*` | Conditions / exchanges / holidays / status |
| Reference (Polygon) | `/market/tickers*` | Pass-through |
| Reference (DB) | `/market/reference/*` | Coverage / search over `market.ticker` |
| Coverage | `/market/coverage/*` | Simplified SQL coverage / gaps |
| Corporate actions | `/market/corporate-actions` | DB read |
| Technical / TQ | `/market/technical-indicators/*`, `/market/trades-quotes/*` | Pass-through |

**Deferred to P7 (D13=A):** Celery sync guts, SSE stream, option fill eligibility, gap batch POST writers, Trade API route retirement, frontend rewire.

---

## Explicitly out of P1 scope (remain in Trade / Research for now)

- `stock_readiness_daily`, `cache_stock_snapshot`
- Legacy `report_option_max_pain_daily` / `report_option_atm_iv_daily` (replaced by `market_analytics.*`)
- `option_trades` (Developer tier)
- `ticker_types`, `ticker_related_tickers`
- `job_bars_backfill`, `job_sepa_phase4`, IB bars paths

---

## Owner review checklist

- [ ] Field design for bars / options / ticker / financials jsonb
- [ ] Partition strategy (year vs month)
- [ ] Confirm no missing core tables for Research/AI near-term needs
- [ ] Confirm jsonb fundamentals vs six scalar tables decision
- [ ] Confirm `market_analytics` four-table daily contract (Wave 0-A)
