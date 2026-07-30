# Market Data Schema (`market.*` + `data_ops.*`)

Owner review deliverable for program **market-data-subcontractor** Phase **P1**.

Physical database: shared PostgreSQL (`bifrost_dev` / `bifrost_prod`).  
Logical isolation: schemas `market` (public market data) and `data_ops` (ingest jobs / ops metadata).  
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
| Partitioning | Year for `stock_daily`; month for minute / option daily / snapshot |
| Fundamentals | One jsonb table (`stock_financials`) instead of six flat tables |
| Jobs | `data_ops.job_ingest` is the broker (`SELECT FOR UPDATE SKIP LOCKED` in P3) |

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

## `market` views

### `market.v_us_equity_universe`

Active US common stocks from `market.ticker` (`locale=us`, `market=stocks`, `instrument_type=cs`).

### `market.v_option_chain_latest`

`DISTINCT ON (option_ticker)` latest row from `market.option_snapshot` (convenience; may be heavy on large datasets).

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
| `data_ops.ensure_month_partitions(schema, table, months_back, months_forward)` | minute / option daily / snapshot |

`apply_ddl()` calls these after table creation:

| Table | Granularity | Back / Forward |
|-------|-------------|----------------|
| stock_daily | year | 5 / 2 |
| stock_minute | month | 12 / 4 |
| option_daily | month | 12 / 4 |
| option_minute | month | 12 / 4 |
| option_snapshot | month | 12 / 4 |

Each partitioned parent also gets a `*_default` partition for out-of-range values.

---

## Roles (`scripts/create_roles.sql`)

| Role | Access |
|------|--------|
| `data_writer` | USAGE/CREATE + ALL on `market` and `data_ops` |
| `market_reader` | SELECT on `market.*`; SELECT on selected `data_ops` status tables |

Passwords in the SQL file are placeholders (`CHANGE_ME_*`).

---

## Explicitly out of P1 scope (remain in Trade / Research for now)

- `stock_readiness_daily`, `cache_stock_snapshot`
- `report_option_max_pain_daily`, `report_option_atm_iv_daily`
- `option_trades` (Developer tier)
- `ticker_types`, `ticker_related_tickers`
- `job_bars_backfill`, `job_sepa_phase4`, IB bars paths

---

## Owner review checklist

- [ ] Field design for bars / options / ticker / financials jsonb
- [ ] Partition strategy (year vs month)
- [ ] Confirm no missing core tables for Research/AI near-term needs
- [ ] Confirm jsonb fundamentals vs six scalar tables decision
