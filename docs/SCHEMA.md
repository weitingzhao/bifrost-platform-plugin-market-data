# Market Data Schema — Golden Source (`bifrost_golden_source`)

> **Canonical schemas (Wave 6.6):** `raw_market.*` (Polygon raw), `features.*` (Research Feature Store), `ops_jobs.*` (ingest queue + freshness).  
> **Legacy aliases (retired Wave 6.6):** `market.*`, `market_analytics.*`, `features_daily.*`, `data_ops.*` — see `bifrost-trade-core/docs/DATABASE.md`.

Owner review deliverable for program **market-data-subcontractor** Phase **P1**,
extended by **market-data-expand** Wave **0-A** (historical `market_analytics` / `features_daily`, retired Wave 7).

## `features.*` (owned by bifrost-research)

Wave 7: Plugin **does not** create or write `features.*`. DDL, scheduled compute, and
upserts live in **`bifrost-research`** (`features.option_metric_*` volatility tables).
Plugin API and coverage routes read canonical tables for compatibility:

| Legacy alias | Canonical table |
|--------------|-----------------|
| `max_pain_daily` | `features.option_metric_max_pain_daily` |
| `atm_iv_daily` | `features.option_metric_atm_iv_daily` |
| `pcr_daily` | `features.option_metric_pcr_daily` |
| `iv_percentile_daily` | `features.option_metric_iv_percentile_daily` |

Research API `:8795` is the preferred write + read path (`/analytics/options/*`).

## Golden Source Model (since W2 — 2026-08-14)

As of **market-data-golden-source** Wave 2, Market Data runs as a **Single Golden
Source** instance. Per-environment database separation (`bifrost_dev` / `bifrost_prod`)
has been retired; all environments share one ingest instance writing to
**`bifrost_golden_source`** (target database name after Owner-gated rename).

- **One Plugin namespace** (`plugin-market-data`) serves all Trade environments
- **One database** with schemas `raw_market`, `features.*` (Research), `ops_jobs`
- **Watchlist union mode**: Plugin reads the union of all Trade environment watchlists
  via `platform-api` (`GET /api/v1/watchlist/union`)
- **Trade consumers** read exclusively through Plugin API HTTP (`:8790` proxied via
  `platform-api` `:8780`) — zero direct SQL against `market.*`
- **STG/PROD overlays archived** in `k8s/overlays/_archived/` (retained for rollback
  reference; `plugin-market-data-stg` / `plugin-market-data-prod` namespaces dormant)

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
| Partitioning | Year for `stock_daily`; month for minute / option daily / snapshot / analytics; **day** for `option_trades` (30d retention); **no partition** for `stock_snapshot` / `stock_movers` (daily upsert by session_date) |
| Fundamentals | One jsonb table (`stock_financials`) instead of six flat tables |
| Jobs | `ops_jobs.job_ingest` is the broker (`SELECT FOR UPDATE SKIP LOCKED` in P3) |
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
**Partitions:** `stock_daily_yYYYY` + `stock_daily_default` via `ops_jobs.ensure_year_partitions`

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

### `market.option_trades`

Daily REST options tape from Polygon `GET /v3/trades/{optionsTicker}` (not WebSocket).
Used by Research Order Flow when present (`data_source=option_trades_tape`).

| Column | Type | Notes |
|--------|------|-------|
| option_ticker | text | Polygon native key; PK part |
| underlying / expiry / strike / option_right | | contract identity |
| trade_date | date | NY session date; **day** RANGE partition key |
| sip_ts | timestamptz | SIP receive time (from ns) |
| sequence_number | bigint | per-ticker sequence; PK part |
| price / size | | print price and size |
| exchange | integer | Polygon exchange id |
| conditions | integer[] | condition codes |
| correction | integer | correction indicator |
| participant_ts | timestamptz | exchange timestamp |
| fetched_at | timestamptz | ingest wall clock |

**PK:** `(option_ticker, trade_date, sip_ts, sequence_number)`  
**Partitions:** `option_trades_dYYYYMMDD` + default via `ops_jobs.ensure_day_partitions`  
**Retention:** 30 days — `trim` slot calls `ops_jobs.drop_day_partitions_older_than(..., 30)`  
**Job kind:** `option_trades` · slot `option-trades` (~23:00 UTC)  
**Universe:** SPX ∪ Trade watchlist (sorted union truncated to 50, always include SPX)

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
| filing_date | date | When the 10-Q / 10-K was filed. Nullable. |
| fetched_at | timestamptz | |

`period_date` is the fiscal period **end**; `filing_date` is when the report was
actually filed, which is the date an earnings-event study needs. The gap is not
constant — 24 to 31 days on NVDA — so `period_date` plus a fixed offset is not a
substitute. Null on `ttm` rows and on fiscal-Q4 `quarterly` rows, whose filing is
reported by the matching `annual` row; a calendar should read both. Partial index
`<table>_filing_date` covers the non-null rows.

Added by `wave8_migrations.add_financials_filing_date`, called from `apply_ddl`
and **not** from `apply_wave8_migrations` — `raw_market` tables are owned by
`postgres`, so the ALTER needs the superuser path the schema-migrate job header
documents.

**PK:** `(symbol, report_type, period_date, period_type)`  
(`period_type` defaults to `''` so NULL is not required in PK.)

### `market.corporate_action`

Replaces `public.massive_corporate_action`.

**Unique:** `(symbol, action_type, ex_date)`

### `market.us_market_holiday`

Canonical US exchange holiday / early-close calendar from Polygon
(`/v1/marketstatus/upcoming`). Replaces Trade-owned `public.reference_us_holidays`.

| Column | Type | Notes |
|--------|------|-------|
| exchange | text | PK part; e.g. `NYSE` |
| holiday_date | date | PK part |
| name | text | Holiday label |
| status | text | `closed` / `early-close` |
| open_time / close_time | timestamptz | Session window when provided |
| fetched_at | timestamptz | default `now()` |

**PK:** `(exchange, holiday_date)`  
**Index:** `(holiday_date DESC)`

Plugin scheduler / quality / coverage derive trading sessions as
**weekday − NYSE `status='closed'`** (same semantics as Trade
`get_is_us_trading_day`). `early-close` remains a trading day.
The retired flat table `data_ops.us_trading_calendar` is dropped by DDL.

Trade consumers read this table via FDW (`market.us_market_holiday`).

### `market.ticker_related`

Polygon related-companies peers (`GET /v1/related-companies/{ticker}`).
Replaces Trade-owned `public.ticker_related_tickers`.

| Column | Type | Notes |
|--------|------|-------|
| from_symbol | text | PK part; source ticker |
| to_symbol | text | PK part; peer ticker |
| rank | integer | API `results[]` order (0-based) |
| fetched_at | timestamptz | default `now()` |

**PK:** `(from_symbol, to_symbol)`  
**Indexes:** `(from_symbol)`, `(to_symbol)`

Ingest kind `ticker_related` (one symbol per job) deletes existing peers for
`from_symbol` then upserts the fresh set. Scheduler slot `related-rotate`
rotates watchlist symbols daily (`batch_size` default 40).

Trade consumers read this table via FDW (`market.ticker_related`).

### `market.ticker_type`

Polygon instrument type dictionary (`GET /v3/reference/tickers/types`).
Replaces Trade-owned `public.ticker_types`.

| Column | Type | Notes |
|--------|------|-------|
| code | text | PK part; e.g. `CS`, `ETF` |
| description | text | Human-readable label |
| asset_class | text | PK part; e.g. `stocks`, `indices` |
| locale | text | PK part; e.g. `us` |
| fetched_at | timestamptz | default `now()` |

**PK:** `(code, asset_class, locale)`

Ingest kind `ticker_type` (no payload) TRUNCATEs then upserts the full
dictionary (~25 rows). No CronJob — enqueue manually when Polygon codes change.

Trade consumers read via Plugin HTTP (`/market/reference/ticker-types`).

---

## `market` views

### `market.v_us_equity_universe`

Active US common stocks from `market.ticker` (`locale=us`, `market=stocks`, `instrument_type=cs`).

### `market.v_option_chain_latest`

`DISTINCT ON (option_ticker)` latest row from `market.option_snapshot` (convenience; may be heavy on large datasets).

### `market.v_option_snapshot_with_stock`

Option snapshot joined to same-day `market.stock_daily` close (NY calendar date).

---

## `ops_jobs` tables

### `ops_jobs.job_ingest`

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

### `ops_jobs.ingest_freshness`

Per-dimension freshness for Platform probes (`dimension` PK).

~~`data_ops.us_trading_calendar`~~ — **retired**. Trading-day checks use
`market.us_market_holiday` (weekday − NYSE closed).
---

## Partition helpers

| Function | Use |
|----------|-----|
| `ops_jobs.ensure_year_partitions(schema, table, years_back, years_forward)` | `stock_daily` |
| `ops_jobs.ensure_month_partitions(schema, table, months_back, months_forward)` | minute / option daily / snapshot / analytics |

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
| `data_writer` | USAGE/CREATE + ALL on `raw_market` and `ops_jobs` |
| `market_reader` | SELECT on `raw_market.*`; SELECT on selected `ops_jobs` status tables |

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
| Ingest | `/market/ingest/*` | `POST enqueue` → `ops_jobs.job_ingest` (D15=A) |
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
- `job_bars_backfill`, `job_sepa_phase4`, IB bars paths

> **Note:** `market.option_trades` is now in-scope (plugin-options-tape / Operate Queue) —
> day-partitioned REST tape with 30d retention; see table section above.

---

## Owner review checklist

- [ ] Field design for bars / options / ticker / financials jsonb
- [ ] Partition strategy (year vs month)
- [ ] Confirm no missing core tables for Research/AI near-term needs
- [ ] Confirm jsonb fundamentals vs six scalar tables decision
- [ ] Confirm `market_analytics` four-table daily contract (Wave 0-A)
