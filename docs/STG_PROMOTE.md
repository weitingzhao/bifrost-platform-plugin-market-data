# STG promote notes (Post-P9 L1)

> **ARCHIVED (2026-08-14):** STG overlay retired by `market-data-golden-source` W2-P2.
> Single Golden Source instance in `plugin-market-data` NS replaces per-env workers.
> Overlays preserved at `k8s/overlays/_archived/stg/` for rollback reference.

## Topology (Owner 2026-07-30)

Independent namespace: **`plugin-market-data-stg`** (does not cut over DEV `plugin-market-data`).

| Component | Target |
|-----------|--------|
| Workers / CronJobs | `plugin-market-data-stg` via `kubectl apply -k k8s/overlays/stg` |
| Postgres DB | `bifrost_stg` |
| Redis (config URL) | `redis-queue-stg.data.svc.cluster.local` |
| PG NetworkPolicy | `postgres-trade-ingress` allows `plugin-market-data-stg` |
| Redis NetworkPolicy | `redis-queue-stg-ingress` allows `plugin-market-data-stg` |

## DB

| Step | Status |
|------|--------|
| `market` / `data_ops` DDL (`POSTGRES_DB=bifrost_stg`) | done |
| `public.ticker_related_tickers` → `market.ticker_related` (Golden Source) | done (2026-08-19) |
| `scripts/create_roles.sql` | done |
| `scripts/p9_drop_legacy_tables.sql` | done (`legacy=0`) |
| STG workers consume `ticker_sync` | after overlay apply |
| Daily `reference` CronJob (`ticker_sync` universe) | schedule `30 21 * * *` UTC |
| Daily `fundamentals-rotate` CronJob (`financials` batch_size=40) | schedule `0 3 * * *` UTC; skip non-trading days |
| Daily `related-rotate` CronJob (`ticker_related` batch_size=40) | schedule `30 22 * * *` UTC; skip non-trading days |

### 养库 SLA（dev / analysis base）

| Metric | Target |
|--------|--------|
| `ticker_sync` freshness age | &lt; 24h |
| `financials` freshness age | &lt; 24h (daily successful writes) |
| Watchlist financials coverage | ≤7 trading days per full rotate (`batch_size=40`) |

**Image**: `bifrost-market-data:0.1.2` (`k8s/base` `newTag`) — includes scheduler slots `reference` + `fundamentals-rotate`.

**Readiness rollup** (Platform Gallery optional KPI): reads `public.stock_readiness_daily`. After P9, `public.v_us_equity_universe.tickers_id` is a synthetic `hashtext(symbol)` (bifrost-core ≥0.5.2) so `included_in_universe` / `fund_cache_valid` can be non-zero. There is no separate `fund_cache` table — validity is derived from `fundamental_eval` + `fund_cache_expire_at`.

### `price_ready` gap (DEV, 2026-08-02)

`price_ready` requires ≥**240** `market.stock_daily` bars in a ~420d window. After P9 cutover the span was only ~2026-06-01…2026-07-31 (~2 bars/symbol avg) → `price_ready=0`.

**Backfill path** (running on `bifrost_dev`): enqueue weekday `stock_daily_grouped` jobs from ~2025-06-01 → latest session into `data_ops.job_ingest`; polygon-worker-stocks consumes them. Re-snapshot readiness after depth grows (`syms ≥240` / rollup `price_ready`).

```bash
# Progress
PRIMARY=$(kubectl -n data get cluster bifrost-postgres -o jsonpath='{.status.currentPrimary}')
kubectl -n data exec "$PRIMARY" -c postgres -- psql -d bifrost_dev -c "
SELECT status, count(*) FROM data_ops.job_ingest
 WHERE kind='stock_daily_grouped' AND created_at > now() - interval '1 day'
 GROUP BY status;
SELECT count(*) FILTER (WHERE c >= 240) AS ge240, round(avg(c)::numeric,1) AS avg_bars
  FROM (SELECT symbol, count(*) c FROM market.stock_daily
        WHERE bar_date >= CURRENT_DATE - 420 GROUP BY symbol) s;
"
```

## Apply

```bash
export KUBECONFIG=~/.kube/bifrost-k3s.yaml
# Netpols (trade-infra)
kubectl apply -f bifrost-trade-infra/k8s/data/postgres-ingress-network-policy.yaml
kubectl apply -f bifrost-trade-infra/k8s/data/redis/network-policies.yaml
# Secrets (copy from DEV once)
kubectl -n plugin-market-data get secret market-data-secrets -o yaml \
  | sed 's/namespace: plugin-market-data/namespace: plugin-market-data-stg/' \
  | grep -v '^\s*resourceVersion:\|^\s*uid:\|^\s*creationTimestamp:' \
  | kubectl apply -f -
kubectl apply -k bifrost-platform-plugin-market-data/k8s/overlays/stg
```

## Platform Gallery freshness seat

```bash
# bifrost-platform/.env (STG viewer / seat only)
MARKET_DATA_FRESHNESS_DB=bifrost_stg
```

DEV seat keeps default `bifrost_dev`.

## Verify

```bash
export KUBECONFIG=~/.kube/bifrost-k3s.yaml
kubectl -n plugin-market-data-stg get deploy,pods
PRIMARY=$(kubectl -n data get cluster bifrost-postgres -o jsonpath='{.status.currentPrimary}')
kubectl -n data exec "$PRIMARY" -c postgres -- psql -d bifrost_stg -tAc "
SELECT 'legacy=' || count(*) FROM information_schema.tables
 WHERE table_schema='public' AND table_name IN (
   'stock_day','tickers','job_massive_backfill','option_snapshots');
SELECT 'ticker=' || count(*) FROM market.ticker;
SELECT 'pending_ticker_sync=' || count(*) FROM data_ops.job_ingest
 WHERE kind='ticker_sync' AND status='pending';
SELECT 'pending_financials=' || count(*) FROM data_ops.job_ingest
 WHERE kind='financials' AND status='pending';
SELECT dimension, last_run_at, status, rows_written
  FROM data_ops.ingest_freshness
 WHERE dimension IN ('ticker_sync','financials')
 ORDER BY dimension;
"
```

CronJobs (after overlay apply):

```bash
kubectl -n plugin-market-data-stg get cronjob \
  -l 'bifrost.market-data/slot in (reference,fundamentals-rotate)'
```
