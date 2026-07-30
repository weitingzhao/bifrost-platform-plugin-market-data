# STG promote notes (Post-P9 L1)

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
| `ticker_related_tickers` → `from_symbol` | done |
| `scripts/create_roles.sql` | done |
| `scripts/p9_drop_legacy_tables.sql` | done (`legacy=0`) |
| STG workers consume `ticker_sync` | after overlay apply |

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
"
```
