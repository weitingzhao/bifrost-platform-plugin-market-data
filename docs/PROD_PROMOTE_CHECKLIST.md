# PROD promote checklist (Post-P9 L2)

Owner unlock (2026-07-30): backup first, then DDL / related migrate / roles / `p9_drop` on **`bifrost_prod`**.

## Backup (done)

| Item | Value |
|------|--------|
| Local dump | `stocks/backups/bifrost_prod_pre_p9_20260730T230317Z.dump` (pg_dump `-Fc`, ~175 MiB) |
| SHA-256 | `a0c5396a8d04d114a9e4c575e2b8d8d03f107ec8510f725bf41db60cc1646b92` |
| On-cluster copy | `/var/lib/postgresql/data/bifrost_prod_pre_p9_20260730T230317Z.dump` on primary |

Restore sketch (if needed):

```bash
export KUBECONFIG=~/.kube/bifrost-k3s.yaml
PRIMARY=$(kubectl -n data get cluster bifrost-postgres -o jsonpath='{.status.currentPrimary}')
kubectl -n data cp stocks/backups/bifrost_prod_pre_p9_20260730T230317Z.dump \
  "$PRIMARY":/tmp/restore.dump -c postgres
# Coordinate with Owner before any restore — may require drop/recreate DB.
```

## DB steps (done 2026-07-30)

| Step | Status |
|------|--------|
| `scripts/init_schema.py` (`POSTGRES_DB=bifrost_prod`) | done |
| `scripts/migrate_related_from_symbol.sql` (uses `tickers.ticker`) | done (`related_rows=13012`) |
| `scripts/create_roles.sql` | done |
| `scripts/p9_drop_legacy_tables.sql` | done (`legacy=0`) |
| One-shot Job `market-data-prod-ticker-sync` | done (`market.ticker=5306`) |

## PROD workers (applied 2026-08-05)

| Step | Status |
|------|--------|
| `k8s/overlays/prod` → NS `plugin-market-data-prod` | applied (dbname `bifrost_prod`, redis `redis-queue-prod`) |
| PG / Redis NetworkPolicies allow `plugin-market-data-prod` | applied |
| Owner-safe `stock_daily` seed from `bifrost_dev` (NVDA + last 60d) | done (`~421k` rows; not full history) |
| Trade `:prod` images / bifrost-core **0.5.2** via `make k3s-deliver-prod` | done (`bifrost-deliver-prod-1785945612`) |

## Still Owner-gated

- Full historical `stock_daily` backfill (optional; DEV has ~3.2M rows)
- `MARKET_DATA_FRESHNESS_DB=bifrost_prod` for PROD Gallery seat

## Verify

```bash
export KUBECONFIG=~/.kube/bifrost-k3s.yaml
PRIMARY=$(kubectl -n data get cluster bifrost-postgres -o jsonpath='{.status.currentPrimary}')
kubectl -n data exec "$PRIMARY" -c postgres -- psql -d bifrost_prod -tAc "
SELECT 'legacy=' || count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name IN (
  'stock_day','tickers','job_massive_backfill','option_snapshots');
SELECT 'ticker=' || count(*) FROM market.ticker;
SELECT 'related_from_symbol=' || EXISTS (
  SELECT 1 FROM information_schema.columns
  WHERE table_schema='public' AND table_name='ticker_related_tickers' AND column_name='from_symbol');
"
```
