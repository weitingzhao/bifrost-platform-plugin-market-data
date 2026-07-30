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

## Still Owner-gated

- Permanent PROD workers NS (e.g. `plugin-market-data-prod`) — **not** applied
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
