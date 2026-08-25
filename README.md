# bifrost-platform-plugin-market-data

Bifrost **Market Data Subcontractor** — Polygon.io market data ingestion into PostgreSQL schemas `raw_market.*` and `ops_jobs.*`.

## Scope

- K8s-native asyncio workers (no Celery)
- Shared physical Postgres with schema isolation
- Independent namespace: `plugin-market-data`
- No Interactive Brokers data path

## Quick start

```bash
pip install -e ".[dev]"
make lint
make test
```

## Layout

```
src/bifrost_market_data/
  schema/      # DDL (P1)
  polygon/     # REST client (P2)
  worker/      # PG-as-broker loop (P3)
  ingest/      # upsert handlers (P4)
  scheduler/   # CronJob enqueue (P5)
k8s/base/      # Deployments + NetworkPolicy
```

## Program

Delivery Board: `market-data-subcontractor` · lane `polygon-vendor`.
