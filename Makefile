.PHONY: install-dev test lint db-init db-init-dry apply-roles apply-ownership rollback-ownership run-api kustomize-check verify-market-data sync-platform-write-token sync-write-auth-overlay install-redis-massive apply-external-names-massive

install-dev:
	pip install -e ".[dev]"

test:
	pytest -q

lint:
	ruff check src tests scripts

db-init:
	python scripts/init_schema.py

db-init-dry:
	python scripts/init_schema.py --dry-run

# Best-effort roles (CREATE ROLE / GRANT). Prefer elevated PG credentials via POSTGRES_*.
apply-roles:
	python scripts/init_schema.py --roles-only

# Elevated: give the plugin's role ownership of raw_market / ops_jobs so it can
# create and drop its own partitions. Needs superuser POSTGRES_* credentials.
# Without it, ensure_month_partitions starts failing in 2026-10 and inserts for
# 2027-01 have nowhere to land.
apply-ownership:
	psql "$${POSTGRES_ADMIN_URL:?set POSTGRES_ADMIN_URL to an elevated connection string}" \
	  -v ON_ERROR_STOP=1 -f scripts/fix_object_ownership.sql

# Undo the above. scripts/rollback_object_ownership.sql is a snapshot of who
# owned what before; regenerate it first if the catalogue has moved since.
rollback-ownership:
	psql "$${POSTGRES_ADMIN_URL:?set POSTGRES_ADMIN_URL to an elevated connection string}" \
	  -v ON_ERROR_STOP=1 -f scripts/rollback_object_ownership.sql

run-api:
	python scripts/run_api.py

kustomize-check:
	kubectl kustomize k8s/base >/dev/null

verify-market-data:
	bash scripts/verify-market-data.sh

sync-platform-write-token:
	bash scripts/sync-platform-write-token.sh

sync-write-auth-overlay:
	kubectl -n plugin-market-data create configmap market-data-api-schema-hotfix \
	  --from-file=deps.py=src/bifrost_market_data/api/deps.py \
	  --dry-run=client -o yaml | kubectl apply -f -
	kubectl -n plugin-market-data rollout restart deploy/market-data-api

install-redis-massive:
	./scripts/install-redis-massive.sh

apply-external-names-massive:
	./scripts/apply-external-names-massive.sh
