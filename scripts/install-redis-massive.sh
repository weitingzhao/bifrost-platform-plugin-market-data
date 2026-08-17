#!/usr/bin/env bash
# Apply redis-massive to data NS — creates ACL secret from .env then kubectl apply -k.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${ENV_FILE:-$ROOT/.env}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing $ENV_FILE — copy .env.example and set passwords." >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$ENV_FILE"

for var in REDIS_MASSIVE_POLYGON_WS_PASS REDIS_MASSIVE_TRADE_PROD_PASS REDIS_MASSIVE_TRADE_DEV_PASS REDIS_MASSIVE_PLATFORM_PASS; do
  if [[ -z "${!var:-}" || "${!var}" == change-me-* ]]; then
    echo "Set $var in $ENV_FILE before install." >&2
    exit 1
  fi
done

ACL=$(sed \
  -e "s|>POLYGON_WS_PASS|>${REDIS_MASSIVE_POLYGON_WS_PASS}|g" \
  -e "s|>TRADE_PROD_PASS|>${REDIS_MASSIVE_TRADE_PROD_PASS}|g" \
  -e "s|>TRADE_DEV_PASS|>${REDIS_MASSIVE_TRADE_DEV_PASS}|g" \
  -e "s|>PLATFORM_PASS|>${REDIS_MASSIVE_PLATFORM_PASS}|g" \
  "$ROOT/k8s/redis-massive/acl.conf.example")

kubectl create namespace data --dry-run=client -o yaml | kubectl apply -f -

kubectl create secret generic redis-massive-acl \
  --namespace=data \
  --from-literal=acl.conf="$ACL" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -k "$ROOT/k8s/redis-massive"

echo "redis-massive applied. Verify: kubectl get pods,svc -n data -l app.kubernetes.io/name=redis-massive"
