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
  if [[ -z "${!var:-}" || "${!var}" == change-me-* || "${!var}" == changeme ]]; then
    echo "Set $var in $ENV_FILE before install (not changeme)." >&2
    exit 1
  fi
done

# Build ACL via Python so passwords with quotes/dashes never break shell sed.
ACL=$(
  ENV_FILE="$ENV_FILE" ACL_EXAMPLE="$ROOT/k8s/redis-massive/acl.conf.example" python3 - <<'PY'
import os
from pathlib import Path

env_path = Path(os.environ["ENV_FILE"])
vals = {}
for line in env_path.read_text().splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, _, v = line.partition("=")
    vals[k.strip()] = v.strip().strip('"').strip("'")

replacements = {
    "POLYGON_WS_PASS": vals["REDIS_MASSIVE_POLYGON_WS_PASS"],
    "TRADE_PROD_PASS": vals["REDIS_MASSIVE_TRADE_PROD_PASS"],
    "TRADE_DEV_PASS": vals["REDIS_MASSIVE_TRADE_DEV_PASS"],
    "PLATFORM_PASS": vals["REDIS_MASSIVE_PLATFORM_PASS"],
}

lines = []
for raw in Path(os.environ["ACL_EXAMPLE"]).read_text().splitlines():
    s = raw.strip()
    if not s or s.startswith("#"):
        continue
    for placeholder, password in replacements.items():
        s = s.replace(f">{placeholder}", f">{password}")
    lines.append(s)
print("\n".join(lines))
PY
)

kubectl create namespace data --dry-run=client -o yaml | kubectl apply -f -

kubectl create secret generic redis-massive-acl \
  --namespace=data \
  --from-literal=acl.conf="$ACL" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -k "$ROOT/k8s/redis-massive"
# Ensure replicas in case a prior crash/scale left it at 0
kubectl -n data scale deploy/redis-massive --replicas=1

echo "redis-massive applied. Verify: kubectl get pods,svc -n data -l app.kubernetes.io/name=redis-massive"
