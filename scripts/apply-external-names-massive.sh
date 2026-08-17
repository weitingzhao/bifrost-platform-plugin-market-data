#!/usr/bin/env bash
# Apply redis-massive ExternalName services to Trade + Plugin namespaces.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EXT_DIR="$ROOT/k8s/external-names"

for ns in bifrost-dev bifrost-stg bifrost-prod plugin-market-data; do
  manifest="$EXT_DIR/$ns/redis-massive.yaml"
  if [[ -f "$manifest" ]]; then
    kubectl apply -f "$manifest"
    echo "Applied redis-massive ExternalName in $ns"
  else
    echo "WARN: $manifest not found, skipping" >&2
  fi
done

echo "Done. Verify: kubectl get svc redis-massive -n bifrost-dev"
