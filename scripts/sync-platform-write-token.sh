#!/usr/bin/env bash
# Copy Plugin write-token into platform-api namespaces (Console enqueue hop).
# Does not print the secret. Idempotent kubectl apply.
set -euo pipefail

export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/bifrost-k3s.yaml}"
SRC_NS="${SRC_NS:-plugin-market-data}"
SRC_SECRET="${SRC_SECRET:-market-data-secrets}"
SRC_KEY="${SRC_KEY:-write-token}"
DST_SECRET="${DST_SECRET:-market-data-write-token}"
DST_KEY="${DST_KEY:-write-token}"

tmp="$(mktemp)"
trap 'rm -f "${tmp}"' EXIT
chmod 600 "${tmp}"
kubectl -n "${SRC_NS}" get secret "${SRC_SECRET}" -o jsonpath="{.data.${SRC_KEY}}" | base64 -d > "${tmp}"
if [[ ! -s "${tmp}" ]]; then
  echo "missing ${SRC_NS}/${SRC_SECRET} key ${SRC_KEY}" >&2
  exit 1
fi

for ns in bifrost-platform-stg bifrost-platform-prod; do
  kubectl -n "${ns}" create secret generic "${DST_SECRET}" \
    --from-file="${DST_KEY}=${tmp}" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  echo "synced ${DST_SECRET} → ${ns}"
done
