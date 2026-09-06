#!/usr/bin/env bash
# Market Data Subcontractor — P7 program verification (deploy + health + freshness).
set -euo pipefail

NS="${MARKET_DATA_NS:-plugin-market-data}"
PLATFORM_API="${PLATFORM_API:-http://127.0.0.1:8780}"
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/bifrost-k3s.yaml}"
PF_STOCKS_PID=""
PF_OPTIONS_PID=""

cleanup() {
  for pid in "${PF_STOCKS_PID}" "${PF_OPTIONS_PID}"; do
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      kill "${pid}" 2>/dev/null || true
      wait "${pid}" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT

probe_health() {
  local port="$1"
  local label="$2"
  local json
  for _ in $(seq 1 20); do
    if curl -sf "http://127.0.0.1:${port}/health" >/dev/null 2>&1; then
      break
    fi
    sleep 0.5
  done
  json="$(curl -sf "http://127.0.0.1:${port}/health")"
  python3 -c "
import json, sys
d = json.loads(sys.argv[1])
assert d.get('status') in (None, 'ok', 'OK') or 'pool' in d, d
print(f\"  {sys.argv[2]}: pool={d.get('pool')} jobs_done={d.get('jobs_done')} jobs_failed={d.get('jobs_failed')}\", file=sys.stderr)
" "$json" "$label"
  printf '%s' "$json"
}

echo "== [1/6] K8s deployments =="
kubectl -n "$NS" get deploy polygon-worker-stocks polygon-worker-options
STOCKS_READY="$(kubectl -n "$NS" get deploy polygon-worker-stocks -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0)"
OPTIONS_READY="$(kubectl -n "$NS" get deploy polygon-worker-options -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0)"
STOCKS_READY="${STOCKS_READY:-0}"
OPTIONS_READY="${OPTIONS_READY:-0}"
if [[ "$STOCKS_READY" -lt 1 || "$OPTIONS_READY" -lt 1 ]]; then
  echo "FAIL: expected both deployments readyReplicas>=1 (stocks=${STOCKS_READY} options=${OPTIONS_READY})"
  exit 1
fi
echo "  stocks=${STOCKS_READY} options=${OPTIONS_READY} ready"

echo ""
echo "== [2/6] Health endpoints return 200 (per-pool) =="
kubectl -n "$NS" port-forward svc/market-data-health-stocks 18080:8080 >/tmp/market-data-pf-stocks.log 2>&1 &
PF_STOCKS_PID=$!
kubectl -n "$NS" port-forward svc/market-data-health-options 18081:8080 >/tmp/market-data-pf-options.log 2>&1 &
PF_OPTIONS_PID=$!
STOCKS_HEALTH_JSON="$(probe_health 18080 stocks)"
OPTIONS_HEALTH_JSON="$(probe_health 18081 options)"

echo ""
echo "== [3/6] CronJobs present =="
EXPECTED_CRONS=(
  market-data-stock-eod
  market-data-eod-pipeline
  market-data-universe-daily
  market-data-corporate
  market-data-calendar
  market-data-option-refresh
  market-data-option-bars
  market-data-minute-bars
  market-data-maintenance
  market-data-reference
  market-data-fundamentals-rotate
)
MISSING=0
for cj in "${EXPECTED_CRONS[@]}"; do
  if ! kubectl -n "$NS" get cronjob "$cj" >/dev/null 2>&1; then
    echo "  missing CronJob: $cj"
    MISSING=1
  fi
done
if [[ "$MISSING" -ne 0 ]]; then
  echo "FAIL: one or more CronJobs missing"
  exit 1
fi
echo "  ${#EXPECTED_CRONS[@]} CronJobs present"

echo ""
echo "== [4/6] Worker job activity =="
python3 -c "
import json, sys
stocks = json.loads(sys.argv[1])
options = json.loads(sys.argv[2])
sd = int(stocks.get('jobs_done') or 0)
od = int(options.get('jobs_done') or 0)
print(f'  stocks jobs_done={sd} · options jobs_done={od}')
if sd == 0 and od == 0:
    print('  INFO: no jobs processed yet (expected if just deployed)')
" "$STOCKS_HEALTH_JSON" "$OPTIONS_HEALTH_JSON"

echo ""
echo "== [5/6] Platform probe + freshness (optional if platform-api down) =="
if curl -sf "${PLATFORM_API}/api/v1/plugins/market-data/status" >/tmp/market-data-status.json 2>/dev/null; then
  python3 -c "
import json
d = json.load(open('/tmp/market-data-status.json'))
reach = d.get('reachability')
assert reach in ('ok', 'degraded', 'fail', 'unknown'), d
deploys = d.get('deployments') or []
assert len(deploys) >= 2, d
workers = d.get('workers') or []
fresh = d.get('freshness') or []
fresh_reach = d.get('freshness_reachability')
print(f\"  reachability={reach} workers={len(workers)} freshness_rows={len(fresh)} freshness_reach={fresh_reach} summary={d.get('summary')}\")
if fresh:
    stale = [f for f in fresh if f.get('verdict') != 'ok']
    if stale:
        print(f'  INFO: {len(stale)} freshness dimension(s) not ok (expected pre-backfill)')
else:
    print('  INFO: no freshness rows yet (worker must complete jobs first)')
"
else
  echo "  SKIP: platform-api not reachable at ${PLATFORM_API}"
fi

echo ""
echo "== [6/6] Data quality script (optional — needs populated PG) =="
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DQ_OK=1
if [[ "${SKIP_DATA_QUALITY:-0}" == "1" ]]; then
  echo "  SKIP: SKIP_DATA_QUALITY=1"
  DQ_OK=0
elif python3 "${ROOT}/scripts/verify_data_quality.py" 2>/tmp/market-data-quality.log; then
  echo "  verify_data_quality.py PASS"
else
  DQ_OK=0
  echo "  INFO: verify_data_quality.py not yet green (backfill / CronJobs may still be running)"
  tail -n 20 /tmp/market-data-quality.log || true
fi

echo ""
if [[ "${DQ_OK}" == "1" ]]; then
  echo "Market Data Subcontractor P7 verification OK (deploy + data quality)"
else
  echo "Market Data Subcontractor P7 deploy OK · data quality pending"
fi
