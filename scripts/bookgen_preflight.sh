#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
BookGen Kubernetes preflight checks.

Optional:
  --namespace <ns>            (default: sideline-wire-dailycast)
  --require-ops-worker        require at least one Ready worker with label slw/workload=ops
EOF
}

NAMESPACE="sideline-wire-dailycast"
REQUIRE_OPS_WORKER="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace) NAMESPACE="$2"; shift 2 ;;
    --require-ops-worker) REQUIRE_OPS_WORKER="true"; shift 1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1"; usage; exit 1 ;;
  esac
done

if ! command -v kubectl >/dev/null 2>&1; then
  echo "ERROR: kubectl is not installed or not in PATH."
  exit 1
fi

kubectl cluster-info >/dev/null
kubectl get namespace "${NAMESPACE}" >/dev/null
kubectl -n "${NAMESPACE}" get svc minio >/dev/null
kubectl -n "${NAMESPACE}" get svc lakefs >/dev/null

READY_WORKERS="$(kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"|"}{.metadata.labels.node-role\.kubernetes\.io/control-plane}{"|"}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' \
  | awk -F'|' '$2=="" && $3=="True" {print $1}' | wc -l)"

if [[ "${READY_WORKERS}" -lt 1 ]]; then
  echo "ERROR: no Ready worker nodes available (control-plane excluded)."
  exit 1
fi

if [[ "${REQUIRE_OPS_WORKER}" == "true" ]]; then
  OPS_READY="$(kubectl get nodes -l slw/workload=ops -o jsonpath='{range .items[*]}{.metadata.name}{"|"}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' \
    | awk -F'|' '$2=="True" {print $1}' | wc -l)"
  if [[ "${OPS_READY}" -lt 1 ]]; then
    echo "ERROR: no Ready nodes match label slw/workload=ops."
    exit 1
  fi
fi

echo "Preflight OK"
echo "namespace=${NAMESPACE}"
echo "ready_workers=${READY_WORKERS}"
if [[ "${REQUIRE_OPS_WORKER}" == "true" ]]; then
  echo "require_ops_worker=true"
fi
