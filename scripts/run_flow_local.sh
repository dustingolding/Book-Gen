#!/usr/bin/env bash
set -euo pipefail

RUN_DATE="${1:-$(date -u +%F)}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "${ROOT_DIR}"

echo "WARNING: local flow execution uses this machine's CPU/RAM."
echo "For production BookGen runs, use scripts/run_bookgen_k8s_job.sh instead."

if [[ -f ".venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  . ".venv/bin/activate"
fi

if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

pf_pids=()

cleanup() {
  for pid in "${pf_pids[@]:-}"; do
    kill "${pid}" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

port_open() {
  local host="$1"
  local port="$2"
  (echo >/dev/tcp/"${host}"/"${port}") >/dev/null 2>&1
}

ensure_port_forward() {
  local ns="$1"
  local svc="$2"
  local local_port="$3"
  local remote_port="$4"

  if port_open 127.0.0.1 "${local_port}"; then
    return
  fi

  kubectl -n "${ns}" port-forward "svc/${svc}" "${local_port}:${remote_port}" >/tmp/"${svc}"-pf.log 2>&1 &
  local pf_pid=$!
  pf_pids+=("${pf_pid}")

  for _ in {1..20}; do
    if port_open 127.0.0.1 "${local_port}"; then
      return
    fi
    sleep 0.5
  done

  echo "Failed to establish port-forward for ${svc} (${local_port}:${remote_port})"
  echo "Log:"
  tail -n 40 /tmp/"${svc}"-pf.log || true
  exit 1
}

ensure_port_forward "sideline-wire-dailycast" "postgres" "15432" "5432"
ensure_port_forward "sideline-wire-dailycast" "minio" "19000" "9000"
ensure_port_forward "sideline-wire-dailycast" "prefect-server" "14200" "4200"
ensure_port_forward "log-anomaly" "mlflow" "15000" "5000"

export PG_HOST=127.0.0.1
export PG_PORT=15432
export MINIO_ENDPOINT=http://127.0.0.1:19000
export MINIO_SECURE=false
# Local in-cluster MinIO defaults; override if your cluster uses different creds.
export MINIO_ACCESS_KEY="${MINIO_LOCAL_ACCESS_KEY:-slwminioadmin}"
export MINIO_SECRET_KEY="${MINIO_LOCAL_SECRET_KEY:-slw-minio-secret-change-me}"
export MLFLOW_TRACKING_URI=http://127.0.0.1:15000
export PREFECT_API_URL=http://127.0.0.1:14200/api

python -m app.cli init-db
python -m app.cli run-flow --run-date "$RUN_DATE"
