#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Run a bounded local BookGen pipeline with optional MinIO port-forward and analytics summary.

Usage:
  ./scripts/bookgen_local_run.sh --project-id <id> [--bookspec-path <path> | --bookspec-key <key>] [options]

Required:
  --project-id <id>
  One of:
    --bookspec-path <path>      Local BookSpec JSON/MD/YAML path
    --bookspec-key <key>        Existing object-store key (for previously uploaded spec)

Optional:
  --run-date <YYYY-MM-DD>       (default: UTC today)
  --namespace <ns>              (default: sideline-wire-dailycast)
  --minio-local-port <port>     (default: 19000)
  --no-port-forward             Do not start kubectl port-forward
  --lakefs-enabled <bool>       (default: false for local runs)
  --bookgen-use-llm <bool>      (default: false)
  --bookgen-eval-use-llm <bool> (default: false)
  --bookgen-rewrite-use-llm <bool> (default: false)
  --analytics-report-path <path> Write analytics JSON output to file
  -h, --help

Notes:
  - This script is local-only and may use local CPU/RAM.
  - For in-cluster production runs, use scripts/run_bookgen_k8s_job.sh.
EOF
}

PROJECT_ID=""
BOOKSPEC_PATH=""
BOOKSPEC_KEY=""
RUN_DATE="$(date -u +%F)"
NAMESPACE="sideline-wire-dailycast"
MINIO_LOCAL_PORT="19000"
NO_PORT_FORWARD="false"
LAKEFS_ENABLED_VALUE="false"
BOOKGEN_USE_LLM_VALUE="false"
BOOKGEN_EVAL_USE_LLM_VALUE="false"
BOOKGEN_REWRITE_USE_LLM_VALUE="false"
ANALYTICS_REPORT_PATH=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-id) PROJECT_ID="$2"; shift 2 ;;
    --bookspec-path) BOOKSPEC_PATH="$2"; shift 2 ;;
    --bookspec-key) BOOKSPEC_KEY="$2"; shift 2 ;;
    --run-date) RUN_DATE="$2"; shift 2 ;;
    --namespace) NAMESPACE="$2"; shift 2 ;;
    --minio-local-port) MINIO_LOCAL_PORT="$2"; shift 2 ;;
    --no-port-forward) NO_PORT_FORWARD="true"; shift 1 ;;
    --lakefs-enabled) LAKEFS_ENABLED_VALUE="$2"; shift 2 ;;
    --bookgen-use-llm) BOOKGEN_USE_LLM_VALUE="$2"; shift 2 ;;
    --bookgen-eval-use-llm) BOOKGEN_EVAL_USE_LLM_VALUE="$2"; shift 2 ;;
    --bookgen-rewrite-use-llm) BOOKGEN_REWRITE_USE_LLM_VALUE="$2"; shift 2 ;;
    --analytics-report-path) ANALYTICS_REPORT_PATH="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1"; usage; exit 1 ;;
  esac
done

if [[ -z "${PROJECT_ID}" ]]; then
  echo "--project-id is required."
  usage
  exit 1
fi
if [[ -n "${BOOKSPEC_PATH}" && -n "${BOOKSPEC_KEY}" ]]; then
  echo "Use either --bookspec-path or --bookspec-key, not both."
  exit 1
fi
if [[ -z "${BOOKSPEC_PATH}" && -z "${BOOKSPEC_KEY}" ]]; then
  echo "One of --bookspec-path or --bookspec-key is required."
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

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

PYTHON_BIN=".venv/bin/python"
if [[ ! -x "${PYTHON_BIN}" ]]; then
  PYTHON_BIN="$(command -v python3)"
fi

port_open() {
  local host="$1"
  local port="$2"
  (echo >/dev/tcp/"${host}"/"${port}") >/dev/null 2>&1
}

pf_pid=""
cleanup() {
  if [[ -n "${pf_pid}" ]]; then
    kill "${pf_pid}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [[ "${NO_PORT_FORWARD}" != "true" ]]; then
  if ! command -v kubectl >/dev/null 2>&1; then
    echo "kubectl is required unless --no-port-forward is used."
    exit 1
  fi
  if ! port_open 127.0.0.1 "${MINIO_LOCAL_PORT}"; then
    kubectl -n "${NAMESPACE}" port-forward svc/minio "${MINIO_LOCAL_PORT}:9000" >/tmp/bookgen-minio-pf.log 2>&1 &
    pf_pid=$!
    for _ in {1..20}; do
      if port_open 127.0.0.1 "${MINIO_LOCAL_PORT}"; then
        break
      fi
      sleep 0.5
    done
    if ! port_open 127.0.0.1 "${MINIO_LOCAL_PORT}"; then
      echo "Failed to establish MinIO port-forward on ${MINIO_LOCAL_PORT}."
      tail -n 40 /tmp/bookgen-minio-pf.log || true
      exit 1
    fi
  fi
  export MINIO_ENDPOINT="http://127.0.0.1:${MINIO_LOCAL_PORT}"
fi

export LAKEFS_ENABLED="${LAKEFS_ENABLED_VALUE}"
export BOOKGEN_USE_LLM="${BOOKGEN_USE_LLM_VALUE}"
export BOOKGEN_EVAL_USE_LLM="${BOOKGEN_EVAL_USE_LLM_VALUE}"
export BOOKGEN_REWRITE_USE_LLM="${BOOKGEN_REWRITE_USE_LLM_VALUE}"

bookgen_cmd=("${PYTHON_BIN}" -m app.cli bookgen --project-id "${PROJECT_ID}" --run-date "${RUN_DATE}")
if [[ -n "${BOOKSPEC_PATH}" ]]; then
  bookgen_cmd+=(--bookspec-path "${BOOKSPEC_PATH}")
else
  bookgen_cmd+=(--bookspec-key "${BOOKSPEC_KEY}")
fi

echo "Running BookGen locally for project_id=${PROJECT_ID} run_date=${RUN_DATE}"
"${bookgen_cmd[@]}"

echo "Rendering analytics report..."
analytics_json="$("${PYTHON_BIN}" -m app.cli bookgen-analytics-report --project-id "${PROJECT_ID}")"
printf '%s\n' "${analytics_json}"

if [[ -n "${ANALYTICS_REPORT_PATH}" ]]; then
  mkdir -p "$(dirname "${ANALYTICS_REPORT_PATH}")"
  printf '%s\n' "${analytics_json}" > "${ANALYTICS_REPORT_PATH}"
  echo "Wrote analytics report: ${ANALYTICS_REPORT_PATH}"
fi
