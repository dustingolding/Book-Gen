#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Run a production BookGen release path:
  1) upload input to object storage
  2) run in-cluster Kubernetes BookGen job
  3) optionally promote LakeFS branch to main

Required:
  --project-id <id>
  --input-path <local-file>

Optional:
  --object-key <key>                       (default: inputs/<project-id>/<filename>)
  --run-date <YYYY-MM-DD>                  (default: UTC today)
  --namespace <ns>                         (default: sideline-wire-dailycast)
  --image <repo:tag>                       (default: run_bookgen_k8s_job.sh default)
  --resource-profile <light|standard|heavy>(default: light)
  --bookgen-use-llm <true|false>           (default: true)
  --bookgen-llm-chapter-limit <int>        (default: 1)
  --bookgen-allow-lock-override <true|false> (default: false)
  --llm-timeout-seconds <int>              (default: 60)
  --llm-max-retries <int>                  (default: 2)
  --wait-timeout-seconds <int>             (default: 43200)
  --allow-concurrent                       (allow concurrent active jobs for same project id)
  --cleanup-old-jobs                       (delete terminal jobs for same project id before run)
  --promote                                (merge BookGen branch into main after success)
  --promote-dest-branch <name>             (default: main)
  --skip-preflight                         (skip cluster preflight checks)
  --dry-run                                (print resolved commands only)
EOF
}

PROJECT_ID=""
INPUT_PATH=""
OBJECT_KEY=""
RUN_DATE="$(date -u +%F)"
NAMESPACE="sideline-wire-dailycast"
IMAGE=""
RESOURCE_PROFILE="light"
BOOKGEN_USE_LLM="true"
BOOKGEN_LLM_CHAPTER_LIMIT="1"
BOOKGEN_ALLOW_LOCK_OVERRIDE="false"
LLM_TIMEOUT_SECONDS="60"
LLM_MAX_RETRIES="2"
WAIT_TIMEOUT_SECONDS="43200"
ALLOW_CONCURRENT="false"
CLEANUP_OLD_JOBS="false"
PROMOTE="false"
PROMOTE_DEST_BRANCH="main"
DRY_RUN="false"
RUN_PREFLIGHT="true"
pf_pids=()
RELEASE_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
JOB_NAME=""
POD_NAME=""
NODE_NAME=""
PROMOTED_REF=""
PYTHON_BIN=""

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

SOURCE_DOTENV="${BOOKGEN_SOURCE_DOTENV:-true}"
if [[ "${SOURCE_DOTENV}" == "true" && -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-id) PROJECT_ID="$2"; shift 2 ;;
    --input-path) INPUT_PATH="$2"; shift 2 ;;
    --object-key) OBJECT_KEY="$2"; shift 2 ;;
    --run-date) RUN_DATE="$2"; shift 2 ;;
    --namespace) NAMESPACE="$2"; shift 2 ;;
    --image) IMAGE="$2"; shift 2 ;;
    --resource-profile) RESOURCE_PROFILE="$2"; shift 2 ;;
    --bookgen-use-llm) BOOKGEN_USE_LLM="$2"; shift 2 ;;
    --bookgen-llm-chapter-limit) BOOKGEN_LLM_CHAPTER_LIMIT="$2"; shift 2 ;;
    --bookgen-allow-lock-override) BOOKGEN_ALLOW_LOCK_OVERRIDE="$2"; shift 2 ;;
    --llm-timeout-seconds) LLM_TIMEOUT_SECONDS="$2"; shift 2 ;;
    --llm-max-retries) LLM_MAX_RETRIES="$2"; shift 2 ;;
    --wait-timeout-seconds) WAIT_TIMEOUT_SECONDS="$2"; shift 2 ;;
    --allow-concurrent) ALLOW_CONCURRENT="true"; shift 1 ;;
    --cleanup-old-jobs) CLEANUP_OLD_JOBS="true"; shift 1 ;;
    --promote) PROMOTE="true"; shift 1 ;;
    --promote-dest-branch) PROMOTE_DEST_BRANCH="$2"; shift 2 ;;
    --skip-preflight) RUN_PREFLIGHT="false"; shift 1 ;;
    --dry-run) DRY_RUN="true"; shift 1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1"; usage; exit 1 ;;
  esac
done

if [[ -z "${PROJECT_ID}" || -z "${INPUT_PATH}" ]]; then
  echo "--project-id and --input-path are required."
  usage
  exit 1
fi

cleanup() {
  for pid in "${pf_pids[@]:-}"; do
    kill "${pid}" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

resolve_python_bin() {
  if [[ -x ".venv/bin/python" ]]; then
    PYTHON_BIN=".venv/bin/python"
    return
  fi
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
    return
  fi
  if command -v python >/dev/null 2>&1; then
    PYTHON_BIN="python"
    return
  fi
  echo "No Python interpreter found (.venv/bin/python, python3, or python)." >&2
  exit 1
}

endpoint_host() {
  local endpoint="$1"
  local without_scheme="${endpoint#*://}"
  without_scheme="${without_scheme%%/*}"
  echo "${without_scheme%%:*}"
}

endpoint_port() {
  local endpoint="$1"
  local without_scheme="${endpoint#*://}"
  without_scheme="${without_scheme%%/*}"
  if [[ "${without_scheme}" == *:* ]]; then
    echo "${without_scheme##*:}"
  else
    echo "80"
  fi
}

port_open() {
  local host="$1"
  local port="$2"
  (echo >/dev/tcp/"${host}"/"${port}") >/dev/null 2>&1
}

host_resolvable() {
  local host="$1"
  getent ahosts "${host}" >/dev/null 2>&1
}

project_job_prefix() {
  local raw="$1"
  local safe
  safe="$(printf '%s' "${raw}" | tr '[:upper:]' '[:lower:]' | tr -cs 'a-z0-9-' '-' | sed 's/^-*//; s/-*$//')"
  echo "bookgen-${safe}-"
}

ensure_no_active_project_job() {
  local prefix
  prefix="$(project_job_prefix "${PROJECT_ID}")"
  local active
  active="$(
    kubectl -n "${NAMESPACE}" get jobs -o jsonpath='{range .items[*]}{.metadata.name}{"|"}{.status.active}{"\n"}{end}' \
      | awk -F'|' -v p="${prefix}" '$1 ~ ("^" p) && ($2+0) > 0 {print $1}' \
      | head -n 1
  )"
  if [[ -n "${active}" ]]; then
    echo "ERROR: active BookGen job already running for project '${PROJECT_ID}': ${active}"
    echo "Use --allow-concurrent if you intentionally want overlapping runs."
    exit 1
  fi
}

cleanup_terminal_project_jobs() {
  local prefix
  prefix="$(project_job_prefix "${PROJECT_ID}")"
  mapfile -t jobs_to_delete < <(
    kubectl -n "${NAMESPACE}" get jobs -o jsonpath='{range .items[*]}{.metadata.name}{"|"}{.status.active}{"|"}{.status.succeeded}{"|"}{.status.failed}{"\n"}{end}' \
      | awk -F'|' -v p="${prefix}" '
          $1 ~ ("^" p) {
            active = ($2 == "" ? 0 : $2 + 0)
            succeeded = ($3 == "" ? 0 : $3 + 0)
            failed = ($4 == "" ? 0 : $4 + 0)
            if (active == 0 && (succeeded > 0 || failed > 0)) print $1
          }
        '
  )

  if [[ "${#jobs_to_delete[@]}" -eq 0 ]]; then
    return
  fi

  if [[ "${DRY_RUN}" == "true" ]]; then
    echo "Cleanup jobs:"
    for job in "${jobs_to_delete[@]}"; do
      echo "  ${job}"
    done
    return
  fi

  kubectl -n "${NAMESPACE}" delete job "${jobs_to_delete[@]}" >/dev/null
  echo "Deleted terminal jobs for project ${PROJECT_ID}: ${#jobs_to_delete[@]}"
}

ensure_minio_local_connectivity() {
  local endpoint="${MINIO_ENDPOINT:-}"
  local local_endpoint="${MINIO_LOCAL_ENDPOINT:-http://127.0.0.1:19000}"
  local host
  host="$(endpoint_host "${endpoint}")"
  if host_resolvable "${host}"; then
    return
  fi
  if [[ -z "${local_endpoint}" ]]; then
    return
  fi
  export MINIO_ENDPOINT="${local_endpoint}"

  local local_host
  local local_port
  local_host="$(endpoint_host "${local_endpoint}")"
  local_port="$(endpoint_port "${local_endpoint}")"

  if [[ "${local_host}" != "127.0.0.1" && "${local_host}" != "localhost" ]]; then
    return
  fi
  if port_open 127.0.0.1 "${local_port}"; then
    return
  fi
  kubectl -n "${NAMESPACE}" port-forward svc/minio "${local_port}:9000" >/tmp/minio-release-pf.log 2>&1 &
  pf_pids+=("$!")
  for _ in {1..20}; do
    if port_open 127.0.0.1 "${local_port}"; then
      return
    fi
    sleep 0.5
  done
  echo "Failed to establish MinIO port-forward at 127.0.0.1:${local_port}"
  tail -n 40 /tmp/minio-release-pf.log || true
  exit 1
}

ensure_lakefs_local_connectivity() {
  local endpoint="${LAKEFS_ENDPOINT:-}"
  local local_endpoint="${LAKEFS_LOCAL_ENDPOINT:-http://127.0.0.1:18000}"
  local host
  host="$(endpoint_host "${endpoint}")"
  if host_resolvable "${host}"; then
    return
  fi
  if [[ -z "${local_endpoint}" ]]; then
    return
  fi
  export LAKEFS_ENDPOINT="${local_endpoint}"

  local local_host
  local local_port
  local_host="$(endpoint_host "${local_endpoint}")"
  local_port="$(endpoint_port "${local_endpoint}")"

  if [[ "${local_host}" != "127.0.0.1" && "${local_host}" != "localhost" ]]; then
    return
  fi
  if port_open 127.0.0.1 "${local_port}"; then
    return
  fi
  kubectl -n "${NAMESPACE}" port-forward svc/lakefs "${local_port}:80" >/tmp/lakefs-release-pf.log 2>&1 &
  pf_pids+=("$!")
  for _ in {1..20}; do
    if port_open 127.0.0.1 "${local_port}"; then
      return
    fi
    sleep 0.5
  done
  echo "Failed to establish LakeFS port-forward at 127.0.0.1:${local_port}"
  tail -n 40 /tmp/lakefs-release-pf.log || true
  exit 1
}

if [[ -z "${OBJECT_KEY}" ]]; then
  OBJECT_KEY="inputs/${PROJECT_ID}/bookspec.json"
fi

JOB_NAME="$(project_job_prefix "${PROJECT_ID}")$(date -u +%s)"

if [[ "${RUN_PREFLIGHT}" == "true" ]]; then
  PREFLIGHT_CMD=(./scripts/bookgen_preflight.sh --namespace "${NAMESPACE}" --require-ops-worker)
  if [[ "${DRY_RUN}" == "true" ]]; then
    echo "Preflight command:"
    printf '  '
    printf '%q ' "${PREFLIGHT_CMD[@]}"
    printf '\n'
  else
    "${PREFLIGHT_CMD[@]}"
  fi
fi

if [[ "${CLEANUP_OLD_JOBS}" == "true" ]]; then
  cleanup_terminal_project_jobs
fi

if [[ "${ALLOW_CONCURRENT}" != "true" ]]; then
  ensure_no_active_project_job
fi

ensure_minio_local_connectivity
resolve_python_bin

UPLOAD_CMD=("${PYTHON_BIN}" scripts/upload_bookgen_input.py --project-id "${PROJECT_ID}" --input-path "${INPUT_PATH}")
if [[ -n "${OBJECT_KEY}" ]]; then
  UPLOAD_CMD+=(--object-key "${OBJECT_KEY}")
fi

if [[ "${DRY_RUN}" == "true" ]]; then
  echo "Upload command:"
  printf '  PYTHONPATH=. '
  printf '%q ' "${UPLOAD_CMD[@]}"
  printf '\n'
else
  # Capture printed key from upload script.
  OBJECT_KEY="$(PYTHONPATH=. "${UPLOAD_CMD[@]}")"
fi

JOB_CMD=(
  ./scripts/run_bookgen_k8s_job.sh
  --project-id "${PROJECT_ID}"
  --bookspec-key "${OBJECT_KEY}"
  --job-name "${JOB_NAME}"
  --run-date "${RUN_DATE}"
  --namespace "${NAMESPACE}"
  --resource-profile "${RESOURCE_PROFILE}"
  --bookgen-use-llm "${BOOKGEN_USE_LLM}"
  --bookgen-llm-chapter-limit "${BOOKGEN_LLM_CHAPTER_LIMIT}"
  --bookgen-allow-lock-override "${BOOKGEN_ALLOW_LOCK_OVERRIDE}"
  --llm-timeout-seconds "${LLM_TIMEOUT_SECONDS}"
  --llm-max-retries "${LLM_MAX_RETRIES}"
  --wait-timeout-seconds "${WAIT_TIMEOUT_SECONDS}"
  --wait
)
if [[ -n "${IMAGE}" ]]; then
  JOB_CMD+=(--image "${IMAGE}")
fi

if [[ "${DRY_RUN}" == "true" ]]; then
  echo "Job command:"
  printf '  '
  printf '%q ' "${JOB_CMD[@]}"
  printf '\n'
else
  "${JOB_CMD[@]}"
  POD_NAME="$(kubectl -n "${NAMESPACE}" get pod -l job-name="${JOB_NAME}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  NODE_NAME="$(kubectl -n "${NAMESPACE}" get pod -l job-name="${JOB_NAME}" -o jsonpath='{.items[0].spec.nodeName}' 2>/dev/null || true)"
fi

if [[ "${PROMOTE}" == "true" ]]; then
  ensure_lakefs_local_connectivity
  REQUIRED_ASSEMBLY_KEY="runs/${PROJECT_ID}/meta/stages/assembly-export.json"
  REQUIRED_ASSEMBLY_ARTIFACTS_KEY="runs/${PROJECT_ID}/meta/stages/assembly-export.artifacts.json"
  PROMOTE_CMD=(
    "${PYTHON_BIN}" scripts/promote_bookgen_branch.py
    --project-id "${PROJECT_ID}"
    --dest-branch "${PROMOTE_DEST_BRANCH}"
    --require-object-path "${REQUIRED_ASSEMBLY_KEY}"
    --require-object-path "${REQUIRED_ASSEMBLY_ARTIFACTS_KEY}"
  )
  if [[ "${DRY_RUN}" == "true" ]]; then
    echo "Promote command:"
    printf '  PYTHONPATH=. '
    printf '%q ' "${PROMOTE_CMD[@]}"
    printf '\n'
  else
    PROMOTE_OUTPUT="$(PYTHONPATH=. "${PROMOTE_CMD[@]}")"
    printf '%s\n' "${PROMOTE_OUTPUT}"
    PROMOTED_REF="$(
      printf '%s' "${PROMOTE_OUTPUT}" | "${PYTHON_BIN}" -c 'import json,sys
s=sys.stdin.read().strip()
try:
    obj=json.loads(s)
except Exception:
    print("")
    raise SystemExit(0)
print(obj.get("reference",""))'
    )"
  fi
fi

RELEASE_FINISHED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

AUDIT_CMD=(
  "${PYTHON_BIN}" scripts/write_release_audit.py
  --project-id "${PROJECT_ID}"
  --run-date "${RUN_DATE}"
  --bookspec-key "${OBJECT_KEY}"
  --job-name "${JOB_NAME}"
  --namespace "${NAMESPACE}"
  --status "succeeded"
  --image "${IMAGE:-ghcr.io/dustingolding/slw-dailycast-base:latest}"
  --resource-profile "${RESOURCE_PROFILE}"
  --llm-chapter-limit "${BOOKGEN_LLM_CHAPTER_LIMIT}"
  --wait-timeout-seconds "${WAIT_TIMEOUT_SECONDS}"
  --pod-name "${POD_NAME}"
  --node-name "${NODE_NAME}"
  --promoted-ref "${PROMOTED_REF}"
  --promoted-dest-branch "${PROMOTE_DEST_BRANCH}"
  --started-at "${RELEASE_STARTED_AT}"
  --finished-at "${RELEASE_FINISHED_AT}"
)

if [[ "${DRY_RUN}" == "true" ]]; then
  echo "Audit command:"
  printf '  PYTHONPATH=. '
  printf '%q ' "${AUDIT_CMD[@]}"
  printf '\n'
  exit 0
fi

if ! PYTHONPATH=. "${AUDIT_CMD[@]}"; then
  echo "WARNING: failed to write release audit metadata."
fi

echo "Release flow complete."
echo "project_id=${PROJECT_ID}"
echo "object_key=${OBJECT_KEY}"
