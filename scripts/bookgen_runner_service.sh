#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Manage the local GitHub self-hosted runner as a systemd service.

Usage:
  ./scripts/bookgen_runner_service.sh --repo <owner/repo> [--runner-dir <path>] <install|start|stop|restart|status|ensure-online>

Defaults:
  --runner-dir ~/actions-runner-bookgen

Examples:
  ./scripts/bookgen_runner_service.sh --repo dustingolding/Book-Gen install
  ./scripts/bookgen_runner_service.sh --repo dustingolding/Book-Gen ensure-online
EOF
}

REPO=""
RUNNER_DIR="${HOME}/actions-runner-bookgen"
ACTION=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo) REPO="$2"; shift 2 ;;
    --runner-dir) RUNNER_DIR="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    install|start|stop|restart|status|ensure-online) ACTION="$1"; shift 1 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "${REPO}" || -z "${ACTION}" ]]; then
  echo "--repo and action are required." >&2
  usage
  exit 1
fi

if ! command -v gh >/dev/null 2>&1; then
  echo "gh CLI is required." >&2
  exit 1
fi
if [[ ! -d "${RUNNER_DIR}" ]]; then
  echo "Runner dir not found: ${RUNNER_DIR}" >&2
  exit 1
fi
if [[ ! -x "${RUNNER_DIR}/svc.sh" ]]; then
  echo "svc.sh not found in ${RUNNER_DIR}" >&2
  exit 1
fi

run_svc() {
  local cmd="$1"
  (cd "${RUNNER_DIR}" && sudo ./svc.sh "${cmd}")
}

check_online() {
  gh api "repos/${REPO}/actions/runners" \
    --jq '[.runners[] | select(.status=="online")] | length'
}

case "${ACTION}" in
  install)
    run_svc install
    ;;
  start)
    run_svc start
    ;;
  stop)
    run_svc stop
    ;;
  restart)
    run_svc stop || true
    run_svc start
    ;;
  status)
    run_svc status
    online="$(check_online || true)"
    echo "GitHub online runners: ${online:-unknown}"
    ;;
  ensure-online)
    if [[ ! -f "${RUNNER_DIR}/.service" ]]; then
      run_svc install
    fi
    run_svc start || true
    online="0"
    for _ in {1..10}; do
      online="$(check_online || true)"
      if [[ "${online:-0}" != "0" ]]; then
        break
      fi
      sleep 2
    done
    if [[ "${online:-0}" == "0" ]]; then
      echo "Runner service command completed but no online runners detected in ${REPO}." >&2
      exit 1
    fi
    echo "Runner online count: ${online}"
    ;;
esac
