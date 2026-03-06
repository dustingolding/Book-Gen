#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Run bounded regression validation across both production series.

Usage:
  ./scripts/bookgen_regression_gate.sh [options]

Options:
  --image <repo:tag>                Default pinned image
  --repo <owner/repo>               Default: dustingolding/Book-Gen
  --resource-profile <profile>      Default: light
  --llm-chapter-limit <int>         Default: 1
  --skip-handoff                    Skip strict GitHub handoff workflow; run local readiness check instead
  -h, --help                        Show help
EOF
}

IMAGE="ghcr.io/dustingolding/slw-dailycast-base:git-b1a801f-20260306163947"
REPO="dustingolding/Book-Gen"
RESOURCE_PROFILE="light"
LLM_CHAPTER_LIMIT="1"
SKIP_HANDOFF="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --image) IMAGE="$2"; shift 2 ;;
    --repo) REPO="$2"; shift 2 ;;
    --resource-profile) RESOURCE_PROFILE="$2"; shift 2 ;;
    --llm-chapter-limit) LLM_CHAPTER_LIMIT="$2"; shift 2 ;;
    --skip-handoff) SKIP_HANDOFF="true"; shift 1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

PYTHON_BIN=""
if [[ -x ".venv/bin/python" ]]; then
  PYTHON_BIN=".venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "Python interpreter not found (.venv/bin/python, python3, or python)." >&2
  exit 1
fi

for series in closed-session time-tinkers; do
  echo "== Regression gate: ${series} =="
  run_cmd=(
    ./scripts/bookgen_series_release.sh
    --series "${series}" \
    --mode bounded \
    --llm-chapter-limit "${LLM_CHAPTER_LIMIT}" \
    --resource-profile "${RESOURCE_PROFILE}" \
    --image "${IMAGE}" \
    --repo "${REPO}"
  )
  if [[ "${SKIP_HANDOFF}" == "true" ]]; then
    run_cmd+=(--skip-handoff)
  fi
  "${run_cmd[@]}"

  if [[ "${SKIP_HANDOFF}" == "true" ]]; then
    project_id=""
    case "${series}" in
      closed-session) project_id="closed-session-llm-pilot-003" ;;
      time-tinkers) project_id="time-tinkers-lab" ;;
    esac
    if [[ -n "${project_id}" ]]; then
      PYTHONPATH=. "${PYTHON_BIN}" scripts/bookgen_publish_readiness.py --project-id "${project_id}" --require-promotion
    fi
  fi
done

echo "Regression gate passed for closed-session and time-tinkers."
