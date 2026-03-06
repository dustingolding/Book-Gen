#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Run a standardized BookGen production release + strict handoff for a known series.

Usage:
  ./scripts/bookgen_series_release.sh --series <closed-session|time-tinkers> [options]

Options:
  --series <name>                   Required. One of: closed-session, time-tinkers
  --mode <bounded|full>             Default: bounded
  --llm-chapter-limit <int>         Override chapter cap (bounded default: 1, full default: 0)
  --resource-profile <profile>      Default: light
  --image <repo:tag>                Default pinned image
  --repo <owner/repo>               Default: dustingolding/Book-Gen
  --skip-handoff                    Run release only; skip strict handoff
  --no-promote                      Skip LakeFS promotion (handoff with require_promotion will fail)
  -h, --help                        Show help
EOF
}

SERIES=""
MODE="bounded"
LLM_CHAPTER_LIMIT=""
RESOURCE_PROFILE="light"
IMAGE="ghcr.io/dustingolding/slw-dailycast-base:git-b1a801f-20260306163947"
REPO="dustingolding/Book-Gen"
SKIP_HANDOFF="false"
PROMOTE="true"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --series) SERIES="$2"; shift 2 ;;
    --mode) MODE="$2"; shift 2 ;;
    --llm-chapter-limit) LLM_CHAPTER_LIMIT="$2"; shift 2 ;;
    --resource-profile) RESOURCE_PROFILE="$2"; shift 2 ;;
    --image) IMAGE="$2"; shift 2 ;;
    --repo) REPO="$2"; shift 2 ;;
    --skip-handoff) SKIP_HANDOFF="true"; shift 1 ;;
    --no-promote) PROMOTE="false"; shift 1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "${SERIES}" ]]; then
  echo "--series is required." >&2
  usage
  exit 1
fi

case "${MODE}" in
  bounded|full) ;;
  *)
    echo "--mode must be bounded or full." >&2
    exit 1
    ;;
esac

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

case "${SERIES}" in
  closed-session)
    PROJECT_ID="closed-session-llm-pilot-003"
    INPUT_PATH="${ROOT_DIR}/docs/bookgen/bookspec.closed-session-llm-pilot-003.json"
    ;;
  time-tinkers)
    PROJECT_ID="time-tinkers-lab"
    INPUT_PATH="${ROOT_DIR}/docs/bookgen/bookspec.time-tinkers-lab.json"
    ;;
  *)
    echo "--series must be one of: closed-session, time-tinkers." >&2
    exit 1
    ;;
esac

if [[ ! -f "${INPUT_PATH}" ]]; then
  echo "Input file not found: ${INPUT_PATH}" >&2
  exit 1
fi

if [[ -z "${LLM_CHAPTER_LIMIT}" ]]; then
  if [[ "${MODE}" == "full" ]]; then
    LLM_CHAPTER_LIMIT="0"
  else
    LLM_CHAPTER_LIMIT="1"
  fi
fi

release_cmd=(
  ./scripts/run_bookgen_release.sh
  --project-id "${PROJECT_ID}"
  --input-path "${INPUT_PATH}"
  --resource-profile "${RESOURCE_PROFILE}"
  --bookgen-llm-chapter-limit "${LLM_CHAPTER_LIMIT}"
  --bookgen-allow-lock-override true
  --image "${IMAGE}"
  --wait-timeout-seconds 43200
)
if [[ "${PROMOTE}" == "true" ]]; then
  release_cmd+=(--promote)
fi

echo "Running series release:"
printf '  %q ' "${release_cmd[@]}"
printf '\n'
"${release_cmd[@]}"

if [[ "${SKIP_HANDOFF}" == "true" ]]; then
  echo "Skipping handoff by request (--skip-handoff)."
  exit 0
fi

handoff_cmd=(
  ./scripts/bookgen_release_handoff.sh
  --repo "${REPO}"
  --project-id "${PROJECT_ID}"
)
echo "Running strict handoff:"
printf '  %q ' "${handoff_cmd[@]}"
printf '\n'
"${handoff_cmd[@]}"
