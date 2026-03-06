#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Run bounded regression validation across both production series with strict handoff.

Usage:
  ./scripts/bookgen_regression_gate.sh [options]

Options:
  --image <repo:tag>                Default pinned image
  --repo <owner/repo>               Default: dustingolding/Book-Gen
  --resource-profile <profile>      Default: light
  --llm-chapter-limit <int>         Default: 1
  -h, --help                        Show help
EOF
}

IMAGE="ghcr.io/dustingolding/slw-dailycast-base:git-b1a801f-20260306163947"
REPO="dustingolding/Book-Gen"
RESOURCE_PROFILE="light"
LLM_CHAPTER_LIMIT="1"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --image) IMAGE="$2"; shift 2 ;;
    --repo) REPO="$2"; shift 2 ;;
    --resource-profile) RESOURCE_PROFILE="$2"; shift 2 ;;
    --llm-chapter-limit) LLM_CHAPTER_LIMIT="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

for series in closed-session time-tinkers; do
  echo "== Regression gate: ${series} =="
  ./scripts/bookgen_series_release.sh \
    --series "${series}" \
    --mode bounded \
    --llm-chapter-limit "${LLM_CHAPTER_LIMIT}" \
    --resource-profile "${RESOURCE_PROFILE}" \
    --image "${IMAGE}" \
    --repo "${REPO}"
done

echo "Regression gate passed for closed-session and time-tinkers."

