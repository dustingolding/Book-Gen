#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Build and publish SLW images with a pinned tag, then print BookGen run command.

Usage:
  ./scripts/bookgen_publish_image.sh [options]

Optional:
  --registry <repo>     (default: ghcr.io/dustingolding)
  --tag <tag>           (default: git-<shortsha>-<utcdate>)
  --project-id <id>     (for printed run command; optional)
  --bookspec-key <key>  (for printed run command; optional)
  --namespace <ns>      (default: sideline-wire-dailycast)
  -h, --help

Requires:
  - docker logged in to target registry
EOF
}

REGISTRY="ghcr.io/dustingolding"
TAG=""
PROJECT_ID=""
BOOKSPEC_KEY=""
NAMESPACE="sideline-wire-dailycast"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --registry) REGISTRY="$2"; shift 2 ;;
    --tag) TAG="$2"; shift 2 ;;
    --project-id) PROJECT_ID="$2"; shift 2 ;;
    --bookspec-key) BOOKSPEC_KEY="$2"; shift 2 ;;
    --namespace) NAMESPACE="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1"; usage; exit 1 ;;
  esac
done

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if [[ -z "${TAG}" ]]; then
  if command -v git >/dev/null 2>&1 && git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    SHORT_SHA="$(git rev-parse --short HEAD)"
  else
    SHORT_SHA="manual"
  fi
  TAG="git-${SHORT_SHA}-$(date -u +%Y%m%d%H%M%S)"
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required."
  exit 1
fi

./scripts/build_images.sh "${REGISTRY}" "${TAG}"

BASE_IMAGE="${REGISTRY}/slw-dailycast-base:${TAG}"
echo
echo "Published BookGen runtime image:"
echo "  ${BASE_IMAGE}"
echo
echo "Example in-cluster run command:"
if [[ -n "${PROJECT_ID}" && -n "${BOOKSPEC_KEY}" ]]; then
  echo "./scripts/run_bookgen_k8s_job.sh \\"
  echo "  --project-id ${PROJECT_ID} \\"
  echo "  --bookspec-key ${BOOKSPEC_KEY} \\"
  echo "  --namespace ${NAMESPACE} \\"
  echo "  --image ${BASE_IMAGE} \\"
  echo "  --with-analytics-report true \\"
  echo "  --wait"
else
  echo "./scripts/run_bookgen_k8s_job.sh --project-id <id> --bookspec-key <key> --image ${BASE_IMAGE} --with-analytics-report true --wait"
fi
