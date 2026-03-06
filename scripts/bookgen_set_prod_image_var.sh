#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Set the GitHub repository variable used as BookGen production image pin.

Usage:
  ./scripts/bookgen_set_prod_image_var.sh --repo <owner/repo> --image <repo:tag>
EOF
}

REPO=""
IMAGE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo) REPO="$2"; shift 2 ;;
    --image) IMAGE="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "${REPO}" || -z "${IMAGE}" ]]; then
  usage
  exit 1
fi

gh variable set BOOKGEN_PROD_IMAGE --repo "${REPO}" --body "${IMAGE}"
echo "Set BOOKGEN_PROD_IMAGE=${IMAGE} for ${REPO}"
