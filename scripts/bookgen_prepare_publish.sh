#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Prepare a BookGen project for publishing:
  1) run automated publish readiness gate
  2) generate publish candidate dossier + checklist
  3) export local publish bundle directory
  4) package publish bundle zip + checksums

Required:
  --project-id <id>

Optional:
  --require-promotion        require promotion reference during readiness gate
  --checklist-output <path>  override checklist output path
  --skip-export              skip local bundle export step
  --skip-package             skip package zip/checksum step
  --no-strict                allow readiness fail without non-zero exit (not recommended)
EOF
}

PROJECT_ID=""
REQUIRE_PROMOTION="false"
CHECKLIST_OUTPUT=""
STRICT="true"
SKIP_EXPORT="false"
SKIP_PACKAGE="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-id) PROJECT_ID="$2"; shift 2 ;;
    --require-promotion) REQUIRE_PROMOTION="true"; shift 1 ;;
    --checklist-output) CHECKLIST_OUTPUT="$2"; shift 2 ;;
    --skip-export) SKIP_EXPORT="true"; shift 1 ;;
    --skip-package) SKIP_PACKAGE="true"; shift 1 ;;
    --no-strict) STRICT="false"; shift 1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1"; usage; exit 1 ;;
  esac
done

if [[ -z "${PROJECT_ID}" ]]; then
  echo "--project-id is required."
  usage
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

READINESS_CMD=(
  .venv/bin/python scripts/bookgen_publish_readiness.py
  --project-id "${PROJECT_ID}"
)
if [[ "${REQUIRE_PROMOTION}" == "true" ]]; then
  READINESS_CMD+=(--require-promotion)
fi
if [[ "${STRICT}" != "true" ]]; then
  READINESS_CMD+=(--no-strict)
fi

echo "Running publish readiness gate..."
PYTHONPATH=. "${READINESS_CMD[@]}"

CANDIDATE_CMD=(
  .venv/bin/python scripts/bookgen_publish_candidate.py
  --project-id "${PROJECT_ID}"
)
if [[ -n "${CHECKLIST_OUTPUT}" ]]; then
  CANDIDATE_CMD+=(--checklist-output "${CHECKLIST_OUTPUT}")
fi

echo "Generating publish candidate dossier..."
PYTHONPATH=. "${CANDIDATE_CMD[@]}"

if [[ "${SKIP_EXPORT}" != "true" ]]; then
  echo "Exporting local publish bundle..."
  PYTHONPATH=. .venv/bin/python scripts/bookgen_export_publish_bundle.py \
    --project-id "${PROJECT_ID}"
fi

if [[ "${SKIP_PACKAGE}" != "true" ]]; then
  echo "Packaging publish bundle..."
  PYTHONPATH=. .venv/bin/python scripts/bookgen_package_publish_bundle.py \
    --project-id "${PROJECT_ID}"
fi

echo "Publish preparation complete for ${PROJECT_ID}."
