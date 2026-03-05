#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Download and verify publish package artifact from a BookGen publish-gate run.

Usage:
  ./scripts/bookgen_collect_publish_artifact.sh --repo <owner/repo> [options]

Options:
  --repo <owner/repo>          GitHub repository (required)
  --run-id <id>                Workflow run ID (default: latest successful self-hosted publish-gate run)
  --artifact-name <name>       Explicit artifact name
  --project-id <id>            Resolve artifact name as publish-package-<project-id>
  --output-dir <path>          Output directory (default: exports/publish-artifacts/<run-id>)
  -h, --help                   Show help
EOF
}

REPO=""
RUN_ID=""
ARTIFACT_NAME=""
PROJECT_ID=""
OUTPUT_DIR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo) REPO="$2"; shift 2 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    --artifact-name) ARTIFACT_NAME="$2"; shift 2 ;;
    --project-id) PROJECT_ID="$2"; shift 2 ;;
    --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "${REPO}" ]]; then
  echo "--repo is required." >&2
  usage
  exit 1
fi

if ! command -v gh >/dev/null 2>&1; then
  echo "gh CLI is required." >&2
  exit 1
fi
if ! command -v sha256sum >/dev/null 2>&1; then
  echo "sha256sum is required." >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required." >&2
  exit 1
fi

if [[ -z "${RUN_ID}" ]]; then
  RUN_ID="$(gh run list \
    --repo "${REPO}" \
    --workflow bookgen-publish-gate-selfhosted.yml \
    --limit 20 \
    --json databaseId,conclusion \
    --jq '.[] | select(.conclusion=="success") | .databaseId' | head -n1)"
fi

if [[ -z "${RUN_ID}" ]]; then
  echo "Could not resolve a successful self-hosted publish-gate run ID." >&2
  exit 1
fi

if [[ -n "${PROJECT_ID}" && -z "${ARTIFACT_NAME}" ]]; then
  ARTIFACT_NAME="publish-package-${PROJECT_ID}"
fi

if [[ -z "${ARTIFACT_NAME}" ]]; then
  ARTIFACT_NAME="$(gh api "repos/${REPO}/actions/runs/${RUN_ID}/artifacts" --jq '.artifacts[0].name')"
fi

if [[ -z "${ARTIFACT_NAME}" || "${ARTIFACT_NAME}" == "null" ]]; then
  echo "No artifacts found for run ${RUN_ID}." >&2
  exit 1
fi

if [[ -z "${OUTPUT_DIR}" ]]; then
  OUTPUT_DIR="exports/publish-artifacts/${RUN_ID}"
fi
rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"

echo "Downloading artifact '${ARTIFACT_NAME}' from run ${RUN_ID}..."
gh run download "${RUN_ID}" \
  --repo "${REPO}" \
  --name "${ARTIFACT_NAME}" \
  --dir "${OUTPUT_DIR}"

pkg_dir=""
if [[ -f "${OUTPUT_DIR}/SHA256SUMS.txt" ]]; then
  pkg_dir="${OUTPUT_DIR}"
elif [[ -f "${OUTPUT_DIR}/${ARTIFACT_NAME}/SHA256SUMS.txt" ]]; then
  pkg_dir="${OUTPUT_DIR}/${ARTIFACT_NAME}"
else
  pkg_dir="$(find "${OUTPUT_DIR}" -maxdepth 10 -type f -name 'SHA256SUMS.txt' -printf '%h\n' | head -n1 || true)"
fi

if [[ -z "${pkg_dir}" ]]; then
  echo "Could not find SHA256SUMS.txt under ${OUTPUT_DIR}." >&2
  exit 1
fi
pkg_dir_abs="$(cd "${pkg_dir}" && pwd)"

echo "Verifying package checksums in ${pkg_dir}..."
(
  cd "${pkg_dir}"
  sha256sum -c SHA256SUMS.txt
)

bundle_checksum_status="not_available"
if [[ -f "${pkg_dir}/BUNDLE_SHA256SUMS.txt" ]]; then
  pkg_zip="$(find "${pkg_dir}" -maxdepth 1 -type f -name '*.zip' | head -n1 || true)"
  if [[ -z "${pkg_zip}" ]]; then
    echo "BUNDLE_SHA256SUMS.txt found but no zip package present in ${pkg_dir}." >&2
    exit 1
  fi
  tmp_extract_dir="$(mktemp -d)"
  trap 'rm -rf "${tmp_extract_dir}"' EXIT

  echo "Verifying unpacked bundle checksums..."
  python3 - "${pkg_zip}" "${tmp_extract_dir}" <<'PY'
import pathlib
import sys
import zipfile

zip_path = pathlib.Path(sys.argv[1])
target_dir = pathlib.Path(sys.argv[2])
with zipfile.ZipFile(zip_path) as zf:
    zf.extractall(target_dir)
PY
  (
    cd "${tmp_extract_dir}"
    sha256sum -c "${pkg_dir_abs}/BUNDLE_SHA256SUMS.txt"
  )
  bundle_checksum_status="pass"
fi

echo "{"
echo "  \"repo\": \"${REPO}\","
echo "  \"run_id\": ${RUN_ID},"
echo "  \"artifact_name\": \"${ARTIFACT_NAME}\","
echo "  \"package_dir\": \"${pkg_dir}\","
echo "  \"package_checksum_status\": \"pass\","
echo "  \"bundle_checksum_status\": \"${bundle_checksum_status}\""
echo "}"
