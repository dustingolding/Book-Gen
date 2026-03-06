#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Run the final BookGen publish handoff sequence:
  1) trigger publish gate (default: self-hosted) and wait
  2) download publish artifact
  3) verify checksum
  4) write handoff summary JSON

Usage:
  ./scripts/bookgen_release_handoff.sh --repo <owner/repo> --project-id <id> [options]

Options:
  --repo <owner/repo>            GitHub repository (required)
  --project-id <id>              BookGen project ID (required)
  --workflow <file>              Workflow file (default: bookgen-publish-gate-selfhosted.yml)
  --github-hosted                Shortcut for --workflow bookgen-publish-gate.yml
  --no-require-promotion         Pass through to publish gate trigger
  --collect-only                 Skip trigger; only collect artifact
  --run-id <id>                  Run ID to collect from (required with --collect-only)
  --output-dir <path>            Artifact output dir (default: exports/publish-artifacts/<run-id>)
  --summary-path <path>          Summary JSON path (default: exports/publish-handoffs/<project-id>.json)
  --skip-runner-check            Skip online self-hosted runner precheck (not recommended)
  -h, --help                     Show help
EOF
}

REPO=""
PROJECT_ID=""
WORKFLOW_FILE="bookgen-publish-gate-selfhosted.yml"
REQUIRE_PROMOTION_FLAG=""
COLLECT_ONLY="false"
RUN_ID=""
OUTPUT_DIR=""
SUMMARY_PATH=""
SKIP_RUNNER_CHECK="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo) REPO="$2"; shift 2 ;;
    --project-id) PROJECT_ID="$2"; shift 2 ;;
    --workflow) WORKFLOW_FILE="$2"; shift 2 ;;
    --github-hosted) WORKFLOW_FILE="bookgen-publish-gate.yml"; shift 1 ;;
    --no-require-promotion) REQUIRE_PROMOTION_FLAG="--no-require-promotion"; shift 1 ;;
    --collect-only) COLLECT_ONLY="true"; shift 1 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
    --summary-path) SUMMARY_PATH="$2"; shift 2 ;;
    --skip-runner-check) SKIP_RUNNER_CHECK="true"; shift 1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "${REPO}" || -z "${PROJECT_ID}" ]]; then
  echo "--repo and --project-id are required." >&2
  usage
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if [[ "${COLLECT_ONLY}" != "true" ]]; then
  if [[ "${WORKFLOW_FILE}" == "bookgen-publish-gate-selfhosted.yml" ]] && [[ "${SKIP_RUNNER_CHECK}" != "true" ]]; then
    online_runners="$(gh api "repos/${REPO}/actions/runners" --jq '[.runners[] | select(.status=="online")] | length')"
    if [[ -z "${online_runners}" ]]; then
      online_runners="0"
    fi
    if [[ "${online_runners}" == "0" ]]; then
      echo "No online self-hosted runners found for ${REPO}; aborting dispatch." >&2
      echo "Bring a runner online or re-run with --skip-runner-check." >&2
      exit 1
    fi
  fi

  ./scripts/bookgen_trigger_publish_gate.sh \
    --repo "${REPO}" \
    --workflow "${WORKFLOW_FILE}" \
    --project-id "${PROJECT_ID}" \
    ${REQUIRE_PROMOTION_FLAG} \
    --wait

  RUN_ID="$(gh run list \
    --repo "${REPO}" \
    --workflow "${WORKFLOW_FILE}" \
    --limit 1 \
    --json databaseId,conclusion \
    --jq 'map(select(.conclusion=="success")) | .[0].databaseId // ""')"
fi

if [[ -z "${RUN_ID}" ]]; then
  RUN_ID="$(gh run list \
    --repo "${REPO}" \
    --workflow "${WORKFLOW_FILE}" \
    --limit 20 \
    --json databaseId,conclusion \
    --jq 'map(select(.conclusion=="success")) | .[0].databaseId // ""')"
  if [[ -z "${RUN_ID}" ]]; then
    echo "Could not resolve successful run ID." >&2
    exit 1
  fi
fi

collect_cmd=(
  ./scripts/bookgen_collect_publish_artifact.sh
  --repo "${REPO}"
  --project-id "${PROJECT_ID}"
  --run-id "${RUN_ID}"
)
if [[ -n "${OUTPUT_DIR}" ]]; then
  collect_cmd+=(--output-dir "${OUTPUT_DIR}")
fi

collect_output="$("${collect_cmd[@]}")"
printf '%s\n' "${collect_output}"

if [[ -z "${SUMMARY_PATH}" ]]; then
  SUMMARY_PATH="exports/publish-handoffs/${PROJECT_ID}.json"
fi
mkdir -p "$(dirname "${SUMMARY_PATH}")"

package_dir="$(printf '%s\n' "${collect_output}" | sed -n 's/^  "package_dir": "\(.*\)",$/\1/p' | tail -n1)"
artifact_name="$(printf '%s\n' "${collect_output}" | sed -n 's/^  "artifact_name": "\(.*\)",$/\1/p' | tail -n1)"
package_checksum_status="$(printf '%s\n' "${collect_output}" | sed -n 's/^  "package_checksum_status": "\(.*\)",$/\1/p' | tail -n1)"
bundle_checksum_status="$(printf '%s\n' "${collect_output}" | sed -n 's/^  "bundle_checksum_status": "\(.*\)"$/\1/p' | tail -n1)"

cat > "${SUMMARY_PATH}" <<EOF
{
  "project_id": "${PROJECT_ID}",
  "repo": "${REPO}",
  "workflow_file": "${WORKFLOW_FILE}",
  "run_id": ${RUN_ID},
  "artifact_name": "${artifact_name}",
  "package_dir": "${package_dir}",
  "package_checksum_status": "${package_checksum_status}",
  "bundle_checksum_status": "${bundle_checksum_status}"
}
EOF

echo "Wrote handoff summary: ${SUMMARY_PATH}"
