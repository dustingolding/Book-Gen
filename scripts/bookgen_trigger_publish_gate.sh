#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Trigger the manual GitHub workflow `bookgen-publish-gate`.

Usage:
  ./scripts/bookgen_trigger_publish_gate.sh --project-id <id> [--repo owner/name] [--workflow <file>] [--github-hosted] [--no-require-promotion] [--wait]

Options:
  --project-id <id>         BookGen project ID
  --repo <owner/name>       GitHub repo (default: inferred from git remote origin)
  --workflow <file>         Workflow file name (default: bookgen-publish-gate-selfhosted.yml)
  --github-hosted           Shortcut for --workflow bookgen-publish-gate.yml
  --no-require-promotion    Set workflow input require_promotion=false
  --wait                    Wait for workflow completion and print final status
  -h, --help                Show help
EOF
}

PROJECT_ID=""
REPO=""
REQUIRE_PROMOTION="true"
WAIT="false"
WORKFLOW_FILE="bookgen-publish-gate-selfhosted.yml"
DISPATCH_RETRIES=3

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-id) PROJECT_ID="$2"; shift 2 ;;
    --repo) REPO="$2"; shift 2 ;;
    --workflow) WORKFLOW_FILE="$2"; shift 2 ;;
    --github-hosted) WORKFLOW_FILE="bookgen-publish-gate.yml"; shift 1 ;;
    --no-require-promotion) REQUIRE_PROMOTION="false"; shift 1 ;;
    --wait) WAIT="true"; shift 1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "${PROJECT_ID}" ]]; then
  echo "--project-id is required." >&2
  usage
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if ! command -v gh >/dev/null 2>&1; then
  echo "gh CLI is required." >&2
  exit 1
fi

if [[ -z "${REPO}" ]]; then
  remote_url="$(git config --get remote.origin.url || true)"
  if [[ -z "${remote_url}" ]]; then
    echo "Could not infer repo from git remote; pass --repo owner/name." >&2
    exit 1
  fi
  REPO="$(echo "${remote_url}" | sed -E 's#(git@github.com:|https://github.com/)##; s#\.git$##')"
fi

echo "Dispatching workflow ${WORKFLOW_FILE} for repo ${REPO}..."
attempt=1
while true; do
  if [[ "${WORKFLOW_FILE}" == "bookgen-publish-gate.yml" ]]; then
    set +e
    out="$(
      gh workflow run "${WORKFLOW_FILE}" \
        --repo "${REPO}" \
        -f project_id="${PROJECT_ID}" \
        -f require_promotion="${REQUIRE_PROMOTION}" \
        -f allow_github_hosted=true 2>&1
    )"
    rc=$?
    set -e
  else
    set +e
    out="$(
      gh workflow run "${WORKFLOW_FILE}" \
        --repo "${REPO}" \
        -f project_id="${PROJECT_ID}" \
        -f require_promotion="${REQUIRE_PROMOTION}" 2>&1
    )"
    rc=$?
    set -e
  fi

  if [[ "${rc}" -eq 0 ]]; then
    break
  fi

  if [[ "${attempt}" -lt "${DISPATCH_RETRIES}" ]] && grep -qi "HTTP 5[0-9][0-9]" <<<"${out}"; then
    sleep_secs=$(( attempt * 2 ))
    echo "Dispatch attempt ${attempt}/${DISPATCH_RETRIES} failed with server error; retrying in ${sleep_secs}s..."
    sleep "${sleep_secs}"
    attempt=$(( attempt + 1 ))
    continue
  fi

  echo "${out}" >&2
  exit "${rc}"
done

echo "Workflow dispatched: project_id=${PROJECT_ID}, require_promotion=${REQUIRE_PROMOTION}"

if [[ "${WAIT}" != "true" ]]; then
  exit 0
fi

echo "Resolving latest workflow run ID..."
RUN_ID="$(gh run list \
  --repo "${REPO}" \
  --workflow "${WORKFLOW_FILE}" \
  --limit 1 \
  --json databaseId \
  --jq '.[0].databaseId')"

if [[ -z "${RUN_ID}" || "${RUN_ID}" == "null" ]]; then
  echo "Could not resolve workflow run ID for ${WORKFLOW_FILE}." >&2
  exit 1
fi

echo "Waiting for completion (run ${RUN_ID})..."
gh run watch "${RUN_ID}" --repo "${REPO}" --exit-status
