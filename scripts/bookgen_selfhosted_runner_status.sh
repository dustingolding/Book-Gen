#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Show self-hosted runner availability and latest publish-gate run state.

Usage:
  ./scripts/bookgen_selfhosted_runner_status.sh [--repo owner/name]
EOF
}

REPO=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo) REPO="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

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

echo "Repository: ${REPO}"
echo
echo "Self-hosted runners:"
gh api "repos/${REPO}/actions/runners" --jq '.runners[] | {name, status, busy, labels: [.labels[].name]}' || true

echo
echo "Latest self-hosted publish-gate runs:"
gh run list --repo "${REPO}" --workflow bookgen-publish-gate-selfhosted.yml --limit 5 \
  --json databaseId,status,conclusion,createdAt,displayTitle \
  --jq '.[] | {run_id: .databaseId, status, conclusion, created_at: .createdAt, title: .displayTitle}' || true
