#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Assess BookGen go-live readiness for production publishing.

Checks:
  - self-hosted runner online count
  - latest nightly integrity workflow status/conclusion/age
  - latest successful self-hosted publish-gate run
  - optional project handoff summary integrity

Usage:
  ./scripts/bookgen_go_live_readiness.sh --repo <owner/repo> [options]

Options:
  --repo <owner/repo>             GitHub repository (required)
  --project-id <id>               Optional project ID; validates local handoff summary
  --max-nightly-age-hours <int>   Max allowed age for nightly success (default: 36)
  -h, --help                      Show help
EOF
}

REPO=""
PROJECT_ID=""
MAX_NIGHTLY_AGE_HOURS=36

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo) REPO="$2"; shift 2 ;;
    --project-id) PROJECT_ID="$2"; shift 2 ;;
    --max-nightly-age-hours) MAX_NIGHTLY_AGE_HOURS="$2"; shift 2 ;;
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
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required." >&2
  exit 1
fi

online_runners="$(gh api "repos/${REPO}/actions/runners" --jq '[.runners[] | select(.status=="online")] | length')"
if [[ -z "${online_runners}" || "${online_runners}" == "null" ]]; then
  online_runners="0"
fi
runner_ok="false"
if [[ "${online_runners}" != "0" ]]; then
  runner_ok="true"
fi

nightly_json="$(gh run list \
  --repo "${REPO}" \
  --workflow bookgen-nightly-integrity.yml \
  --limit 1 \
  --json databaseId,status,conclusion,createdAt \
  --jq '.[0]')"

nightly_run_id="$(printf '%s' "${nightly_json}" | python3 -c 'import json,sys; d=json.load(sys.stdin) if sys.stdin.readable() else {}; print(d.get("databaseId",""))' 2>/dev/null || true)"
nightly_status="$(printf '%s' "${nightly_json}" | python3 -c 'import json,sys; d=json.load(sys.stdin) if sys.stdin.readable() else {}; print(d.get("status",""))' 2>/dev/null || true)"
nightly_conclusion="$(printf '%s' "${nightly_json}" | python3 -c 'import json,sys; d=json.load(sys.stdin) if sys.stdin.readable() else {}; print(d.get("conclusion",""))' 2>/dev/null || true)"
nightly_created_at="$(printf '%s' "${nightly_json}" | python3 -c 'import json,sys; d=json.load(sys.stdin) if sys.stdin.readable() else {}; print(d.get("createdAt",""))' 2>/dev/null || true)"

nightly_age_hours=""
nightly_fresh="false"
if [[ -n "${nightly_created_at}" ]]; then
  nightly_age_hours="$(python3 - "${nightly_created_at}" <<'PY'
import datetime as dt
import sys
created = dt.datetime.fromisoformat(sys.argv[1].replace("Z","+00:00"))
now = dt.datetime.now(dt.timezone.utc)
age_h = (now - created).total_seconds() / 3600.0
print(f"{age_h:.2f}")
PY
)"
  nightly_fresh="$(python3 - "${nightly_age_hours}" "${MAX_NIGHTLY_AGE_HOURS}" <<'PY'
import sys
age=float(sys.argv[1]); lim=float(sys.argv[2])
print("true" if age <= lim else "false")
PY
)"
fi

nightly_ok="false"
if [[ "${nightly_conclusion}" == "success" && "${nightly_fresh}" == "true" ]]; then
  nightly_ok="true"
fi

selfhosted_success_run_id="$(gh run list \
  --repo "${REPO}" \
  --workflow bookgen-publish-gate-selfhosted.yml \
  --limit 20 \
  --json databaseId,conclusion \
  --jq '.[] | select(.conclusion=="success") | .databaseId' | head -n1)"
selfhosted_ok="false"
if [[ -n "${selfhosted_success_run_id}" ]]; then
  selfhosted_ok="true"
fi

project_summary_ok="not_checked"
project_summary_path=""
if [[ -n "${PROJECT_ID}" ]]; then
  project_summary_path="exports/publish-handoffs/${PROJECT_ID}.json"
  if [[ -f "${project_summary_path}" ]]; then
    project_summary_ok="$(python3 - "${project_summary_path}" <<'PY'
import json,sys
p=sys.argv[1]
with open(p,"r",encoding="utf-8") as f:
    d=json.load(f)
pkg=d.get("package_checksum_status")
bndl=d.get("bundle_checksum_status")
print("true" if pkg=="pass" and bndl=="pass" else "false")
PY
)"
  else
    project_summary_ok="false"
  fi
fi

overall_ok="true"
if [[ "${runner_ok}" != "true" || "${nightly_ok}" != "true" || "${selfhosted_ok}" != "true" ]]; then
  overall_ok="false"
fi
if [[ "${project_summary_ok}" == "false" ]]; then
  overall_ok="false"
fi

echo "go_live_ready=${overall_ok}"
echo "runner_online_count=${online_runners} (ok=${runner_ok})"
echo "latest_nightly_run=${nightly_run_id} status=${nightly_status} conclusion=${nightly_conclusion} age_hours=${nightly_age_hours:-unknown} (ok=${nightly_ok})"
echo "latest_selfhosted_success_run=${selfhosted_success_run_id:-none} (ok=${selfhosted_ok})"
if [[ -n "${PROJECT_ID}" ]]; then
  echo "project_summary=${project_summary_path} (ok=${project_summary_ok})"
fi

echo "{"
echo "  \"repo\": \"${REPO}\","
echo "  \"go_live_ready\": ${overall_ok},"
echo "  \"runner_online_count\": ${online_runners},"
echo "  \"runner_ok\": ${runner_ok},"
echo "  \"nightly\": {"
echo "    \"run_id\": \"${nightly_run_id}\","
echo "    \"status\": \"${nightly_status}\","
echo "    \"conclusion\": \"${nightly_conclusion}\","
echo "    \"age_hours\": \"${nightly_age_hours}\","
echo "    \"max_age_hours\": ${MAX_NIGHTLY_AGE_HOURS},"
echo "    \"ok\": ${nightly_ok}"
echo "  },"
echo "  \"selfhosted_publish_gate\": {"
echo "    \"latest_success_run_id\": \"${selfhosted_success_run_id}\","
echo "    \"ok\": ${selfhosted_ok}"
echo "  },"
if [[ -n "${PROJECT_ID}" ]]; then
  echo "  \"project\": {"
  echo "    \"project_id\": \"${PROJECT_ID}\","
  echo "    \"summary_path\": \"${project_summary_path}\","
  echo "    \"summary_ok\": ${project_summary_ok}"
  echo "  }"
else
  echo "  \"project\": null"
fi
echo "}"
