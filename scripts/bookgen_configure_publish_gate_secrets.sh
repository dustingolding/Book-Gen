#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Validate or configure GitHub secrets required by the bookgen-publish-gate workflow.

Default mode is dry-run (validation only).

Usage:
  ./scripts/bookgen_configure_publish_gate_secrets.sh [--env-file .env] [--repo owner/name] [--apply]

Options:
  --env-file <path>   Environment file to source (default: .env)
  --repo <owner/name> GitHub repo for secret writes (default: inferred from git remote origin)
  --apply             Write secrets using `gh secret set` (otherwise dry-run)
  -h, --help          Show help
EOF
}

ENV_FILE=".env"
REPO=""
APPLY="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file) ENV_FILE="$2"; shift 2 ;;
    --repo) REPO="$2"; shift 2 ;;
    --apply) APPLY="true"; shift 1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Env file not found: ${ENV_FILE}" >&2
  exit 1
fi

# shellcheck disable=SC1090
set -a; source "${ENV_FILE}"; set +a

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
  # Supports git@github.com:owner/repo.git and https://github.com/owner/repo(.git)
  repo_guess="$(echo "${remote_url}" | sed -E 's#(git@github.com:|https://github.com/)##; s#\.git$##')"
  if [[ "${repo_guess}" != */* ]]; then
    echo "Failed to parse GitHub repo from remote URL: ${remote_url}" >&2
    exit 1
  fi
  REPO="${repo_guess}"
fi

map_secret_value() {
  local secret_name="$1"
  case "${secret_name}" in
    MINIO_ENDPOINT) printf '%s' "${MINIO_ENDPOINT:-}" ;;
    MINIO_ACCESS_KEY) printf '%s' "${MINIO_ACCESS_KEY:-}" ;;
    MINIO_SECRET_KEY) printf '%s' "${MINIO_SECRET_KEY:-}" ;;
    MINIO_SECURE) printf '%s' "${MINIO_SECURE:-}" ;;
    S3_BUCKET) printf '%s' "${S3_BUCKET:-}" ;;
    LAKEFS_ENDPOINT) printf '%s' "${LAKEFS_ENDPOINT:-}" ;;
    LAKEFS_ACCESS_KEY) printf '%s' "${LAKEFS_ACCESS_KEY:-}" ;;
    LAKEFS_SECRET_KEY) printf '%s' "${LAKEFS_SECRET_KEY:-}" ;;
    LAKEFS_REPOSITORY)
      if [[ -n "${LAKEFS_REPOSITORY:-}" ]]; then
        printf '%s' "${LAKEFS_REPOSITORY}"
      else
        printf '%s' "${LAKEFS_REPO:-}"
      fi
      ;;
    LAKEFS_BRANCH)
      if [[ -n "${LAKEFS_BRANCH:-}" ]]; then
        printf '%s' "${LAKEFS_BRANCH}"
      else
        printf '%s' "${LAKEFS_SOURCE_BRANCH:-main}"
      fi
      ;;
    *)
      return 1
      ;;
  esac
}

required=(
  MINIO_ENDPOINT
  MINIO_ACCESS_KEY
  MINIO_SECRET_KEY
  S3_BUCKET
  LAKEFS_ENDPOINT
  LAKEFS_ACCESS_KEY
  LAKEFS_SECRET_KEY
  LAKEFS_REPOSITORY
  LAKEFS_BRANCH
)
optional=(
  MINIO_SECURE
)

echo "Repository: ${REPO}"
echo "Mode: $([[ "${APPLY}" == "true" ]] && echo apply || echo dry-run)"

missing_required=0
for key in "${required[@]}"; do
  value="$(map_secret_value "${key}")"
  if [[ -z "${value}" ]]; then
    echo "MISSING required: ${key}" >&2
    missing_required=1
  else
    echo "OK required: ${key}"
  fi
done

for key in "${optional[@]}"; do
  value="$(map_secret_value "${key}")"
  if [[ -z "${value}" ]]; then
    echo "SKIP optional (unset): ${key}"
  else
    echo "OK optional: ${key}"
  fi
done

if [[ "${missing_required}" -ne 0 ]]; then
  echo "Cannot continue due to missing required values." >&2
  exit 1
fi

if [[ "${APPLY}" != "true" ]]; then
  echo "Dry-run complete. Re-run with --apply to write GitHub secrets."
  exit 0
fi

for key in "${required[@]}" "${optional[@]}"; do
  value="$(map_secret_value "${key}")"
  if [[ -z "${value}" ]]; then
    continue
  fi
  printf '%s' "${value}" | gh secret set "${key}" --repo "${REPO}" --body -
  echo "SET secret: ${key}"
done

echo "Secret configuration complete."
