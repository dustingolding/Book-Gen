#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Bootstrap a GitHub self-hosted runner for BookGen workflows on a Linux host.

This script downloads the official actions runner, registers it to a repo,
and optionally starts it in the foreground.

Usage:
  ./scripts/bookgen_selfhosted_runner_quickstart.sh [options]

Options:
  --repo <owner/name>         GitHub repo (required)
  --runner-dir <path>         Install dir (default: ~/actions-runner-bookgen)
  --runner-name <name>        Runner name (default: <hostname>-bookgen)
  --labels <csv>              Runner labels (default: self-hosted,linux,bookgen)
  --runner-version <ver>      Override actions runner version (default: latest)
  --ephemeral                 Register as ephemeral runner
  --replace                   Replace existing runner with same name
  --start                     Start runner immediately in foreground (./run.sh)
  --print-token-only          Print registration token and exit
  -h, --help                  Show help

Prereqs:
  - gh CLI authenticated with repo admin rights
  - curl, tar
EOF
}

REPO=""
RUNNER_DIR="${HOME}/actions-runner-bookgen"
RUNNER_NAME="$(hostname)-bookgen"
LABELS="self-hosted,linux,bookgen"
RUNNER_VERSION=""
EPHEMERAL="false"
REPLACE="false"
START="false"
PRINT_TOKEN_ONLY="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo) REPO="$2"; shift 2 ;;
    --runner-dir) RUNNER_DIR="$2"; shift 2 ;;
    --runner-name) RUNNER_NAME="$2"; shift 2 ;;
    --labels) LABELS="$2"; shift 2 ;;
    --runner-version) RUNNER_VERSION="$2"; shift 2 ;;
    --ephemeral) EPHEMERAL="true"; shift 1 ;;
    --replace) REPLACE="true"; shift 1 ;;
    --start) START="true"; shift 1 ;;
    --print-token-only) PRINT_TOKEN_ONLY="true"; shift 1 ;;
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
if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required." >&2
  exit 1
fi
if ! command -v tar >/dev/null 2>&1; then
  echo "tar is required." >&2
  exit 1
fi

echo "Requesting registration token for ${REPO}..."
REG_TOKEN="$(gh api -X POST "repos/${REPO}/actions/runners/registration-token" --jq '.token')"
if [[ -z "${REG_TOKEN}" || "${REG_TOKEN}" == "null" ]]; then
  echo "Failed to get runner registration token." >&2
  exit 1
fi

if [[ "${PRINT_TOKEN_ONLY}" == "true" ]]; then
  printf '%s\n' "${REG_TOKEN}"
  exit 0
fi

if [[ -z "${RUNNER_VERSION}" ]]; then
  RUNNER_VERSION="$(gh api repos/actions/runner/releases/latest --jq '.tag_name' | sed 's/^v//')"
fi
if [[ -z "${RUNNER_VERSION}" ]]; then
  echo "Unable to resolve runner version." >&2
  exit 1
fi

ARCH="$(uname -m)"
case "${ARCH}" in
  x86_64) RUNNER_ARCH="x64" ;;
  aarch64|arm64) RUNNER_ARCH="arm64" ;;
  *)
    echo "Unsupported architecture: ${ARCH}" >&2
    exit 1
    ;;
esac

mkdir -p "${RUNNER_DIR}"
cd "${RUNNER_DIR}"

PKG="actions-runner-linux-${RUNNER_ARCH}-${RUNNER_VERSION}.tar.gz"
URL="https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/${PKG}"

echo "Downloading runner ${RUNNER_VERSION} (${RUNNER_ARCH})..."
curl -fsSL -o "${PKG}" "${URL}"
tar xzf "${PKG}"
rm -f "${PKG}"

CONFIG_ARGS=(
  --url "https://github.com/${REPO}"
  --token "${REG_TOKEN}"
  --unattended
  --name "${RUNNER_NAME}"
  --labels "${LABELS}"
  --work "_work"
)
if [[ "${EPHEMERAL}" == "true" ]]; then
  CONFIG_ARGS+=(--ephemeral)
fi
if [[ "${REPLACE}" == "true" ]]; then
  CONFIG_ARGS+=(--replace)
fi

echo "Configuring runner in ${RUNNER_DIR}..."
./config.sh "${CONFIG_ARGS[@]}"

echo "Runner configured."
echo "Directory: ${RUNNER_DIR}"
echo "Name: ${RUNNER_NAME}"
echo "Labels: ${LABELS}"
echo "Repository: ${REPO}"

if [[ "${START}" == "true" ]]; then
  echo "Starting runner in foreground..."
  exec ./run.sh
fi

echo "Next: cd ${RUNNER_DIR} && ./run.sh"
