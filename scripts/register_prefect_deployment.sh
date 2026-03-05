#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${PREFECT_API_URL:-}" ]]; then
  echo "PREFECT_API_URL is required"
  exit 1
fi

DEPLOYMENT_NAME="${1:-dailycast-prod}"

prefect config set PREFECT_API_URL="${PREFECT_API_URL}"
prefect deploy -n "${DEPLOYMENT_NAME}" -p prefect/prefect.yaml
