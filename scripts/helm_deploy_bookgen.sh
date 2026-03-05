#!/usr/bin/env bash
set -euo pipefail

RELEASE_NAME="${1:-slw-bookgen}"
NAMESPACE="${2:-sideline-wire-dailycast}"
IMAGE_REPO="${3:?image repository required}"
IMAGE_TAG="${4:?image tag required}"
SECRET_NAME="${5:-slw-dailycast-secrets}"
PROJECT_ID="${6:-demo-thriller-001}"

helm upgrade --install "${RELEASE_NAME}" ./helm/slw-dailycast \
  --namespace "${NAMESPACE}" \
  --create-namespace \
  -f ./helm/slw-dailycast/values.bookgen.yaml \
  --set image.repository="${IMAGE_REPO}" \
  --set image.tag="${IMAGE_TAG}" \
  --set secret.name="${SECRET_NAME}" \
  --set cronjob.args[2]="${PROJECT_ID}" \
  --wait --timeout 10m
