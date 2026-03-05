#!/usr/bin/env bash
set -euo pipefail

RELEASE_NAME="${1:-slw-dailycast}"
NAMESPACE="${2:-sideline-wire-dailycast}"
IMAGE_REPO="${3:?image repository required}"
IMAGE_TAG="${4:?image tag required}"
SECRET_NAME="${5:-slw-dailycast-secrets}"

helm upgrade --install "${RELEASE_NAME}" ./helm/slw-dailycast \
  --namespace "${NAMESPACE}" \
  --create-namespace \
  --set image.repository="${IMAGE_REPO}" \
  --set image.tag="${IMAGE_TAG}" \
  --set secret.name="${SECRET_NAME}" \
  --wait --timeout 10m
