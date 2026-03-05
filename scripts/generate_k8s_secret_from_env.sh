#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-.env}"
NAMESPACE="${2:-sideline-wire-dailycast}"
SECRET_NAME="${3:-slw-dailycast-secrets}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "env file not found: ${ENV_FILE}"
  exit 1
fi

set -a
source "${ENV_FILE}"
set +a

kubectl -n "${NAMESPACE}" create secret generic "${SECRET_NAME}" \
  --from-literal=MINIO_ENDPOINT="${MINIO_ENDPOINT}" \
  --from-literal=MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY}" \
  --from-literal=MINIO_SECRET_KEY="${MINIO_SECRET_KEY}" \
  --from-literal=MINIO_SECURE="${MINIO_SECURE}" \
  --from-literal=PG_HOST="${PG_HOST}" \
  --from-literal=PG_PORT="${PG_PORT}" \
  --from-literal=PG_USER="${PG_USER}" \
  --from-literal=PG_PASSWORD="${PG_PASSWORD}" \
  --from-literal=PG_DB="${PG_DB}" \
  --from-literal=MLFLOW_TRACKING_URI="${MLFLOW_TRACKING_URI}" \
  --from-literal=PREFECT_API_URL="${PREFECT_API_URL}" \
  --from-literal=LLM_ENDPOINT="${LLM_ENDPOINT:-}" \
  --from-literal=LLM_API_KEY="${LLM_API_KEY:-}" \
  --from-literal=LLM_MODEL="${LLM_MODEL:-}" \
  --from-literal=FACTPACK_USE_LLM="${FACTPACK_USE_LLM:-false}" \
  --from-literal=SPORTS_API_URL="${SPORTS_API_URL:-}" \
  --from-literal=SPORTS_API_KEY="${SPORTS_API_KEY:-}" \
  --from-literal=NEWS_API_URL="${NEWS_API_URL:-}" \
  --from-literal=NEWS_API_KEY="${NEWS_API_KEY:-}" \
  --from-literal=SPORTSDB_API_URL="${SPORTSDB_API_URL:-}" \
  --from-literal=SPORTSDB_API_KEY="${SPORTSDB_API_KEY:-}" \
  --from-literal=ESPN_SITE_API_URL="${ESPN_SITE_API_URL:-}" \
  --from-literal=ESPN_CORE_API_URL="${ESPN_CORE_API_URL:-}" \
  --from-literal=ESPN_SPORTS="${ESPN_SPORTS:-}" \
  --from-literal=NEWSAPI_URL="${NEWSAPI_URL:-}" \
  --from-literal=NEWSAPI_API_KEY="${NEWSAPI_API_KEY:-}" \
  --from-literal=GNEWS_URL="${GNEWS_URL:-}" \
  --from-literal=GNEWS_API_KEY="${GNEWS_API_KEY:-}" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "Applied secret ${SECRET_NAME} in namespace ${NAMESPACE}"
