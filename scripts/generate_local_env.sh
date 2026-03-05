#!/usr/bin/env bash
set -euo pipefail

OUT_FILE="${1:-.env.local.generated}"

rand_alnum() {
  local len="$1"
  openssl rand -base64 96 | tr -dc 'A-Za-z0-9' | cut -c1-"$len"
}

MINIO_ACCESS_KEY="${MINIO_LOCAL_ACCESS_KEY:-slwminioadmin}"
MINIO_SECRET_KEY="${MINIO_LOCAL_SECRET_KEY:-slw-minio-secret-change-me}"
PG_PASSWORD="$(rand_alnum 40)"

cat > "${OUT_FILE}" <<EOF
MINIO_ENDPOINT=http://127.0.0.1:19000
MINIO_LOCAL_ENDPOINT=http://127.0.0.1:19000
MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY}
MINIO_SECRET_KEY=${MINIO_SECRET_KEY}
MINIO_SECURE=false
PG_HOST=postgres.sideline-wire-dailycast.svc.cluster.local
PG_PORT=5432
PG_USER=slw
PG_PASSWORD=${PG_PASSWORD}
PG_DB=slw_dailycast
MLFLOW_TRACKING_URI=http://mlflow.log-anomaly.svc.cluster.local:5000
PREFECT_API_URL=http://prefect-server.sideline-wire-dailycast.svc.cluster.local:4200/api
LAKEFS_ENABLED=true
LAKEFS_ENDPOINT=http://127.0.0.1:18000
LAKEFS_LOCAL_ENDPOINT=http://127.0.0.1:18000
LAKEFS_REPO=slw-dailycast
LAKEFS_ACCESS_KEY=
LAKEFS_SECRET_KEY=
LAKEFS_SOURCE_BRANCH=main
LAKEFS_BOOKGEN_BRANCH_PREFIX=bookgen
LAKEFS_DAILYCAST_BRANCH_PREFIX=run
LLM_ENDPOINT=
LLM_API_KEY=
LLM_MODEL=
SPORTS_API_URL=
SPORTS_API_KEY=
NEWS_API_URL=
NEWS_API_KEY=
SPORTSDB_API_URL=https://www.thesportsdb.com
SPORTSDB_API_KEY=
ESPN_SITE_API_URL=https://site.api.espn.com
ESPN_CORE_API_URL=https://sports.core.api.espn.com
ESPN_SPORTS=football/nfl,basketball/nba,baseball/mlb,hockey/nhl
NEWSAPI_URL=https://newsapi.org/v2
NEWSAPI_API_KEY=
GNEWS_URL=https://gnews.io/api/v4
GNEWS_API_KEY=
S3_BUCKET=slw-dailycast
LOG_LEVEL=INFO
EOF

echo "Wrote ${OUT_FILE}"
echo "Next steps:"
echo "  1) cp ${OUT_FILE} .env"
echo "  2) Port-forward required services for local runs:"
echo "     kubectl -n sideline-wire-dailycast port-forward svc/minio 19000:9000"
echo "     kubectl -n sideline-wire-dailycast port-forward svc/lakefs 18000:80"
echo "  3) Run scripts/run_flow_local.sh"
echo "  4) Set provider keys: SPORTSDB_API_KEY, NEWSAPI_API_KEY, GNEWS_API_KEY"
echo "  5) If you use an external LLM, set LLM_ENDPOINT/LLM_API_KEY/LLM_MODEL"
echo "     Example (Groq):"
echo "       LLM_ENDPOINT=https://api.groq.com/openai/v1/chat/completions"
echo "       LLM_MODEL=llama-3.1-8b-instant"
