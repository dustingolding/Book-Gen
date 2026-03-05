#!/usr/bin/env bash
set -euo pipefail

REGISTRY="${1:?registry required}"
TAG="${2:-latest}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$ROOT_DIR"

BASE_IMAGE="${REGISTRY}/slw-dailycast-base:${TAG}"
INGEST_IMAGE="${REGISTRY}/slw-dailycast-ingest:${TAG}"
AGENT_IMAGE="${REGISTRY}/slw-dailycast-agent:${TAG}"
RENDERER_IMAGE="${REGISTRY}/slw-dailycast-renderer:${TAG}"

docker build -f docker/Dockerfile.base -t "$BASE_IMAGE" .
docker push "$BASE_IMAGE"

docker build -f docker/Dockerfile.ingest --build-arg BASE_IMAGE="$BASE_IMAGE" -t "$INGEST_IMAGE" .
docker push "$INGEST_IMAGE"

docker build -f docker/Dockerfile.agent --build-arg BASE_IMAGE="$BASE_IMAGE" -t "$AGENT_IMAGE" .
docker push "$AGENT_IMAGE"

docker build -f docker/Dockerfile.renderer --build-arg BASE_IMAGE="$BASE_IMAGE" -t "$RENDERER_IMAGE" .
docker push "$RENDERER_IMAGE"

echo "Published images:"
echo "  $BASE_IMAGE"
echo "  $INGEST_IMAGE"
echo "  $AGENT_IMAGE"
echo "  $RENDERER_IMAGE"
