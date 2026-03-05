# SIDE LINE WIRE DAILYCAST

Production-grade daily sports pipeline orchestrated by Prefect on Kubernetes.

## Architecture

```text
                +--------------------------+
                |      Prefect Server      |
                +-----------+--------------+
                            |
                            v
+-------------------+   +---+----------------------+   +------------------+
| Sports Ingest     +---> Normalize + Rank (MLflow)+---> Briefpack Build  |
+-------------------+   +--------------------------+   +---------+--------+
                                                                  |
                                                                  v
                                                      +-----------+-----------+
                                                      | DailyCast v4          |
                                                      | FactPack -> RAG ->    |
                                                      | Script -> Realism ->  |
                                                      | Verify -> Publish     |
                                                      +-----------+-----------+
                                                                  |
                                                                  v
+-------------------+      +-------------------+       +---------+----------+
| MinIO Artifact    |<-----+ Publisher         +------>+ NotebookLM Input    |
| raw/normalized/...|      +-------------------+       | markdown transcript |
+-------------------+                                  +---------------------+
         ^
         |
+--------+---------+
| PostgreSQL       |
| normalized data  |
+------------------+
```

## Components

- `app/services/ingest_sports.py`: previous-day scores ingest.
- `app/services/ingest_news.py`: sports headlines ingest.
- `app/services/normalize.py`: canonical event normalization.
- `app/services/rank.py`: ranking scores with MLflow tracking.
- `app/services/briefpack.py`: briefpack assembly.
- `app/services/factpack.py`: FactPack builder + NotebookLM feed artifacts.
- `app/services/rag/*`: chunking, embedding, indexing, retrieval.
- `app/services/script_gen_v4.py`: script generation from FactPack + RAG context.
- `app/services/realism_v4.py`: realism naturalization pass.
- `app/services/verify_v4.py`: deterministic QA report + quality gate.
- `app/services/publish_v4.py`: publish manifest for v4 artifacts.
- `app/services/agent_transcript.py`: legacy transcript orchestrator.
- `app/services/agents/*.py`: planner/writer/verifier/transcript builder modules.
- `app/services/verify_transcript.py`: deterministic verification report artifact.
- `app/services/quality_gate.py`: strict fail-closed gates.
- `app/services/render_markdown.py`: broadcast-ready markdown.
- `app/services/publish.py`: final publish package.
- `app/flows/dailycast_flow_v4.py`: parent Prefect flow (default path).
- `app/flows/dailycast_flow.py`: legacy parent Prefect flow.
- `app/flows/*_flow.py`: stage subflows (ingest, normalize, rank, briefpack, agent, verify, render, publish).

## Prerequisites

- Python 3.11+
- Docker buildx
- Access to Kubernetes cluster (k3s)
- Existing services:
  - MLflow Tracking Server
  - Prefect Server + workers
  - Optional external MinIO/PostgreSQL (in-cluster manifests are provided in `k8s/minio.yaml` and `k8s/postgres.yaml`)

## Required Environment Variables

```bash
export MINIO_ENDPOINT=http://minio.sideline-wire-dailycast.svc.cluster.local:9000
export MINIO_ACCESS_KEY=...
export MINIO_SECRET_KEY=...
export PG_HOST=postgres.sideline-wire-dailycast.svc.cluster.local
export PG_USER=slw
export PG_PASSWORD=...
export PG_DB=slw_dailycast
export MLFLOW_TRACKING_URI=http://mlflow.log-anomaly.svc.cluster.local:5000
export PREFECT_API_URL=http://prefect-server.sideline-wire-dailycast.svc.cluster.local:4200/api
# Optional
export LLM_ENDPOINT=https://llm-gateway.internal/v1/chat/completions
export LLM_API_KEY=...
export LLM_MODEL=gpt-4o-mini
export TRANSCRIPT_REQUIRE_LLM=true
export SPORTSDB_API_URL=https://www.thesportsdb.com
export SPORTSDB_API_KEY=...
export ESPN_SITE_API_URL=https://site.api.espn.com
export ESPN_CORE_API_URL=https://sports.core.api.espn.com
export ESPN_SPORTS=football/nfl,basketball/nba,baseball/mlb,hockey/nhl
export NEWSAPI_URL=https://newsapi.org/v2
export NEWSAPI_API_KEY=...
export GNEWS_URL=https://gnews.io/api/v4
export GNEWS_API_KEY=...
```

Groq example:

```bash
export LLM_ENDPOINT=https://api.groq.com/openai/v1/chat/completions
export LLM_API_KEY=...
export LLM_MODEL=llama-3.3-70b-versatile
```

If `TRANSCRIPT_REQUIRE_LLM=true`, the run will fail instead of publishing degraded fallback transcripts when LLM calls fail.

## Setup

0. Generate local secrets and env file:

```bash
./scripts/generate_local_env.sh .env
```

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Initialize database schema:

```bash
python -m app.cli init-db
```

3. Run one local end-to-end flow (v4 default):

```bash
python -m app.cli run-flow --run-date $(date -u +%F)
```

Legacy flow (if needed):

```bash
python -m app.cli run-flow-legacy --run-date $(date -u +%F)
```

Recommended local runner (auto-port-forward + env overrides):

```bash
./scripts/run_flow_local.sh $(date -u +%F)
```

4. Apply env values into Kubernetes secret:

```bash
./scripts/generate_k8s_secret_from_env.sh .env sideline-wire-dailycast slw-dailycast-secrets
```

## Book Building Pipeline (New)

Separate Prefect flow stack for book generation (does not modify dailycast flow):

- Parent flow: `bookgen-parent`
- Stage flows:
  - `bookgen-intake`
  - `bookgen-planning`
  - `bookgen-chapters`
  - `bookgen-assembly`

Run locally:

```bash
python -m app.cli run-book-flow \
  --project-id demo-thriller-001 \
  --run-date $(date +%F) \
  --bookspec-path docs/bookgen/bookspec.sample.json
```

Deploy as a separate workload/name (recommended):

```bash
./scripts/helm_deploy_bookgen.sh \
  slw-bookgen \
  sideline-wire-dailycast \
  ghcr.io/dustingolding/slw-dailycast-base \
  <tag> \
  slw-dailycast-secrets \
  demo-thriller-001
```

This creates a distinct CronJob name (for example `slw-bookgen-bookgen`) and
uses `run-book-flow` instead of `run-flow`.

Required object keys before running (pinned versions, fail-fast if missing):

- `prompt-packs/<genre>/<version>/manifest.json`
- `rubrics/<genre>/<version>/rubric.json`
- `inputs/<project_id>/bookspec.json` (auto-uploaded when using `--bookspec-path`)

Optional but recommended for production lineage: LakeFS stage commits.
When `LAKEFS_ENABLED=true`, each bookgen stage creates or reuses branch
`<LAKEFS_BOOKGEN_BRANCH_PREFIX>/<project_id>` and commits stage artifacts:

- `LAKEFS_ENDPOINT` (e.g. `http://lakefs.<ns>.svc.cluster.local:8000`)
- `LAKEFS_REPO`
- `LAKEFS_ACCESS_KEY`
- `LAKEFS_SECRET_KEY`
- `LAKEFS_SOURCE_BRANCH` (default `main`)
- `LAKEFS_BOOKGEN_BRANCH_PREFIX` (default `bookgen`)

Bootstrap samples are in:

- `docs/bookgen/prompt_pack_manifest.sample.json`
- `docs/bookgen/rubric.sample.json`
- `docs/bookgen/bookspec.sample.json`

## Prefect Deployment

Deploy flow with schedule:

```bash
prefect deploy -n dailycast-prod -p prefect/prefect.yaml
```

Schedule is configured for daily UTC run.

## Build and Push Images

```bash
./scripts/build_images.sh ghcr.io/dustingolding latest
```

## CI/CD

GitHub Actions workflows:

- `.github/workflows/ci.yml`
  - Lint (`ruff`, `helm lint`)
  - Unit tests
  - Integration tests with Postgres + MinIO service containers
- `.github/workflows/cd.yml`
  - Build and push images to GHCR
  - Helm deploy to Kubernetes
  - Prefect deployment registration

Required GitHub repository secrets for CD:

- `KUBE_CONFIG_B64`: base64-encoded kubeconfig for target cluster.
- `PREFECT_API_URL`: Prefect API URL for deployment registration.

Helm deploy helper:

```bash
./scripts/helm_deploy.sh slw-dailycast sideline-wire-dailycast ghcr.io/dustingolding/slw-dailycast-base latest
```

Prefect deployment registration helper:

```bash
PREFECT_API_URL=http://prefect-server.sideline-wire-dailycast.svc.cluster.local:4200/api \
./scripts/register_prefect_deployment.sh dailycast-prod
```

## Kubernetes Deploy

Recommended (production):

```bash
./scripts/helm_deploy.sh slw-dailycast sideline-wire-dailycast ghcr.io/dustingolding/slw-dailycast-base latest
```

Helm path is the source of truth for:
- node scheduling (`nodeSelector`, `affinity`, `tolerations`)
- resource requests/limits
- image tag pinning and rollout behavior

Direct raw manifests (testing/bootstrap only):

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/secrets.template.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/postgres.yaml
kubectl apply -f k8s/minio.yaml
kubectl apply -f k8s/prefect.yaml
kubectl apply -f k8s/networkpolicy.yaml
kubectl apply -f k8s/networkpolicy-prefect-ingress.yaml
kubectl apply -f k8s/jobs/
```

Prefect server health check (in-cluster):

```bash
kubectl -n sideline-wire-dailycast run nettest --restart=Never --image=curlimages/curl:8.10.1 --labels app.kubernetes.io/part-of=slw-dailycast -- sleep 60
kubectl -n sideline-wire-dailycast wait --for=condition=Ready pod/nettest --timeout=120s
kubectl -n sideline-wire-dailycast exec nettest -- curl -sS http://prefect-server:4200/api/health
kubectl -n sideline-wire-dailycast delete pod nettest
```

For local CLI runs from your VM shell, cluster DNS names are not resolvable. Use:

```bash
kubectl -n sideline-wire-dailycast port-forward svc/prefect-server 14200:4200
```

Then set:

```bash
PREFECT_API_URL=http://127.0.0.1:14200/api
```

Infra-only helper:

```bash
./scripts/deploy_infra.sh sideline-wire-dailycast
```

Current default worker placement policy (both Helm and raw manifests):
- `kubernetes.io/arch=amd64`
- `slw/workload=ops`

Free API provider options are documented in:
- `docs/free-api-options.md`

## Troubleshooting

- Check Prefect run logs:

```bash
prefect flow-run ls --limit 10
prefect flow-run logs <FLOW_RUN_ID>
```

- End-of-run summary:

Look for `dailycast_run_summary` in flow logs. It includes item counts, quality metrics, and whether LLM degraded mode was used (`llm_degraded` and `llm_fallback_count`).

- Check Kubernetes jobs:

```bash
kubectl -n sideline-wire-dailycast get jobs,pods
kubectl -n sideline-wire-dailycast logs job/slw-ingest-sports
```

- Verify MinIO artifacts:

```bash
mc ls minio/slw-dailycast/transcripts/
```

- Validate quality gate failures:

```bash
python -m app.cli quality-gate --run-date 2026-01-01
```

- Generate verification artifact manually:

```bash
python -m app.cli verify-transcript --run-date 2026-01-01
```
