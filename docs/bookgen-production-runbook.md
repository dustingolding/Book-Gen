# BookGen Production Runbook

## Standard Release

Run a production release with preflight, Kubernetes execution, and promotion:

```bash
./scripts/run_bookgen_release.sh \
  --project-id <project-id> \
  --input-path "<input-file>" \
  --resource-profile light \
  --bookgen-llm-chapter-limit 1 \
  --wait-timeout-seconds 7200 \
  --promote
```

The release runner blocks duplicate in-flight jobs for the same `project-id` by default.
Use `--allow-concurrent` only when overlapping runs are intentional.
Use `--cleanup-old-jobs` to remove completed/failed jobs for the same `project-id` before starting.
Promotion is gated by required stage artifacts (`assembly-export.json` and `.artifacts.json`) existing on the source branch.

Each successful release writes audit metadata to:

- `runs/<project_id>/meta/release_audit.json`

## Manual Promotion

Promote only after manual artifact review:

```bash
PYTHONPATH=. .venv/bin/python scripts/promote_bookgen_branch.py --project-id <project-id>
```

## Preflight Only

Check cluster/service readiness without running generation:

```bash
./scripts/bookgen_preflight.sh --namespace sideline-wire-dailycast --require-ops-worker
```

## Failure Triage

Get job status and node:

```bash
kubectl -n sideline-wire-dailycast get pod -l job-name=<job-name> -o wide
```

Get recent logs:

```bash
kubectl -n sideline-wire-dailycast logs job/<job-name> --tail=200
```

Common causes:

- `JSONDecodeError` at intake means a non-BookSpec payload was uploaded.
- `400 Bad Request` on LakeFS branch create usually indicates an old image still using slash branch naming.
- `Temporary failure in name resolution` on local promotion/upload means `.svc.cluster.local` is not resolvable locally and port-forward fallback should be used.

## Alert Routing Smoke Test

Run a controlled BookGen failure and verify `BookGenJobFailedRecent` appears:

```bash
./scripts/bookgen_alert_smoke_test.sh
```

Options:

- `--alert-timeout-seconds <int>` (default `600`)
- `--no-alertmanager-check`
- `--keep-job`

## Rollback Pattern

Keep prior project IDs immutable. Roll back by using the previous promoted LakeFS reference and re-promoting that branch/state if needed.

## Nightly Job Cleanup

Apply the cleanup CronJob:

```bash
kubectl apply -f k8s/jobs/bookgen-job-cleanup-cronjob.yaml
```

Defaults:

- schedule: daily at `03:17 UTC`
- retention: `72` hours (`RETENTION_HOURS` env in CronJob)
- scope: `bookgen-*` jobs in `sideline-wire-dailycast`

## Publish Readiness Gate

Run automated publish-readiness checks for a project:

```bash
PYTHONPATH=. .venv/bin/python scripts/bookgen_publish_readiness.py \
  --project-id <project-id> \
  --require-promotion
```

Output report key:

- `runs/<project_id>/meta/publish_readiness.json`

Manual checklist template:

- `docs/bookgen/publish_checklist.template.md`

## Publish Candidate Dossier

Generate a publish-candidate dossier and project-specific checklist after readiness is `pass`:

```bash
PYTHONPATH=. .venv/bin/python scripts/bookgen_publish_candidate.py \
  --project-id <project-id>
```

Outputs:

- object: `runs/<project_id>/meta/publish_candidate.json`
- local checklist: `docs/bookgen/checklists/<project_id>.md`

## One-Command Publish Prep

Run readiness + candidate generation in one command:

```bash
./scripts/bookgen_prepare_publish.sh \
  --project-id <project-id> \
  --require-promotion
```

## Export Local Publish Bundle

Export manuscript + metadata artifacts into one local bundle directory:

```bash
PYTHONPATH=. .venv/bin/python scripts/bookgen_export_publish_bundle.py \
  --project-id <project-id>
```

Default output:

- `exports/publish-bundles/<project-id>/`

## Package Publish Bundle

Create a delivery zip and integrity metadata:

```bash
PYTHONPATH=. .venv/bin/python scripts/bookgen_package_publish_bundle.py \
  --project-id <project-id>
```

Default output:

- `exports/publish-packages/<project-id>/<project-id>-publish-bundle.zip`
- `exports/publish-packages/<project-id>/SHA256SUMS.txt`
- `exports/publish-packages/<project-id>/BUNDLE_SHA256SUMS.txt`
- `exports/publish-packages/<project-id>/bundle_manifest.json`

Notes:

- `SHA256SUMS.txt` validates the zip package in-place.
- `BUNDLE_SHA256SUMS.txt` validates unpacked bundle files after extraction.

## CI Publish Gate (Manual)

Use GitHub Actions workflow `bookgen-publish-gate` for an isolated publish-prep check that does not change regular CI/CD.

Workflows:

- `bookgen-publish-gate-selfhosted.yml` (default/recommended)
- `bookgen-publish-gate.yml` (GitHub-hosted, explicit opt-in)

Required workflow input:

- `project_id`

Optional workflow input:

- `require_promotion` (default `true`)

Required GitHub repository secrets:

- `MINIO_ENDPOINT`
- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`
- `S3_BUCKET`
- `LAKEFS_ENDPOINT`
- `LAKEFS_ACCESS_KEY`
- `LAKEFS_SECRET_KEY`
- `LAKEFS_REPOSITORY`
- `LAKEFS_BRANCH`

Optional GitHub repository secret:

- `MINIO_SECURE`

Local helper commands:

```bash
# 1) Validate required secret values from .env (dry-run)
./scripts/bookgen_configure_publish_gate_secrets.sh

# 2) Write secrets to GitHub (explicit apply)
./scripts/bookgen_configure_publish_gate_secrets.sh --apply

# 3) Trigger the publish gate workflow for a project and wait for result
./scripts/bookgen_trigger_publish_gate.sh --project-id <project-id> --wait

# 4) Trigger GitHub-hosted variant (explicit opt-in)
./scripts/bookgen_trigger_publish_gate.sh \
  --github-hosted \
  --project-id <project-id> \
  --wait

# 5) Check self-hosted runner capacity and recent run states
./scripts/bookgen_selfhosted_runner_status.sh --repo <owner>/<repo>

# 6) Bootstrap a self-hosted runner on a Linux host (foreground start)
./scripts/bookgen_selfhosted_runner_quickstart.sh \
  --repo <owner>/<repo> \
  --start

# 7) Download and verify publish artifact from latest successful self-hosted run
./scripts/bookgen_collect_publish_artifact.sh \
  --repo <owner>/<repo>

# 8) Download and verify artifact for a specific project/run
./scripts/bookgen_collect_publish_artifact.sh \
  --repo <owner>/<repo> \
  --project-id <project-id> \
  --run-id <run-id>

# 9) One-command handoff (trigger gate + collect artifact + write summary)
./scripts/bookgen_release_handoff.sh \
  --repo <owner>/<repo> \
  --project-id <project-id>

# 10) Handoff from an already-successful run (no retrigger)
./scripts/bookgen_release_handoff.sh \
  --repo <owner>/<repo> \
  --project-id <project-id> \
  --collect-only \
  --run-id <run-id>
```
