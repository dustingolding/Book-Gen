# Bookgen Bootstrap

Use these sample artifacts to bootstrap the new book-building flow:

- `docs/bookgen/prompt_pack_manifest.sample.json`
- `docs/bookgen/rubric.sample.json`
- `docs/bookgen/bookspec.sample.json`
- `docs/bookgen/closed_session.bookspec.generated.json`
- `docs/bookgen/constitution.sample.yaml`
- `docs/bookgen/installment_pack.sample.yaml`
- `schemas/bookgen/*.schema.yaml`

Upload targets in object storage:

- `prompt-packs/<genre>/<version>/manifest.json`
- `rubrics/<genre>/<version>/rubric.json`
- `inputs/<project_id>/bookspec.json`

Production default: run BookGen in-cluster as a Kubernetes Job (see below).
Local CLI execution is fallback-only for debugging.

Canonical markdown intake is also supported. If the file contains the `constitution_input` and `installment_input` YAML block, bookgen compiles it to a runnable `bookspec` automatically:

```bash
python3 -m app.cli run-book-flow \
  --project-id closed-session-series \
  --run-date $(date +%F) \
  --bookspec-path ../The\ Closed\ Session\ Input.md
```

The upgraded pipeline materializes and validates:

- `bookgen/<project_id>/constitution/constitution.yaml`
- `bookgen/<project_id>/installments/<installment_id>/installment_pack.yaml`
- `bookgen/<project_id>/installments/<installment_id>/outline.yaml`
- `bookgen/<project_id>/installments/<installment_id>/ledgers/*.yaml`
- `bookgen/<project_id>/installments/<installment_id>/chapters/ch-XX/{chapter_pack,draft,final,eval}.yaml|md`
- `exports/<project_id>/<installment_id>/manuscript.md`
- `exports/<project_id>/<installment_id>/manuscript.docx` when `output_formats` includes `docx`

Hard gates now enforce:

- constitution schema validity
- installment bounds presence
- outline escalation ceilings
- chapter hard-fail rubric categories
- all-pass assembly before export

The flow fails fast if prompt pack or rubric paths are missing.

## LakeFS (Recommended)

Enable stage-boundary commits by setting:

- `LAKEFS_ENABLED=true`
- `LAKEFS_ENDPOINT`
- `LAKEFS_REPO`
- `LAKEFS_ACCESS_KEY`
- `LAKEFS_SECRET_KEY`
- `LAKEFS_SOURCE_BRANCH` (optional, defaults to `main`)
- `LAKEFS_BOOKGEN_BRANCH_PREFIX` (optional, defaults to `bookgen`)

When enabled, `bookgen` commits each stage to branch
`<LAKEFS_BOOKGEN_BRANCH_PREFIX>/<project_id>`.

## Kubernetes Job Execution

To offload BookGen generation from your local machine, run a one-shot Kubernetes Job.

Fastest production path (single command):

```bash
./scripts/run_bookgen_release.sh \
  --project-id closed-session-series \
  --input-path ../The\ Closed\ Session\ Input.md \
  --resource-profile light \
  --bookgen-llm-chapter-limit 1
```

Add `--promote` to automatically merge the successful branch into `main`.
If local DNS cannot resolve `*.svc.cluster.local`, the script auto-falls back to `MINIO_LOCAL_ENDPOINT` and starts a temporary MinIO port-forward.
The release command runs `scripts/bookgen_preflight.sh` by default; use `--skip-preflight` only for debugging.
It also blocks duplicate active jobs per project id by default; use `--allow-concurrent` only when needed.
Add `--cleanup-old-jobs` to remove completed/failed jobs for the same project id before a new run.

1) Upload your local input file to object storage:

```bash
python3 scripts/upload_bookgen_input.py \
  --project-id closed-session-series \
  --input-path ../The\ Closed\ Session\ Input.md \
  --object-key inputs/closed-session-series/bookspec.json
```

For `.md`/`.yaml` canonical intake files, the uploader compiles them into a runnable `bookspec` JSON automatically.

2) Submit the in-cluster BookGen Job:

```bash
./scripts/run_bookgen_k8s_job.sh \
  --project-id closed-session-series \
  --bookspec-key inputs/closed-session-series/bookspec.json \
  --resource-profile standard \
  --bookgen-use-llm true \
  --bookgen-llm-chapter-limit 1 \
  --wait
```

The job spec excludes control-plane nodes by default via node affinity. If your cluster has only a control-plane node, the job will stay Pending until a worker node exists.

Resource control options:

- `--resource-profile light|standard|heavy` (default `standard`)
- Explicit overrides: `--cpu-request`, `--cpu-limit`, `--memory-request`, `--memory-limit`
- `--wait-timeout-seconds <int>` to cap how long `--wait` will block

Recommended starting point to reduce node pressure:

```bash
./scripts/run_bookgen_k8s_job.sh \
  --project-id closed-session-series \
  --bookspec-key inputs/closed-session-series/canonical_intake.md \
  --resource-profile light \
  --bookgen-use-llm true \
  --bookgen-llm-chapter-limit 1 \
  --wait
```

## Promote LakeFS Branches

After validating outputs on a BookGen branch, merge it into `main`:

```bash
python3 scripts/promote_bookgen_branch.py --project-id closed-session-k8s-003
python3 scripts/promote_bookgen_branch.py --project-id time-tinkers-k8s-001
```

Use `--dry-run` first to inspect the merge request without executing:

```bash
python3 scripts/promote_bookgen_branch.py --project-id closed-session-k8s-003 --dry-run
```

## Local Fallback (Debug Only)

Use local flow execution only for debugging or rapid iteration:

```bash
./scripts/run_flow_local.sh
```

Operational runbook: `docs/bookgen-production-runbook.md`
