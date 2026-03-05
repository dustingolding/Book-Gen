# DailyCast lakeFS Data Model

## Repository

- Repository: `slw-dailycast` (configurable via `LAKEFS_REPO`)
- Source branch: `main` (configurable via `LAKEFS_SOURCE_BRANCH`)

## Run branch strategy

One branch per run date + unique run id:

- Pattern: `<prefix>-<YYYY-MM-DD>-<runid>`
- Default prefix: `run` (`LAKEFS_DAILYCAST_BRANCH_PREFIX`)

Example:

- `run-2026-02-27-a1b2c3d4e5`

## Stage commit cadence

After each stage in `dailycast_parent_flow_v4`:

- `factpack`
- `rag_index`
- `script_draft`
- `realism`
- `verified`
- `audio` (if enabled)
- `published`

## Artifact paths

- `factpacks/<date>/factpack.json`
- `notebooklm/<date>/*.md`
- `rag/<date>/chunks.jsonl`
- `scripts/<date>/script.draft.json`
- `scripts/<date>/script.realism.json`
- `scripts/<date>/script.final.json`
- `quality/<date>/qa_report.json`
- `audio/<date>/episode.ssml`
- `audio/<date>/episode.mp3`
- `publish/<date>/manifest.v4.json`
