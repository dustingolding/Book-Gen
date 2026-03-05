# DailyCast v4 Runbook

## 1) Pre-flight

- Confirm secrets/config are current in namespace `sideline-wire-dailycast`.
- Confirm lakeFS endpoint/auth if `LAKEFS_ENABLED=true`.
- Confirm Postgres connectivity for rank + RAG index.

## 2) Trigger a run

```bash
python -m app.cli run-flow --run-date $(date +%F)
```

In-cluster manual trigger (CronJob-derived):

```bash
ts=$(date +%s)
kubectl -n sideline-wire-dailycast create job --from=cronjob/slw-dailycast-parent manual-$ts
kubectl -n sideline-wire-dailycast logs -f job/manual-$ts
```

## 3) Verify outputs

- `factpacks/<run_date>/factpack.json`
- `notebooklm/<run_date>/*.md`
- `quality/<run_date>/qa_report.json`
- `publish/<run_date>/manifest.v4.json`

If lakeFS is enabled, verify artifacts on the run branch and stage commits.

## 4) Common failure triage

- `404/400` on lakeFS object upload:
  - verify repo exists
  - verify source branch exists
  - verify access key/secret key
  - verify branch naming + endpoint URL
- FactPack quality gate failure:
  - inspect `verification.json` and `qa_report.json`
  - inspect `notebooklm/<run_date>/` docs for boilerplate flags
- RAG index failure:
  - check `PGVECTOR_CONNINFO`, extension availability, and embedding provider settings.

## 5) Optional audio stage

Audio is skipped unless:

- `ELEVENLABS_ENABLED=true`
- voice IDs and API key are present

This keeps transcript/factpack quality validation independent of TTS cost.
