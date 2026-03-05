from __future__ import annotations

import json

from prefect import flow, task

from app.clients.lakefs import LakeFSClient
from app.services.lakefs_paths import script_draft_json, script_realism_json
from app.services.realism_v4 import apply_realism


@task(retries=1, retry_delay_seconds=10, log_prints=True)
def realism_task_v2(run_date: str, lakefs_branch: str) -> dict:
    lakefs = LakeFSClient()
    if not lakefs.enabled:
        return {"run_date": run_date, "status": "skipped", "reason": "lakefs_disabled"}

    draft = json.loads(lakefs.download_object(branch=lakefs_branch, path=script_draft_json(run_date)).decode("utf-8"))
    rewritten = apply_realism(draft)
    key = script_realism_json(run_date)
    lakefs.upload_object(
        branch=lakefs_branch,
        path=key,
        content=json.dumps(rewritten, ensure_ascii=True, separators=(",", ":")).encode("utf-8"),
        content_type="application/json",
    )
    return {"run_date": run_date, "status": "completed", "script_key": key}


@flow(name="slw-dailycast-realism-v2")
def realism_flow_v2(run_date: str, lakefs_branch: str) -> dict:
    return realism_task_v2(run_date, lakefs_branch)
