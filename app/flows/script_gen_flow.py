from __future__ import annotations

import json

from prefect import flow, task

from app.clients.lakefs import LakeFSClient
from app.services.lakefs_paths import factpack_json, script_draft_json
from app.services.script_gen_v4 import build_script


@task(retries=2, retry_delay_seconds=20, log_prints=True)
def script_gen_task(run_date: str, lakefs_branch: str) -> dict:
    lakefs = LakeFSClient()
    if not lakefs.enabled:
        return {"run_date": run_date, "status": "skipped", "reason": "lakefs_disabled"}

    factpack = json.loads(lakefs.download_object(branch=lakefs_branch, path=factpack_json(run_date)).decode("utf-8"))
    script = build_script(run_date, factpack)
    key = script_draft_json(run_date)
    lakefs.upload_object(
        branch=lakefs_branch,
        path=key,
        content=json.dumps(script, ensure_ascii=True, separators=(",", ":")).encode("utf-8"),
        content_type="application/json",
    )
    return {"run_date": run_date, "status": "built", "script_key": key}


@flow(name="slw-dailycast-script-gen")
def script_gen_flow(run_date: str, lakefs_branch: str) -> dict:
    return script_gen_task(run_date, lakefs_branch)
