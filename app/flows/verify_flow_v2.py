from __future__ import annotations

import json

from prefect import flow, task

from app.clients.lakefs import LakeFSClient
from app.services.lakefs_paths import factpack_json, qa_report_json, script_final_json, script_realism_json
from app.services.verify_v4 import verify_script


@task(retries=1, retry_delay_seconds=10, log_prints=True)
def verify_task_v2(run_date: str, lakefs_branch: str) -> dict:
    lakefs = LakeFSClient()
    if not lakefs.enabled:
        return {"run_date": run_date, "status": "skipped", "reason": "lakefs_disabled"}

    factpack = json.loads(lakefs.download_object(branch=lakefs_branch, path=factpack_json(run_date)).decode("utf-8"))
    script = json.loads(lakefs.download_object(branch=lakefs_branch, path=script_realism_json(run_date)).decode("utf-8"))

    report = verify_script(run_date, script, factpack)
    report_key = qa_report_json(run_date)
    final_key = script_final_json(run_date)

    lakefs.upload_object(
        branch=lakefs_branch,
        path=report_key,
        content=json.dumps(report, ensure_ascii=True, separators=(",", ":")).encode("utf-8"),
        content_type="application/json",
    )
    lakefs.upload_object(
        branch=lakefs_branch,
        path=final_key,
        content=json.dumps(script, ensure_ascii=True, separators=(",", ":")).encode("utf-8"),
        content_type="application/json",
    )

    if report.get("status") != "pass":
        issues = [i.get("code", "unknown") for i in report.get("issues", []) if isinstance(i, dict)]
        raise RuntimeError(f"V4 quality gate failed (issues={issues})")

    return {"run_date": run_date, "status": "pass", "qa_key": report_key, "script_key": final_key}


@flow(name="slw-dailycast-verify-v2")
def verify_flow_v2(run_date: str, lakefs_branch: str) -> dict:
    return verify_task_v2(run_date, lakefs_branch)
