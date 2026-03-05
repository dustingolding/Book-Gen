from __future__ import annotations

import json

from prefect import flow, task

from app.clients.lakefs import LakeFSClient
from app.services.lakefs_paths import publish_manifest
from app.services.publish_v4 import build_manifest


@task(retries=0, log_prints=True)
def publish_task_v2(run_date: str, lakefs_branch: str) -> dict:
    lakefs = LakeFSClient()
    manifest = build_manifest(run_date=run_date, branch=lakefs_branch)
    if lakefs.enabled:
        key = publish_manifest(run_date)
        lakefs.upload_object(
            branch=lakefs_branch,
            path=key,
            content=json.dumps(manifest, ensure_ascii=True, separators=(",", ":")).encode("utf-8"),
            content_type="application/json",
        )
        manifest["manifest_key"] = key
    return manifest


@flow(name="slw-dailycast-publish-v2")
def publish_flow_v2(run_date: str, lakefs_branch: str) -> dict:
    return publish_task_v2(run_date, lakefs_branch)
