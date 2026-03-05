from __future__ import annotations

import json

from prefect import flow, task

from app.clients.lakefs import LakeFSClient
from app.services import factpack, verify_factpack
from app.services.lakefs_paths import factpack_json, notebooklm_dir
from app.storage import ObjectStore


@task(retries=2, retry_delay_seconds=20, log_prints=True)
def build_factpack_v2_task(run_date: str, lakefs_branch: str) -> dict:
    result = factpack.run(run_date)
    verification = verify_factpack.run(run_date)
    if not bool(verification.get("approved", False)):
        issues = verification.get("issues") or []
        raise RuntimeError(f"Factpack quality gate failed (issues={issues})")

    lakefs = LakeFSClient()
    if lakefs.enabled:
        store = ObjectStore()
        fp_key = f"factpacks/{run_date}/factpack.json"
        lakefs.upload_object(
            branch=lakefs_branch,
            path=factpack_json(run_date),
            content=json.dumps(store.get_json(fp_key), ensure_ascii=True, separators=(",", ":")).encode("utf-8"),
            content_type="application/json",
        )

        prefix = notebooklm_dir(run_date)
        for obj in store.client.list_objects(store.bucket, prefix=prefix, recursive=True):
            if not obj.object_name.endswith(".md"):
                continue
            content = store.get_text(obj.object_name)
            lakefs.upload_object(
                branch=lakefs_branch,
                path=obj.object_name,
                content=content.encode("utf-8"),
                content_type="text/markdown",
            )
        lakefs.upload_object(
            branch=lakefs_branch,
            path=f"factpacks/{run_date}/verification.json",
            content=json.dumps(verification, ensure_ascii=True, separators=(",", ":")).encode("utf-8"),
            content_type="application/json",
        )
    return result


@flow(name="slw-dailycast-factpack-v2")
def factpack_flow_v2(run_date: str, lakefs_branch: str) -> dict:
    return build_factpack_v2_task(run_date, lakefs_branch)
