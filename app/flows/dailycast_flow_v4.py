from __future__ import annotations

from datetime import date

from prefect import flow, get_run_logger, task

from app.clients.lakefs import LakeFSClient
from app.config import get_settings
from app.db import set_pipeline_status
from app.flows.elevenlabs_flow import elevenlabs_flow
from app.flows.factpack_flow_v2 import factpack_flow_v2
from app.flows.publish_flow_v2 import publish_flow_v2
from app.flows.rag_index_flow import rag_index_flow
from app.flows.realism_flow_v2 import realism_flow_v2
from app.flows.script_gen_flow import script_gen_flow
from app.flows.verify_flow_v2 import verify_flow_v2
from app.services.lakefs_refs import dailycast_run_branch, stage_commit_message


@task
def init_run_branch_task(run_date: str) -> str:
    client = LakeFSClient()
    if not client.enabled:
        return ""
    branch = dailycast_run_branch(run_date)
    client.ensure_branch(branch=branch, source=client.source_branch)
    return branch


@task
def commit_stage_task(run_date: str, branch: str, stage: str) -> str:
    client = LakeFSClient()
    if not client.enabled:
        return ""
    return client.commit(
        branch=branch,
        message=stage_commit_message(run_date, stage),
        metadata={"stage": stage, "run_date": run_date},
    )


@flow(name="slw-dailycast-parent-v4")
def dailycast_parent_flow_v4(run_date: str | None = None) -> dict:
    run_date = run_date or date.today().isoformat()
    logger = get_run_logger()
    settings = get_settings()

    set_pipeline_status(run_date, "running")
    branch = ""
    try:
        branch = init_run_branch_task(run_date)
        logger.info("v4_run_context run_date=%s lakefs_branch=%s", run_date, branch or "disabled")

        factpack_res = factpack_flow_v2(run_date, branch)
        commit_stage_task(run_date, branch, "factpack")

        rag_res = rag_index_flow(run_date, branch)
        commit_stage_task(run_date, branch, "rag_index")

        script_res = script_gen_flow(run_date, branch)
        commit_stage_task(run_date, branch, "script_draft")

        realism_res = realism_flow_v2(run_date, branch)
        commit_stage_task(run_date, branch, "realism")

        verify_res = verify_flow_v2(run_date, branch)
        commit_stage_task(run_date, branch, "verified")

        if settings.elevenlabs_enabled:
            audio_res = elevenlabs_flow(run_date, branch)
            commit_stage_task(run_date, branch, "audio")
        else:
            logger.info("elevenlabs_disabled skipping audio stage")
            audio_res = {"status": "skipped", "reason": "ELEVENLABS_ENABLED=false"}

        publish_res = publish_flow_v2(run_date, branch)
        commit_stage_task(run_date, branch, "published")

        set_pipeline_status(run_date, "succeeded")
        return {
            "run_date": run_date,
            "status": "succeeded",
            "lakefs_branch": branch,
            "stages": {
                "factpack": factpack_res,
                "rag": rag_res,
                "script": script_res,
                "realism": realism_res,
                "verify": verify_res,
                "audio": audio_res,
                "publish": publish_res,
            },
        }
    except Exception:
        set_pipeline_status(run_date, "failed")
        raise
