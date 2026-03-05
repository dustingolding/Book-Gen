from __future__ import annotations

from datetime import date

from prefect import flow, get_run_logger, task

from app.db import set_pipeline_status
from app.flows.ingest_flow import ingest_flow
from app.flows.normalize_flow import normalize_flow
from app.flows.rank_flow import rank_flow
from app.services import editorial_engine


@task
def build_articles_task(run_date: str) -> dict:
    return editorial_engine.run(run_date)


@flow(name="slw-dailycast-editorial")
def editorial_parent_flow(run_date: str | None = None) -> dict:
    run_date = run_date or date.today().isoformat()
    logger = get_run_logger()
    set_pipeline_status(run_date, "running")
    try:
        ingest_res = ingest_flow(run_date)
        normalize_res = normalize_flow(run_date)
        rank_res = rank_flow(run_date)
        article_res = build_articles_task(run_date)
        set_pipeline_status(run_date, "succeeded")
        logger.info(
            "editorial_run_complete run_date=%s article_count=%s",
            run_date,
            article_res.get("article_count", 0),
        )
        return {
            "run_date": run_date,
            "status": "succeeded",
            "stages": {
                "ingest": ingest_res,
                "normalize": normalize_res,
                "rank": rank_res,
                "articles": article_res,
            },
        }
    except Exception:
        set_pipeline_status(run_date, "failed")
        raise
