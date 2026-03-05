from datetime import date

from prefect import flow, get_run_logger

from app.db import set_pipeline_status
from app.flows.factpack_flow import factpack_flow
from app.flows.ingest_flow import ingest_flow
from app.flows.normalize_flow import normalize_flow
from app.flows.publish_flow import publish_flow
from app.flows.rank_flow import rank_flow
from app.flows.render_flow import render_flow
from app.flows.verify_flow import verify_flow


@flow(name="slw-dailycast-parent")
def dailycast_parent_flow(run_date: str | None = None) -> dict:
    if not run_date:
        run_date = date.today().isoformat()

    set_pipeline_status(run_date, "running")
    logger = get_run_logger()
    try:
        ingest_res = ingest_flow(run_date)
        normalize_res = normalize_flow(run_date)
        rank_res = rank_flow(run_date)
        factpack_res = factpack_flow(run_date)
        verify_res = verify_flow(run_date)
        quality_res = verify_res.get("quality_gate", {})
        render_res = render_flow(run_date)
        publish_res = publish_flow(run_date)

        logger.info(
            "dailycast_run_summary | run_date=%s scores=%s upcoming=%s headlines=%s events=%s ranked_events=%s factpack_results=%s factpack_matchups=%s factpack_news=%s citation_coverage=%.2f verification_issues=%s",
            run_date,
            ingest_res.get("sports", {}).get("scores", 0),
            ingest_res.get("sports", {}).get("upcoming", 0),
            ingest_res.get("news", {}).get("headlines", 0),
            normalize_res.get("events", 0),
            rank_res.get("ranked_events", 0),
            int(factpack_res.get("yesterday_results", 0)),
            int(factpack_res.get("today_matchups", 0)),
            int(factpack_res.get("major_news", 0)),
            float(quality_res.get("citation_coverage", 0.0)),
            len(verify_res.get("verify", {}).get("issues", [])),
        )

        set_pipeline_status(run_date, "succeeded")
        return {
            "run_date": run_date,
            "status": "succeeded",
            "publish": publish_res,
            "summary": {
                "scores": ingest_res.get("sports", {}).get("scores", 0),
                "upcoming": ingest_res.get("sports", {}).get("upcoming", 0),
                "headlines": ingest_res.get("news", {}).get("headlines", 0),
                "events": normalize_res.get("events", 0),
                "ranked_events": rank_res.get("ranked_events", 0),
                "factpack_results": int(factpack_res.get("yesterday_results", 0)),
                "factpack_matchups": int(factpack_res.get("today_matchups", 0)),
                "factpack_news": int(factpack_res.get("major_news", 0)),
                "citation_coverage": float(quality_res.get("citation_coverage", 0.0)),
                "verification_issue_count": len(verify_res.get("verify", {}).get("issues", [])),
                "render_status": render_res.get("status", "unknown"),
            },
        }
    except Exception:
        set_pipeline_status(run_date, "failed")
        raise


if __name__ == "__main__":
    dailycast_parent_flow()
