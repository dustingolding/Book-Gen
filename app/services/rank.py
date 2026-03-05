import logging
from statistics import mean

import mlflow

from app.config import get_settings
from app.db import delete_ranked_events, fetch_normalized_events, upsert_ranked_event
from app.service_endpoints import resolve_service_uri
from app.storage import ObjectStore

logger = logging.getLogger(__name__)


def _score_event(event: dict) -> tuple[float, str]:
    base = 0.35
    if event["event_type"] == "score":
        margin = event["metrics"].get("score_margin", 0)
        score = base + min(0.4, margin / 50.0)
        return score, f"score_event_margin:{margin}"
    if event["event_type"] == "headline":
        score = base + 0.25
        return score, "headline_relevance"
    if event["event_type"] == "upcoming":
        score = base + 0.15
        return score, "upcoming_schedule"
    return base, "default"


def run(run_date: str) -> dict:
    cfg = get_settings()
    events = fetch_normalized_events(run_date)
    if not events:
        raise RuntimeError("No normalized events for ranking.")
    delete_ranked_events(run_date)

    mlflow.set_tracking_uri(resolve_service_uri(cfg.mlflow_tracking_uri, cfg.mlflow_local_tracking_uri))
    mlflow.set_experiment("slw-dailycast-ranking")

    scores = []
    with mlflow.start_run(run_name=f"rank-{run_date}"):
        for e in events:
            score, rationale = _score_event(e)
            upsert_ranked_event(run_date, e["event_id"], score, rationale)
            mlflow.log_metric(f"score_{e['event_id']}", score)
            scores.append(score)

        mlflow.log_param("event_count", len(events))
        mlflow.log_metric("avg_score", mean(scores))
        mlflow.set_tag("pipeline", "slw-dailycast")

        model_info = {
            "name": "ranking-baseline-v1",
            "description": "Heuristic ranking model tracked for reproducibility.",
            "stage": "Staging",
            "run_date": run_date,
        }
        # Artifact upload may fail in local port-forward validation when the tracking
        # server advertises a local filesystem artifact root (e.g. /mlflow). Keep
        # the ranking run successful and retain metric/param logging.
        try:
            mlflow.log_dict(model_info, "model_registry_snapshot.json")
        except Exception as exc:
            # Local port-forward validations may hit legacy local artifact roots.
            # Keep this non-fatal and low-noise.
            logger.info("mlflow_artifact_log_skipped", extra={"error": str(exc)})

    store = ObjectStore()
    store.put_json(
        f"mlflow/{run_date}/ranking_summary.json",
        {"run_date": run_date, "event_count": len(events), "avg_score": mean(scores)},
    )

    logger.info("ranking_complete", extra={"run_date": run_date, "events": len(events)})
    return {"ranked_events": len(events)}
