import logging
from datetime import date, timedelta

from app.clients.sports_api import SportsClient
from app.db import upsert_ingest_row
from app.storage import ObjectStore

logger = logging.getLogger(__name__)


def run(run_date: str) -> dict:
    client = SportsClient()
    store = ObjectStore()

    target_date = (date.fromisoformat(run_date) - timedelta(days=1)).isoformat()
    scores = client.fetch_previous_day_scores(target_date)
    upcoming = client.fetch_upcoming_matchups(run_date)

    payload = {
        "run_date": run_date,
        "previous_day": target_date,
        "scores": scores,
        "upcoming": upcoming,
    }

    upsert_ingest_row(f"sports:{run_date}", run_date, "sports", payload)
    store.put_json(f"raw/{run_date}/sports.json", payload)

    logger.info("sports_ingest_complete", extra={"run_date": run_date, "scores": len(scores)})
    return {"scores": len(scores), "upcoming": len(upcoming)}
