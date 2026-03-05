import logging

from app.clients.news_api import NewsClient
from app.db import upsert_ingest_row
from app.storage import ObjectStore

logger = logging.getLogger(__name__)


def run(run_date: str) -> dict:
    client = NewsClient()
    store = ObjectStore()

    headlines = client.fetch_headlines(run_date)
    payload = {"run_date": run_date, "headlines": headlines}

    upsert_ingest_row(f"news:{run_date}", run_date, "news", payload)
    store.put_json(f"raw/{run_date}/news.json", payload)

    logger.info("news_ingest_complete", extra={"run_date": run_date, "headlines": len(headlines)})
    return {"headlines": len(headlines)}
