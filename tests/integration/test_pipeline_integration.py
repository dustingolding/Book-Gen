import pytest

from app.db import fetch_ingest_rows, initialize_db
from app.flows.dailycast_flow import dailycast_parent_flow
from app.services import ingest_news, ingest_sports
from app.storage import ObjectStore


@pytest.mark.integration
def test_dailycast_flow_end_to_end():
    run_date = "2026-02-01"
    initialize_db()

    result = dailycast_parent_flow(run_date=run_date)
    assert result["status"] == "succeeded"

    store = ObjectStore()
    factpack = store.get_json(f"factpacks/{run_date}/factpack.json")
    manifest = store.get_json(f"publish/{run_date}/manifest.json")

    assert factpack["show_identity"]["name"] == "SideLine Wire DailyCast"
    assert manifest["markdown_key"] == f"publish/{run_date}/dailycast.md"


@pytest.mark.integration
def test_ingest_is_idempotent():
    run_date = "2026-02-02"
    initialize_db()

    ingest_sports.run(run_date)
    ingest_news.run(run_date)
    ingest_sports.run(run_date)
    ingest_news.run(run_date)

    rows = fetch_ingest_rows(run_date)
    assert len(rows) == 2
