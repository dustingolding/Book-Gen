from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import jsonschema

from app.db import fetch_ranked_events
from app.services.assembly.factpack_agent import build_factpack_from_blobs, build_notebook_docs
from app.services.providers import provider_registry
from app.services.scoring.score_engine import score_blobs
from app.storage import ObjectStore

logger = logging.getLogger(__name__)

EXCLUDED_LEAGUES = {"MLS"}
EXCLUDED_LEAGUE_TOKENS = {
    "NBA G LEAGUE",
    "G LEAGUE",
    "NBAGL",
    "NBA-G LEAGUE",
    "NBA GLEAGUE",
}


def _is_example_source(url: str) -> bool:
    u = (url or "").lower()
    return ".example/" in u or ".example." in u


def _is_excluded_league(league: str) -> bool:
    raw = (league or "").upper().strip()
    if raw in EXCLUDED_LEAGUES:
        return True
    return any(token in raw for token in EXCLUDED_LEAGUE_TOKENS)


def _load_blob_schema() -> dict[str, Any]:
    schema_path = Path("schemas/fact_blob_v1.schema.json")
    return json.loads(schema_path.read_text(encoding="utf-8"))


def _filter_ranked_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    filtered_rows: list[dict[str, Any]] = []
    for row in rows:
        if _is_excluded_league(str(row.get("league", ""))):
            continue

        citations = [
            c
            for c in (row.get("citations") or [])
            if isinstance(c, str) and c.strip() and not _is_example_source(c)
        ]
        if not citations:
            continue

        item = dict(row)
        item["citations"] = citations
        filtered_rows.append(item)

    return filtered_rows


def _validate_fact_blobs(raw_blobs: list[dict[str, Any]], schema: dict[str, Any]) -> list[dict[str, Any]]:
    valid_blobs: list[dict[str, Any]] = []
    for blob in raw_blobs:
        try:
            jsonschema.validate(blob, schema)
            valid_blobs.append(blob)
        except jsonschema.ValidationError as exc:
            logger.warning(
                "fact_blob_schema_reject blob_id=%s reason=%s",
                str(blob.get("blob_id", "")),
                str(exc).splitlines()[0],
            )
    return valid_blobs


def _persist_outputs(run_date: str, factpack: dict[str, Any], docs: dict[str, str]) -> None:
    store = ObjectStore()

    notebook_prefix = f"notebooklm/{run_date}/"
    for obj in store.client.list_objects(store.bucket, prefix=notebook_prefix, recursive=True):
        store.client.remove_object(store.bucket, obj.object_name)

    store.put_json(f"factpacks/{run_date}/factpack.json", factpack)
    for filename, content in docs.items():
        store.put_text(f"notebooklm/{run_date}/{filename}", content, content_type="text/markdown")


def run(run_date: str) -> dict[str, Any]:
    rows = fetch_ranked_events(run_date)
    if not rows:
        raise RuntimeError("No ranked events available for factpack build.")

    filtered_rows = _filter_ranked_rows(rows)
    if not filtered_rows:
        raise RuntimeError("No ranked events with non-example citations available for factpack build.")

    raw_blobs: list[dict[str, Any]] = []
    for provider in provider_registry():
        raw_blobs.extend(provider.build(run_date, filtered_rows))

    if not raw_blobs:
        raise RuntimeError("No fact blobs produced by sport providers.")

    blob_schema = _load_blob_schema()
    valid_blobs = _validate_fact_blobs(raw_blobs, blob_schema)
    if not valid_blobs:
        raise RuntimeError("No valid fact blobs remain after schema validation.")

    scored_blobs = score_blobs(valid_blobs)
    factpack = build_factpack_from_blobs(run_date, scored_blobs)
    docs = build_notebook_docs(factpack)

    _persist_outputs(run_date, factpack, docs)

    return {
        "run_date": run_date,
        "status": "built",
        "factpack_key": f"factpacks/{run_date}/factpack.json",
        "notebook_docs": sorted(docs.keys()),
        "blob_count": len(valid_blobs),
        "yesterday_results": len(factpack.get("yesterday_results", [])),
        "today_matchups": len(factpack.get("today_matchups", [])),
        "major_news": len(factpack.get("major_news", [])),
    }
