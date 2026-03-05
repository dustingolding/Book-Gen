from __future__ import annotations

import logging
from typing import Any

from app.db import fetch_ranked_events
from app.services.editorial_core.coverage_engine import build_editorial_allocations
from app.services.editorial_core.story_selector import build_storysets
from app.services.outputs.article.league_article_builder import build_article
from app.services.providers import provider_registry
from app.services.quality.article_validator import validate_article
from app.services.quality.blob_validator import validate_blobs
from app.services.quality.storyset_validator import validate_storysets
from app.services.scoring.score_engine import score_blobs
from app.storage import ObjectStore
from app.services.factpack import _filter_ranked_rows, _load_blob_schema, _validate_fact_blobs
from app.services.publisher.cms_exporter import FilesystemPublisher

logger = logging.getLogger(__name__)


def _build_scored_blobs(run_date: str) -> list[dict[str, Any]]:
    rows = fetch_ranked_events(run_date)
    if not rows:
        raise RuntimeError("No ranked events available for editorial build.")
    filtered_rows = _filter_ranked_rows(rows)
    if not filtered_rows:
        raise RuntimeError("No ranked events with real citations available for editorial build.")
    raw_blobs: list[dict[str, Any]] = []
    for provider in provider_registry():
        raw_blobs.extend(provider.build(run_date, filtered_rows))
    if not raw_blobs:
        raise RuntimeError("No fact blobs produced for editorial build.")
    schema = _load_blob_schema()
    valid_blobs = _validate_fact_blobs(raw_blobs, schema)
    if not valid_blobs:
        raise RuntimeError("No valid fact blobs remain for editorial build.")
    issues = validate_blobs(valid_blobs)
    if issues:
        raise RuntimeError(f"Blob quality gate failed (issues={issues})")
    return score_blobs(valid_blobs)


def _load_recent_articles(store: ObjectStore | None, sport: str, *, exclude_run_date: str | None = None) -> list[str]:
    if store is None:
        return []
    out: list[str] = []
    prefix = f"articles/archive/{sport}/"
    for obj in store.client.list_objects(store.bucket, prefix=prefix, recursive=True):
        if not obj.object_name.endswith(".json"):
            continue
        if exclude_run_date and obj.object_name.startswith(f"{prefix}{exclude_run_date}-"):
            continue
        data = store.get_json(obj.object_name)
        text = str(data.get("markdown", "")).strip()
        if text:
            out.append(text)
    return out[-30:]


def _load_recent_thesis_cache(store: ObjectStore | None, run_date: str | None = None) -> list[str]:
    if store is None:
        return []
    theses: list[str] = []
    prefix = "editorial/"
    for obj in store.client.list_objects(store.bucket, prefix=prefix, recursive=True):
        key = obj.object_name
        if not key.endswith("/storysets.json"):
            continue
        if run_date and key == f"editorial/{run_date}/storysets.json":
            continue
        try:
            payload = store.get_json(key)
        except Exception:
            continue
        for storyset in payload.get("storysets") or []:
            thesis = str(storyset.get("central_thesis", "")).strip()
            if thesis:
                theses.append(thesis)
    return theses[-50:]


def run(run_date: str) -> dict[str, Any]:
    store: ObjectStore | None = None
    try:
        store = ObjectStore()
    except Exception as exc:
        logger.warning("editorial_engine_object_store_unavailable: %s", exc)
    scored_blobs = _build_scored_blobs(run_date)
    allocations = build_editorial_allocations(run_date=run_date, scored_blobs=scored_blobs)
    recent_thesis_cache = _load_recent_thesis_cache(store, run_date=run_date)
    storysets = build_storysets(scored_blobs, allocations, recent_thesis_cache=recent_thesis_cache)
    storyset_issues = validate_storysets(storysets)
    if storyset_issues:
        raise RuntimeError(f"StorySet quality gate failed (issues={storyset_issues})")
    blob_index = {str(blob.get("blob_id")): blob for blob in scored_blobs}
    published: list[dict[str, Any]] = []
    publisher = FilesystemPublisher()
    publisher.clear_run(run_date)
    built_articles: list[dict[str, Any]] = []
    for storyset in storysets:
        article = build_article(run_date=run_date, storyset=storyset, blob_index=blob_index)
        recent = _load_recent_articles(store, str(article.get("sport", "")), exclude_run_date=run_date)
        issues = validate_article(article, recent)
        if issues:
            logger.warning(
                "editorial_article_rejected sport=%s title=%s issues=%s",
                article.get("sport"),
                article.get("title"),
                issues,
            )
            continue
        publish_res = publisher.publish(run_date=run_date, article=article)
        built_articles.append(article)
        published.append({
            "story_set_id": storyset.get("story_set_id"),
            "title": article.get("title"),
            "sport": article.get("sport"),
            "slug": article.get("slug"),
            "publish": publish_res,
        })
    if not published:
        raise RuntimeError("No article passed validation for filesystem publish.")
    payload = {
        "run_date": run_date,
        "coverage": allocations,
        "storysets": storysets,
        "articles": built_articles,
        "published": published,
        "blob_count": len(scored_blobs),
    }
    if store is not None:
        try:
            store.put_json(f"editorial/{run_date}/storysets.json", {"storysets": storysets})
            store.put_json(f"editorial/{run_date}/articles.json", payload)
        except Exception as exc:
            logger.warning("editorial_engine_object_store_write_failed: %s", exc)
    return {
        "run_date": run_date,
        "blob_count": len(scored_blobs),
        "storyset_count": len(storysets),
        "article_count": len(published),
        "published": published,
    }
