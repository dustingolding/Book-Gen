from __future__ import annotations

from typing import Any


def validate_blobs(blobs: list[dict[str, Any]]) -> list[str]:
    issues: list[str] = []
    for blob in blobs:
        provenance = blob.get("provenance") or {}
        labels = blob.get("labels") or {}
        typed = ((blob.get("facts") or {}).get("typed_fields") or {})
        if not provenance.get("source_ids"):
            issues.append("blob_missing_provenance")
            break
        if not blob.get("event_time_et"):
            issues.append("blob_missing_event_time")
            break
        if str(blob.get("blob_type", "")) == "news":
            headline = str(typed.get("headline") or typed.get("title") or "")
            summary = str(typed.get("summary") or "")
            if headline and summary and headline.strip() == summary.strip():
                issues.append("blob_news_narrative_too_raw")
                break
        if not labels.get("category"):
            issues.append("blob_missing_category")
            break
    return sorted(set(issues))
