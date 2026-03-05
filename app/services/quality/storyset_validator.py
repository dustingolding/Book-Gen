from __future__ import annotations

from typing import Any


def validate_storysets(storysets: list[dict[str, Any]]) -> list[str]:
    issues: list[str] = []
    if not storysets:
        return ["storysets_empty"]
    for story in storysets:
        if not str(story.get("central_thesis", "")).strip():
            issues.append("storyset_missing_thesis")
        if len(story.get("primary_blobs") or []) < 3:
            issues.append("storyset_primary_blobs_below_minimum")
        if len(story.get("context_blobs") or []) < 1:
            issues.append("storyset_context_blobs_missing")
        if not (story.get("seo_targets") or {}).get("primary_keywords"):
            issues.append("storyset_missing_seo_targets")
    return sorted(set(issues))
