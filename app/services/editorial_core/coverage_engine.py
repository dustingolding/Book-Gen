from __future__ import annotations

from dataclasses import asdict
from datetime import date
from typing import Any

from app.services.coverage import allocate_show_time, compute_sport_states, load_yaml
from app.services.editorial_core.story_potential import build_story_potential


EDITORIAL_RUNTIME_TARGET = 36.0


def build_editorial_allocations(*, run_date: str, scored_blobs: list[dict[str, Any]]) -> dict[str, Any]:
    seasonality_cfg = load_yaml("config/seasonality.yaml")
    weights_cfg = load_yaml("config/coverage_weights.yaml")
    on_date = date.fromisoformat(run_date)

    story_potential = build_story_potential(run_date=run_date, scored_blobs=scored_blobs)
    activity_units = {
        sport: int(metrics.get("activity_units", 0) or 0)
        for sport, metrics in story_potential.items()
    }
    big_news_flags: dict[str, bool] = {}
    major_international_event = False

    for blob in scored_blobs:
        sport = str(blob.get("sport", "")).strip()
        if not sport:
            continue
        btype = str(blob.get("blob_type", "")).strip()
        score = float(((blob.get("scoring") or {}).get("final_priority_score") or 0.0))
        if btype == "news" and score >= 85.0:
            big_news_flags[sport] = True
            if sport == "major_international":
                major_international_event = True

    sport_states = compute_sport_states(
        on_date=on_date,
        seasonality_cfg=seasonality_cfg,
        weights_cfg=weights_cfg,
        game_counts_yesterday=activity_units,
        big_news_flags=big_news_flags,
        major_international_event_flag=major_international_event,
    )
    allocations = allocate_show_time(
        sport_states=sport_states,
        weights_cfg=weights_cfg,
        runtime_minutes_target=EDITORIAL_RUNTIME_TARGET,
    )
    allocation_rows: list[dict[str, Any]] = []
    for allocation in allocations:
        row = asdict(allocation)
        row["story_potential"] = story_potential.get(allocation.sport, {})
        allocation_rows.append(row)
    return {
        "runtime_target_minutes": EDITORIAL_RUNTIME_TARGET,
        "sport_states": [asdict(x) for x in sport_states],
        "allocations": allocation_rows,
        "story_potential": story_potential,
    }
