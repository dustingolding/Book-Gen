import json
from datetime import date, timedelta
from pathlib import Path
from collections import defaultdict

import jsonschema

from app.db import fetch_ranked_events
from app.storage import ObjectStore


def _is_example_source(url: str) -> bool:
    u = (url or "").lower()
    return ".example/" in u or ".example." in u


def _normalized_league(league: str) -> str:
    raw = (league or "").upper().strip()
    if "NCAA" in raw:
        return "NCAA_BASKETBALL"
    if raw in {"NCAAM", "NCAAB", "NCAA", "NCAAW"}:
        return "NCAA_BASKETBALL"
    return raw


def _seasonal_league_boosts(run_date: str) -> dict[str, float]:
    d = date.fromisoformat(run_date)
    boosts: dict[str, float] = {}

    # College basketball tournament window: lean heavier but not dominant.
    if date(d.year, 2, 15) <= d <= date(d.year, 4, 15):
        boosts["NCAA_BASKETBALL"] = 0.22
    # NBA/NHL stretch and playoffs.
    if date(d.year, 3, 15) <= d <= date(d.year, 6, 30):
        boosts["NBA"] = 0.12
        boosts["NHL"] = 0.12
    # NFL playoffs/Super Bowl window.
    if date(d.year, 1, 1) <= d <= date(d.year, 2, 20):
        boosts["NFL"] = 0.15
    # MLB postseason window.
    if date(d.year, 9, 1) <= d <= date(d.year, 11, 10):
        boosts["MLB"] = 0.15
    # MLS playoffs window.
    if date(d.year, 10, 1) <= d <= date(d.year, 12, 15):
        boosts["MLS"] = 0.12
    return boosts


def _weighted_score(row: dict, boosts: dict[str, float]) -> float:
    league = _normalized_league(str(row.get("league", "")))
    base = float(row.get("score", 0.0) or 0.0)
    bump = boosts.get(league, 0.0)
    return base * (1.0 + bump)


def _select_weighted_rows(rows: list[dict], limit: int, boosts: dict[str, float], per_league_cap: int) -> list[dict]:
    ranked = sorted(rows, key=lambda r: _weighted_score(r, boosts), reverse=True)
    selected: list[dict] = []
    league_counts: dict[str, int] = defaultdict(int)

    for row in ranked:
        league_key = _normalized_league(str(row.get("league", "")))
        if league_counts[league_key] >= per_league_cap:
            continue
        selected.append(row)
        league_counts[league_key] += 1
        if len(selected) >= limit:
            return selected

    # If caps filtered too much, fill remaining slots by raw order.
    for row in ranked:
        if row in selected:
            continue
        selected.append(row)
        if len(selected) >= limit:
            break
    return selected


def run(run_date: str) -> dict:
    ranked = fetch_ranked_events(run_date)
    if not ranked:
        raise RuntimeError("No ranked events available for briefpack build.")

    filtered_ranked = []
    for r in ranked:
        citations = [c for c in (r.get("citations") or []) if isinstance(c, str) and not _is_example_source(c)]
        if not citations:
            continue
        r = dict(r)
        r["citations"] = citations
        filtered_ranked.append(r)
    ranked = filtered_ranked
    if not ranked:
        raise RuntimeError("No ranked events with non-example citations available for briefpack build.")

    prev_day = (date.fromisoformat(run_date) - timedelta(days=1)).isoformat()
    league_boosts = _seasonal_league_boosts(run_date)
    top_scores_all = [r for r in ranked if r["event_type"] == "score"]
    top_scores_filtered = [
        r
        for r in top_scores_all
        if str((r.get("metrics") or {}).get("status", "")).lower() == "final"
        and str((r.get("metrics") or {}).get("game_date", "")) == prev_day
    ]
    top_scores = _select_weighted_rows(top_scores_filtered, limit=6, boosts=league_boosts, per_league_cap=3)
    headlines = _select_weighted_rows(
        [r for r in ranked if r["event_type"] == "headline"],
        limit=5,
        boosts=league_boosts,
        per_league_cap=3,
    )
    upcoming_all = [r for r in ranked if r["event_type"] == "upcoming"]
    upcoming_filtered = [
        r
        for r in upcoming_all
        if str((r.get("metrics") or {}).get("scheduled_time_utc", "")).startswith(run_date)
    ]
    upcoming = _select_weighted_rows(upcoming_filtered, limit=6, boosts=league_boosts, per_league_cap=3)

    coverage_flags = {
        "partial_scores_coverage": len(top_scores) < 3,
        "partial_headline_coverage": len(headlines) < 3,
        "partial_upcoming_coverage": len(upcoming) < 2,
        "expected_score_date": prev_day,
        "seasonality": {
            "active_focus_leagues": [k for k, v in sorted(league_boosts.items()) if v > 0.0],
            "league_focus_weights": {k: round(1.0 + v, 2) for k, v in league_boosts.items()},
            "balance_guardrail_max_share_per_league": 0.50,
        },
    }

    citations = sorted({c for r in ranked for c in r["citations"]})

    briefpack = {
        "run_date": run_date,
        "top_scores": [
            {
                "event_id": t["event_id"],
                "title": t["title"],
                "summary": t["summary"],
                "league": t["league"],
                "score": t["score"],
                "citations": t["citations"],
            }
            for t in top_scores
        ],
        "headlines": [
            {
                "event_id": h["event_id"],
                "title": h["title"],
                "summary": h["summary"],
                "score": h["score"],
                "citations": h["citations"],
            }
            for h in headlines
        ],
        "upcoming_matchups": [
            {
                "event_id": u["event_id"],
                "title": u["title"],
                "summary": u["summary"],
                "score": u["score"],
                "citations": u["citations"],
            }
            for u in upcoming
        ],
        "citations": citations,
        "flags": coverage_flags,
    }

    schema_path = Path("schemas/briefpack.schema.json")
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    jsonschema.validate(briefpack, schema)

    store = ObjectStore()
    store.put_json(f"briefpacks/{run_date}/briefpack.json", briefpack)
    return briefpack
