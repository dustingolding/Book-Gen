from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any


POWER_FOUR_TEAMS = {
    "ARIZ", "ASU", "BAY", "BC", "CAL", "CLEM", "COLO", "DUKE", "FSU", "GT", "IOWA", "ILL", "IU", "KAN",
    "KSU", "KU", "LOU", "LSU", "MD", "MICH", "MINN", "MISS", "MIZ", "MSST", "MSU", "NCST", "NEB", "NW",
    "OHST", "OKLA", "ORE", "OSU", "PSU", "PITT", "PUR", "RUTG", "SMU", "STAN", "SYR", "TA&M", "TEX",
    "TCU", "TENN", "TTU", "UCF", "UCLA", "UNC", "USC", "UTAH", "UVA", "VT", "WAKE", "WASH", "WIS", "WVU",
}


def _safe_rank(value: Any) -> int | None:
    try:
        rank = int(value)
    except Exception:
        return None
    return rank if 0 < rank <= 25 else None


def _run_date(run_date: str) -> date:
    return date.fromisoformat(run_date)


def _in_window(on_date: date, start: str, end: str) -> bool:
    return date.fromisoformat(start) <= on_date <= date.fromisoformat(end)


def _conference_tournament_window(sport: str, on_date: date) -> bool:
    windows = {
        "college_basketball": ("2026-03-08", "2026-03-15"),
        "womens_college_basketball": ("2026-03-04", "2026-03-15"),
    }
    window = windows.get(sport)
    if not window:
        return False
    return _in_window(on_date, window[0], window[1])


def _is_power_four_team(team: str) -> bool:
    return str(team or "").upper().strip() in POWER_FOUR_TEAMS


def _game_potential_increment(blob: dict[str, Any], on_date: date) -> float:
    sport = str(blob.get("sport", "")).strip()
    labels = blob.get("labels") or {}
    fields = ((blob.get("facts") or {}).get("typed_fields") or {})

    score = 1.0
    home_rank = _safe_rank(fields.get("home_rank"))
    away_rank = _safe_rank(fields.get("away_rank"))
    ranked_teams = int(home_rank is not None) + int(away_rank is not None)
    ranked_matchup = ranked_teams == 2

    score += 0.5 * ranked_teams
    if ranked_matchup:
        score += 1.0

    if sport in {"nba", "nhl"} and bool(fields.get("playoff_implication")):
        score += 0.5

    if sport in {"college_basketball", "womens_college_basketball"}:
        home_team = str(fields.get("home_team", "")).strip()
        away_team = str(fields.get("away_team", "")).strip()
        if _conference_tournament_window(sport, on_date) and _is_power_four_team(home_team) and _is_power_four_team(away_team):
            score += 1.5
        phase = str(labels.get("season_phase", "")).upper()
        if phase in {"POSTSEASON", "CHAMPIONSHIP_WINDOW"}:
            # NCAA tournament / Final Four window deserves a stronger game-only potential bump.
            score += 2.0
            if ranked_matchup:
                score += 0.5

    if sport == "college_football":
        phase = str(labels.get("season_phase", "")).upper()
        if phase in {"POSTSEASON", "CHAMPIONSHIP_WINDOW"}:
            score += 1.5

    return score


def _event_potential_increment(blob: dict[str, Any]) -> float:
    if str(blob.get("blob_type", "")).strip() != "news":
        return 0.0
    labels = blob.get("labels") or {}
    category = str(labels.get("category", "")).strip().lower()
    score = float(((blob.get("scoring") or {}).get("final_priority_score") or 0.0))
    if score < 40.0:
        return 0.0
    weights = {
        "combine": 2.0,
        "draft": 1.5,
        "free_agency": 1.2,
        "trade": 1.0,
        "injury": 0.8,
        "coaching": 0.8,
        "discipline": 0.6,
        "record_watch": 0.6,
        "milestone": 0.5,
    }
    return weights.get(category, 0.0)


def build_story_potential(*, run_date: str, scored_blobs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    on_date = _run_date(run_date)
    out: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "raw_game_count": 0,
        "game_potential_score": 0.0,
        "event_potential_score": 0.0,
        "activity_units": 0,
        "ranked_games": 0,
        "ranked_team_appearances": 0,
        "power_four_tournament_games": 0,
        "ncaa_tournament_games": 0,
    })

    for blob in scored_blobs:
        sport = str(blob.get("sport", "")).strip()
        if not sport:
            continue
        bucket = out[sport]
        btype = str(blob.get("blob_type", "")).strip()
        fields = ((blob.get("facts") or {}).get("typed_fields") or {})
        labels = blob.get("labels") or {}

        if btype == "game_result":
            bucket["raw_game_count"] += 1
            inc = _game_potential_increment(blob, on_date)
            bucket["game_potential_score"] += inc
            home_rank = _safe_rank(fields.get("home_rank"))
            away_rank = _safe_rank(fields.get("away_rank"))
            ranked_teams = int(home_rank is not None) + int(away_rank is not None)
            bucket["ranked_team_appearances"] += ranked_teams
            if ranked_teams == 2:
                bucket["ranked_games"] += 1
            if sport in {"college_basketball", "womens_college_basketball"}:
                home_team = str(fields.get("home_team", "")).strip()
                away_team = str(fields.get("away_team", "")).strip()
                if _conference_tournament_window(sport, on_date) and _is_power_four_team(home_team) and _is_power_four_team(away_team):
                    bucket["power_four_tournament_games"] += 1
                phase = str(labels.get("season_phase", "")).upper()
                if phase in {"POSTSEASON", "CHAMPIONSHIP_WINDOW"}:
                    bucket["ncaa_tournament_games"] += 1
        elif btype == "news":
            bucket["event_potential_score"] += _event_potential_increment(blob)

    for sport, bucket in out.items():
        activity_units = max(
            int(bucket["raw_game_count"]),
            int(round(float(bucket["game_potential_score"]) + float(bucket["event_potential_score"]))),
        )
        bucket["activity_units"] = activity_units
        bucket["sport"] = sport

    return dict(out)
