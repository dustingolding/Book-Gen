from __future__ import annotations

import re
from typing import Any


def slugify(text: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", str(text).lower()).strip("-")
    return re.sub(r"-+", "-", text)[:120]


def title_case_sport(sport: str) -> str:
    mapping = {
        "nba": "NBA",
        "nfl": "NFL",
        "nhl": "NHL",
        "mlb": "MLB",
        "wnba": "WNBA",
        "college_basketball": "Men's College Basketball",
        "womens_college_basketball": "Women's College Basketball",
        "college_football": "College Football",
        "major_international": "International Sports",
    }
    return mapping.get(sport, sport.replace("_", " ").title())


def build_internal_links(teams: list[str], players: list[str], sport: str) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    for team in teams[:5]:
        links.append({"label": team, "href": f"/{sport}/teams/{slugify(team)}/"})
    for player in players[:5]:
        links.append({"label": player, "href": f"/{sport}/players/{slugify(player)}/"})
    return links


def stat_line_from_blob(blob: dict[str, Any]) -> str:
    tf = ((blob.get("facts") or {}).get("typed_fields") or {})
    if tf.get("player_line"):
        return str(tf.get("player_line"))
    stats = tf.get("stats") or {}
    if not stats:
        return ""
    parts = [f"{v} {k}" for k, v in stats.items() if v]
    name = str(tf.get("player_name", "")).strip()
    return f"{name} - {', '.join(parts)}".strip(" -")
