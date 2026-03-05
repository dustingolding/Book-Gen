from __future__ import annotations

from collections import Counter
import math
import re
from typing import Any

BANNED_PHRASES = {
    "standings pressure",
    "from an editorial standpoint",
    "keep this in the lead",
    "tiebreak pressure",
    "both teams look to",
    "a big one",
    "what to watch",
    "setting the tone",
    "statement win",
    "they'll look to bounce back",
}


def _ngrams(text: str, n: int = 3) -> set[str]:
    tokens = re.findall(r"[a-z0-9']+", str(text).lower())
    if len(tokens) < n:
        return {" ".join(tokens)} if tokens else set()
    return {" ".join(tokens[i : i + n]) for i in range(len(tokens) - n + 1)}


def _similarity(a: str, b: str) -> float:
    ga = _ngrams(a)
    gb = _ngrams(b)
    if not ga or not gb:
        return 0.0
    return len(ga & gb) / max(1, len(ga | gb))


def _contains_banned(text: str) -> bool:
    low = str(text).lower()
    return any(token in low for token in BANNED_PHRASES)


def _first_team(game: dict[str, Any], key: str) -> str:
    return str(((game.get("facts") or {}).get("typed_fields") or {}).get(key, "")).strip()


def _score_term(sport: str) -> str:
    if sport == "mlb":
        return "runs"
    if sport == "nhl":
        return "goals"
    return "points"


def _margin_phrase(sport: str, margin: int) -> str:
    term = _score_term(sport)
    unit = term[:-1] if abs(margin) == 1 else term
    return f"{margin}-{unit}"


def compute_signal_summary(primary_blobs: list[dict[str, Any]], context_blobs: list[dict[str, Any]], article_type: str) -> dict[str, Any]:
    all_blobs = list(primary_blobs) + list(context_blobs)
    top_games = [b for b in primary_blobs if b.get("blob_type") == "game_result"]
    top_news = [b for b in primary_blobs if b.get("blob_type") == "news"]
    player_lines = [b for b in all_blobs if b.get("blob_type") == "player_line"]
    team_trends = [b for b in all_blobs if b.get("blob_type") == "team_trend"]

    close_finishes = 0
    blowouts = 0
    streak_blobs: list[dict[str, Any]] = []
    for game in top_games:
        tf = ((game.get("facts") or {}).get("typed_fields") or {})
        margin = abs(int(tf.get("margin", 0) or 0))
        if margin <= 5:
            close_finishes += 1
        if margin >= 15:
            blowouts += 1
    for trend in team_trends:
        tf = ((trend.get("facts") or {}).get("typed_fields") or {})
        streak = str(tf.get("streak", "")).strip().upper()
        m = re.match(r"([WL])(\d+)", streak)
        if m and int(m.group(2)) >= 4:
            streak_blobs.append(trend)

    categories = Counter(str(((b.get("labels") or {}).get("category") or "other")).lower() for b in top_news)
    top_score = max((float(((b.get("scoring") or {}).get("final_priority_score") or 0.0)) for b in primary_blobs), default=0.0)
    top_k = [float(((b.get("scoring") or {}).get("final_priority_score") or 0.0)) for b in primary_blobs[:5]]
    dominant_ratio = top_score / max(1.0, sum(top_k)) if top_k else 0.0
    spread = top_score - (sorted(top_k)[len(top_k) // 2] if top_k else 0.0)

    return {
        "top_games": top_games,
        "top_news": top_news,
        "top_players": player_lines[:8],
        "streak_blobs": streak_blobs,
        "close_finishes": close_finishes,
        "blowouts": blowouts,
        "categories": dict(categories),
        "dominant_story_score_ratio": round(dominant_ratio, 4),
        "spread": round(spread, 4),
        "article_type": article_type,
    }


def choose_thesis_type(signals: dict[str, Any], article_type: str, season_phase: str) -> str | None:
    categories = signals.get("categories") or {}
    top_news = signals.get("top_news") or []
    top_games = signals.get("top_games") or []
    streaks = signals.get("streak_blobs") or []
    if article_type == "news_focus":
        if categories.get("injury"):
            return "injury_ripple"
        if categories.get("trade") or categories.get("free_agency") or categories.get("coaching"):
            return "transaction_shift"
        if categories.get("combine") or categories.get("draft"):
            return "prospect_momentum"
        if top_news:
            return "news_cycle"
        return None
    if not top_games:
        return None
    if signals.get("close_finishes", 0) >= 2:
        return "tight_race"
    if signals.get("blowouts", 0) >= 2:
        return "power_shift"
    if top_games:
        return "top_games"
    if categories.get("injury"):
        return "injury_ripple"
    if streaks:
        return "streaks_trend"
    return None


def fill_slots(thesis_type: str, sport: str, signals: dict[str, Any]) -> dict[str, Any]:
    top_games = signals.get("top_games") or []
    top_news = signals.get("top_news") or []
    streaks = signals.get("streak_blobs") or []
    if thesis_type in {"tight_race", "power_shift", "top_games"} and top_games:
        lead = top_games[0]
        tf = ((lead.get("facts") or {}).get("typed_fields") or {})
        return {
            "blob_ids": [lead.get("blob_id")],
            "team_a": _first_team(lead, "winner") or _first_team(lead, "home_team"),
            "team_b": _first_team(lead, "loser") or _first_team(lead, "away_team"),
            "margin": int(tf.get("margin", 0) or 0),
            "scoreline": f"{tf.get('winner','')} {tf.get('home_score','')}-{tf.get('away_score','')}",
            "sport": sport,
        }
    if thesis_type == "streaks_trend" and streaks:
        lead = streaks[0]
        tf = ((lead.get("facts") or {}).get("typed_fields") or {})
        return {
            "blob_ids": [lead.get("blob_id")],
            "team": str(tf.get("name", "")).strip(),
            "streak": str(tf.get("streak", "")).strip(),
            "last10": str(tf.get("last10", "")).strip(),
            "sport": sport,
        }
    if thesis_type in {"injury_ripple", "transaction_shift", "prospect_momentum", "news_cycle"} and top_news:
        lead = top_news[0]
        tf = ((lead.get("facts") or {}).get("typed_fields") or {})
        return {
            "blob_ids": [lead.get("blob_id")],
            "headline": str(tf.get("headline", "")).strip(),
            "summary": str(((lead.get("facts") or {}).get("typed_fields") or {}).get("summary", "")).strip(),
            "sport": sport,
        }
    return {"blob_ids": [], "sport": sport}


def render_thesis(thesis_type: str, slots: dict[str, Any]) -> str:
    sport = str(slots.get("sport", "")).replace("_", " ")
    margin = int(slots.get("margin", 0) or 0)
    if thesis_type == "tight_race":
        return (
            f"{slots['team_a']} versus {slots['team_b']} produced the lead {sport} result after a "
            f"{_margin_phrase(str(slots.get('sport', '')), margin)} finish with immediate standings weight."
        )
    if thesis_type == "power_shift":
        return (
            f"{slots['team_a']} over {slots['team_b']} was the lead {sport} result after a "
            f"{_margin_phrase(str(slots.get('sport', '')), margin)} margin that stood above the rest of the slate."
        )
    if thesis_type == "top_games":
        return (
            f"{slots['team_a']} over {slots['team_b']} was the lead {sport} result after a "
            f"{_margin_phrase(str(slots.get('sport', '')), margin)} finish that shaped the rest of the slate."
        )
    if thesis_type == "streaks_trend":
        return f"{slots['team']} is the clearest trend line in the {slots['sport'].replace('_', ' ')} file with a {slots['streak']} streak and {slots['last10']} recent form."
    if thesis_type == "injury_ripple":
        return f"{slots['headline']} became the central story because its impact reaches beyond one result and changes the shape of the league week."
    if thesis_type == "transaction_shift":
        return f"{slots['headline']} is the story because it changes roster leverage and reshapes the competitive picture immediately."
    if thesis_type == "prospect_momentum":
        return f"{slots['headline']} is the key development because it shifts draft and evaluation momentum across the league."
    if thesis_type == "news_cycle":
        return f"{slots['headline']} is the leading story because it carries the clearest downstream impact on the next phase of league play."
    return ""


def score_confidence(signals: dict[str, Any], thesis_type: str) -> float:
    ratio = float(signals.get("dominant_story_score_ratio", 0.0) or 0.0)
    spread = float(signals.get("spread", 0.0) or 0.0)
    base = 0.55 + min(0.2, ratio) + min(0.2, spread / 100.0)
    if thesis_type in {"injury_ripple", "transaction_shift", "prospect_momentum"}:
        base += 0.05
    return round(min(0.99, base), 3)


def thesis_quality_ok(thesis: str, slot_map: dict[str, Any], recent_thesis_cache: list[str], thesis_type: str) -> bool:
    words = thesis.split()
    if len(words) < 10 or len(words) > 36:
        return False
    if _contains_banned(thesis):
        return False
    needs_numeric = thesis_type not in {"injury_ripple", "transaction_shift", "prospect_momentum", "news_cycle"}
    if needs_numeric and not re.search(r"\d", thesis):
        return False
    if not any(slot_map.get(key) for key in ("team", "team_a", "headline")):
        return False
    if len(slot_map.get("blob_ids") or []) < 1:
        return False
    if any(_similarity(thesis, prev) >= 0.70 for prev in recent_thesis_cache[-30:]):
        return False
    return True


def build_thesis(
    *,
    scored_blobs: list[dict[str, Any]],
    sport: str,
    article_type: str,
    season_phase: str,
    run_date: str,
    recent_thesis_cache: list[str],
    primary_blobs: list[dict[str, Any]],
    context_blobs: list[dict[str, Any]],
) -> dict[str, Any] | None:
    _ = run_date
    signals = compute_signal_summary(primary_blobs, context_blobs, article_type)
    thesis_type = choose_thesis_type(signals, article_type, season_phase)
    if thesis_type is None:
        return None
    slots = fill_slots(thesis_type, sport, signals)
    thesis = render_thesis(thesis_type, slots)
    if not thesis or not thesis_quality_ok(thesis, slots, recent_thesis_cache, thesis_type):
        return None
    return {
        "central_thesis": thesis,
        "thesis_type": thesis_type,
        "thesis_signals": signals,
        "thesis_confidence": score_confidence(signals, thesis_type),
        "slot_map": slots,
    }
