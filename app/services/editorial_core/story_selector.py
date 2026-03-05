from __future__ import annotations

from collections import defaultdict
import re
from typing import Any

from app.services.editorial_core.thesis_builder import build_thesis

SPORT_ORDER = [
    "college_football",
    "nfl",
    "college_basketball",
    "nba",
    "mlb",
    "womens_college_basketball",
    "wnba",
    "nhl",
    "major_international",
]

PRIMARY_SPORTS = {"nba", "nfl", "college_basketball", "womens_college_basketball"}

GAME_FIRST_SPORTS = {"nba", "college_basketball", "womens_college_basketball", "nhl", "mlb", "wnba"}
NEWS_FIRST_SPORTS = {"nfl", "college_football"}

JUNK_PATTERNS = (
    r"\bcoupon\b",
    r"\bair compressor\b",
    r"\bportable cordless\b",
    r"\bfantasy baseball\b",
    r"\bhow to watch\b",
    r"\bbest goals?\b",
    r"\bpromo code\b",
    r"\btragic accident\b",
    r"\bdies at age\b",
    r"\bcelebrity\b",
    r"\bshopping\b",
    r"\bfantasy hockey\b",
    r"\bfantasy football\b",
    r"\bfantasy basketball\b",
    r"\bviral stars?\b",
    r"\bmock draft\b",
    r"\broundup\b",
)

SPORT_REQUIRED_PATTERNS: dict[str, tuple[str, ...]] = {
    "nba": (r"\bnba\b", r"\bknicks\b", r"\bceltics\b", r"\blakers\b", r"\bspurs\b", r"\bhawks\b", r"\bbulls\b"),
    "nfl": (r"\bnfl\b", r"\bcombine\b", r"\bdraft\b", r"\bfree agency\b", r"\btrade\b", r"\bbears\b", r"\bchiefs\b", r"\bbrowns\b"),
    "nhl": (r"\bnhl\b", r"\btrade deadline\b", r"\bplayoffs\b", r"\bpenguins\b", r"\bknights\b", r"\bislanders\b", r"\bpanthers\b"),
    "mlb": (r"\bmlb\b", r"\bspring training\b", r"\bopening day\b", r"\brotation\b", r"\bgiants\b", r"\bpadres\b", r"\bcardinals\b"),
    "college_basketball": (r"\bncaa\b", r"\bmarch madness\b", r"\bconference tournament\b", r"\bseed", r"\bboilermakers\b", r"\bspartans\b"),
    "womens_college_basketball": (r"\bwomen'?s\b", r"\bncaa\b", r"\bseed", r"\bbracket\b", r"\bhuskies\b", r"\bmountaineers\b", r"\bcyclones\b"),
}

SPORT_FORBIDDEN_PATTERNS: dict[str, tuple[str, ...]] = {
    "nba": (r"\bnhl\b", r"\bfantasy hockey\b", r"\brangers\b", r"\bmlb\b", r"\bfantasy baseball\b"),
    "nfl": (r"\bfantasy baseball\b", r"\bnhl\b", r"\bmlb spring training\b"),
    "nhl": (r"\bfantasy baseball\b", r"\bnba\b", r"\bmlb\b", r"\bfantasy basketball\b"),
    "mlb": (r"\bnhl\b", r"\bfantasy hockey\b", r"\bnba\b"),
    "college_basketball": (r"\bfantasy baseball\b", r"\bair compressor\b", r"\btragic accident\b"),
    "womens_college_basketball": (r"\bfantasy baseball\b", r"\bair compressor\b", r"\btragic accident\b"),
}


def _score(blob: dict[str, Any]) -> float:
    return float(((blob.get("scoring") or {}).get("final_priority_score") or 0.0))


def _typed(blob: dict[str, Any]) -> dict[str, Any]:
    return ((blob.get("facts") or {}).get("typed_fields") or {})


def _category(blob: dict[str, Any]) -> str:
    return str(((blob.get("labels") or {}).get("category") or "other")).lower()


def _text(blob: dict[str, Any]) -> str:
    tf = _typed(blob)
    return " ".join(
        [
            str(tf.get("headline") or ""),
            str(tf.get("title") or ""),
            str(tf.get("summary") or ""),
            str(blob.get("blob_type") or ""),
            str(blob.get("sport") or ""),
        ]
    ).strip()


def _is_clean_for_sport(blob: dict[str, Any], sport: str) -> bool:
    blob_sport = str(blob.get("sport", "")).strip()
    if blob_sport and blob_sport != sport:
        return False
    text = _text(blob).lower()
    if any(re.search(pattern, text) for pattern in JUNK_PATTERNS):
        return False
    forbidden = SPORT_FORBIDDEN_PATTERNS.get(sport, ())
    if any(re.search(pattern, text) for pattern in forbidden):
        return False
    required = SPORT_REQUIRED_PATTERNS.get(sport, ())
    if blob.get("blob_type") == "news" and required and not any(re.search(pattern, text) for pattern in required):
        return False
    if blob.get("blob_type") == "news":
        if "mock draft" in text:
            return False
        if "preview" in text and "combine" not in text and "trade" not in text and "injury" not in text:
            return False
    return True


def _seo_targets(blobs: list[dict[str, Any]], sport: str, thesis: str) -> dict[str, list[str]]:
    teams: list[str] = []
    players: list[str] = []
    for blob in blobs:
        tf = _typed(blob)
        for key in ("winner", "loser", "home_team", "away_team", "team", "name"):
            v = str(tf.get(key, "")).strip()
            if v and v not in teams:
                teams.append(v)
        player_name = str(tf.get("player_name", "")).strip()
        if player_name and player_name not in players:
            players.append(player_name)
    primary = [sport.replace("_", " "), thesis]
    primary.extend(teams[:3])
    secondary = teams[3:6] + players[:6]
    return {
        "primary_keywords": [x for x in primary if x][:6],
        "secondary_keywords": [x for x in secondary if x][:10],
    }


def _article_type_for_sport(sport: str, grouped: dict[str, list[dict[str, Any]]]) -> str | None:
    games = sorted(grouped.get("game_result", []), key=_score, reverse=True)
    news = sorted(grouped.get("news", []), key=_score, reverse=True)

    high_games = [b for b in games if _score(b) >= 0.45]
    high_news = [b for b in news if _score(b) >= 0.40]

    game_threshold = 3
    if sport == "college_basketball":
        season_phases = {
            str(((b.get("labels") or {}).get("season_phase") or "")).upper()
            for b in high_games
        }
        clean_player_lines = [b for b in grouped.get("player_line", []) if _is_clean_for_sport(b, sport)]
        clean_team_trends = [b for b in grouped.get("team_trend", []) if _is_clean_for_sport(b, sport)]
        if len(high_games) >= 2 and season_phases & {"REGULAR_SEASON", "POSTSEASON", "CHAMPIONSHIP_WINDOW"} and len(clean_player_lines) >= 4 and len(clean_team_trends) >= 2:
            game_threshold = 2

    if sport in NEWS_FIRST_SPORTS:
        if len(high_news) >= 3:
            return "news_focus"
        if len(high_games) >= game_threshold:
            return "top_games"
        return None

    if sport in GAME_FIRST_SPORTS:
        if len(high_games) >= game_threshold:
            return "top_games"
        if len(high_news) >= 3:
            return "news_focus"
        return None

    if len(high_games) >= game_threshold:
        return "top_games"
    if len(high_news) >= 3:
        return "news_focus"
    return None


def _minimum_primary_count(
    sport: str,
    article_type: str,
    grouped: dict[str, list[dict[str, Any]]],
    coverage_allocation: dict[str, Any],
) -> int:
    if article_type == "news_focus":
        return 3
    minimum = 3
    story_potential = coverage_allocation.get("story_potential") or {}
    season_phase = {
        str(((b.get("labels") or {}).get("season_phase") or "")).upper()
        for b in grouped.get("game_result", [])
    }
    if sport in {"college_basketball", "womens_college_basketball"}:
        strong_context = len(grouped.get("player_line", [])) >= 4 and len(grouped.get("team_trend", [])) >= 2
        if (
            int(story_potential.get("power_four_tournament_games", 0) or 0) >= 1
            or int(story_potential.get("ncaa_tournament_games", 0) or 0) >= 1
            or (season_phase & {"POSTSEASON", "CHAMPIONSHIP_WINDOW"})
        ) and strong_context:
            return 2
        if sport == "college_basketball" and strong_context and int(story_potential.get("ranked_team_appearances", 0) or 0) >= 4:
            return 2
    return minimum


def _build_primary_blobs(sport: str, article_type: str, grouped: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    if article_type == "news_focus":
        news = [b for b in grouped.get("news", []) if _is_clean_for_sport(b, sport)]
        news = sorted(news, key=_score, reverse=True)
        if sport == "nfl":
            preferred = {"combine", "draft", "free_agency", "injury", "coaching"}
            news.sort(key=lambda b: (_category(b) not in preferred, -_score(b)))
        return news[:6]

    games = [b for b in grouped.get("game_result", []) if _is_clean_for_sport(b, sport)]
    games = sorted(games, key=_score, reverse=True)
    return games[:6]


def _build_context_blobs(sport: str, article_type: str, grouped: dict[str, list[dict[str, Any]]], primary_blobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = {str(blob.get("blob_id")) for blob in primary_blobs}
    context: list[dict[str, Any]] = []

    if article_type == "top_games":
        order = ("team_trend", "player_line", "matchup", "news", "outlook_signal")
    elif sport == "nfl":
        order = ("news", "team_trend", "matchup", "player_line", "outlook_signal")
    else:
        order = ("team_trend", "news", "matchup", "player_line", "outlook_signal")

    for blob_type in order:
        items = [b for b in grouped.get(blob_type, []) if _is_clean_for_sport(b, sport)]
        items = sorted(items, key=_score, reverse=True)
        for blob in items:
            blob_id = str(blob.get("blob_id"))
            if blob_id in seen:
                continue
            context.append(blob)
            seen.add(blob_id)
    if sport in PRIMARY_SPORTS:
        return context[:12]
    return context[:10]


def _build_counterpoints(article_type: str, grouped: dict[str, list[dict[str, Any]]], primary_blobs: list[dict[str, Any]]) -> list[str]:
    seen = {str(blob.get("blob_id")) for blob in primary_blobs}
    candidates = grouped.get("news" if article_type == "top_games" else "game_result", [])
    out: list[str] = []
    for blob in sorted(candidates, key=_score, reverse=True):
        if str(blob.get("blob_id")) in seen:
            continue
        sport = str(blob.get("sport", "")).strip()
        if sport and not _is_clean_for_sport(blob, sport):
            continue
        tf = _typed(blob)
        text = str(tf.get("headline") or tf.get("title") or "").strip()
        if not text:
            continue
        out.append(text)
        if len(out) >= 3:
            break
    return out


def build_storysets(
    scored_blobs: list[dict[str, Any]],
    allocations: dict[str, Any],
    recent_thesis_cache: list[str] | None = None,
) -> list[dict[str, Any]]:
    recent_thesis_cache = recent_thesis_cache or []
    by_sport: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for blob in scored_blobs:
        sport = str(blob.get("sport", "")).strip()
        if sport:
            by_sport[sport].append(blob)

    allocation_map = {str(item.get("sport", "")): item for item in allocations.get("allocations", [])}
    storysets: list[dict[str, Any]] = []
    for sport in SPORT_ORDER:
        pool = by_sport.get(sport, [])
        if not pool:
            continue

        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for blob in pool:
            grouped[str(blob.get("blob_type", ""))].append(blob)

        article_type = _article_type_for_sport(sport, grouped)
        if not article_type:
            continue

        primary_blobs = _build_primary_blobs(sport, article_type, grouped)
        minimum_primary_count = _minimum_primary_count(
            sport,
            article_type,
            grouped,
            allocation_map.get(sport, {}),
        )
        if len(primary_blobs) < minimum_primary_count:
            continue
        context_blobs = _build_context_blobs(sport, article_type, grouped, primary_blobs)
        if len(context_blobs) < 1:
            continue

        season_phase = str(((primary_blobs[0].get("labels") or {}).get("season_phase") or "OFFSEASON"))
        thesis = build_thesis(
            scored_blobs=scored_blobs,
            sport=sport,
            article_type=article_type,
            season_phase=season_phase,
            run_date=str(primary_blobs[0].get("event_time_et", ""))[:10] or "",
            recent_thesis_cache=recent_thesis_cache,
            primary_blobs=primary_blobs,
            context_blobs=context_blobs,
        )
        if not thesis:
            continue

        selected = primary_blobs + context_blobs
        score = max((_score(blob) for blob in primary_blobs), default=0.0)
        storysets.append(
            {
                "story_set_id": f"{sport}-{article_type}-{primary_blobs[0].get('blob_id')}",
                "sport": sport,
                "article_type": article_type,
                "season_phase": season_phase,
                "coverage_allocation": allocation_map.get(sport, {}),
                "central_thesis": thesis["central_thesis"],
                "thesis_type": thesis["thesis_type"],
                "thesis_signals": thesis["thesis_signals"],
                "thesis_confidence": thesis["thesis_confidence"],
                "primary_blobs": [b.get("blob_id") for b in primary_blobs],
                "context_blobs": [b.get("blob_id") for b in context_blobs],
                "counterpoints": _build_counterpoints(article_type, grouped, primary_blobs),
                "supporting_stats": [b.get("blob_id") for b in sorted(grouped.get("player_line", []), key=_score, reverse=True)[:8]],
                "seo_targets": _seo_targets(selected, sport, thesis["central_thesis"]),
                "story_score": round(score, 4),
            }
        )

    storysets.sort(key=lambda x: float(x.get("story_score", 0.0)), reverse=True)
    return storysets
