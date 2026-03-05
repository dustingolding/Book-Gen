from __future__ import annotations

import hashlib
import re
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from app.services.coverage import determine_phase, load_yaml

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

GAME_NEWS_RE = re.compile(
    r"\b(highlights?|recap|preview|box score|final score|beat\s|defeat(?:ed|s)?|will face|face(?:s|d)?|will play|plays|odds|best bets|prediction|pick[s]?)\b",
    re.IGNORECASE,
)


def et_window(run_date: str) -> tuple[datetime, datetime]:
    d = date.fromisoformat(run_date)
    start = datetime.combine(d, time.min, tzinfo=ET)
    end = start + timedelta(days=1)
    return start, end


def parse_event_time_et(metrics: dict[str, Any], run_date: str) -> str:
    when = metrics.get("scheduled_time_utc") or metrics.get("game_time_utc")
    if when:
        try:
            dt = datetime.fromisoformat(str(when).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt.astimezone(ET).isoformat()
        except Exception:
            pass
    d = date.fromisoformat(run_date)
    return datetime.combine(d, time(hour=12), tzinfo=ET).isoformat()


def phase_for_sport(sport: str, on_date: date) -> str:
    seasonality = load_yaml("config/seasonality.yaml")
    return determine_phase(sport, seasonality, on_date)


def make_blob_id(seed: str) -> str:
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]


def infer_importance(score: float) -> str:
    if score >= 0.75:
        return "high"
    if score >= 0.45:
        return "medium"
    return "low"


def audience_weight_for_sport(sport: str) -> str:
    if sport in {"nfl", "college_football", "college_basketball", "nba"}:
        return "core"
    if sport in {"nhl", "mlb", "womens_college_basketball"}:
        return "secondary"
    return "niche"


def classify_news_category(title: str, summary: str) -> str:
    text = f"{title} {summary}".lower()
    if "combine" in text or "pro day" in text:
        return "combine"
    if "draft" in text:
        return "draft"
    if "injury" in text:
        return "injury"
    if "trade" in text:
        return "trade"
    if "free agent" in text or "free agency" in text or "contract" in text or "sign" in text:
        return "free_agency"
    if "coach" in text or "coaching" in text or "fired" in text or "hired" in text:
        return "coaching"
    if "standings" in text or "seed" in text or "playoff" in text:
        return "standings"
    if "suspend" in text or "discipline" in text:
        return "discipline"
    return "other"


def is_non_game_news(row: dict[str, Any]) -> bool:
    title = str(row.get("title", ""))
    summary = str(row.get("summary", ""))
    if GAME_NEWS_RE.search(title) or GAME_NEWS_RE.search(summary):
        return False
    return True
