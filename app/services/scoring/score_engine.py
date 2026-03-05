from __future__ import annotations

from collections import Counter
from datetime import datetime
from hashlib import sha1
from typing import Any
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

DEFAULT_SPORT_BASE = {
    "college_football": 100,
    "nfl": 90,
    "college_basketball": 80,
    "nba": 70,
    "womens_college_basketball": 55,
    "nhl": 55,
    "mlb": 55,
    "wnba": 45,
    "major_international": 35,
}

DEFAULT_PHASE_MULT = {
    "OFFSEASON": 0.25,
    "PRESEASON": 0.75,
    "REGULAR_SEASON": 1.0,
    "POSTSEASON": 1.6,
    "CHAMPIONSHIP_WINDOW": 2.0,
}

DEFAULT_BLOB_TYPE_MULT = {
    "game_result": 1.2,
    "matchup": 0.9,
    "news": 1.0,
    "team_trend": 0.8,
    "player_line": 0.9,
    "standings_snapshot": 1.0,
    "outlook_signal": 0.85,
}

CATEGORY_BONUS = {
    "injury": 1.15,
    "trade": 1.12,
    "coaching": 1.18,
    "discipline": 1.10,
    "controversy": 1.08,
    "milestone": 1.05,
    "record_watch": 1.05,
    "scores": 1.00,
    "preview": 1.00,
    "standings": 1.03,
    "combine": 1.15,
    "draft": 1.10,
    "free_agency": 1.08,
    "other": 1.00,
}


def _parse_iso(dt_str: str) -> datetime:
    dt = datetime.fromisoformat(str(dt_str))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ET)
    return dt.astimezone(ET)


def recency_multiplier(event_time_et: str, now_et: datetime, window: str) -> float:
    if window in ("yesterday", "today"):
        return 1.0
    age_hours = (now_et - _parse_iso(event_time_et)).total_seconds() / 3600.0
    if age_hours <= 24:
        return 1.0
    if age_hours <= 72:
        return 0.85
    if age_hours <= 168:
        return 0.65
    return 0.35


def impact_multiplier(blob: dict[str, Any]) -> float:
    fields = ((blob.get("facts") or {}).get("typed_fields") or {})
    cat = str(((blob.get("labels") or {}).get("category") or "other")).lower()
    mult = CATEGORY_BONUS.get(cat, 1.0)

    margin = abs(float(fields.get("margin", 0.0) or 0.0))
    if margin >= 25:
        mult *= 1.20
    elif margin >= 15:
        mult *= 1.10

    playoff = bool(fields.get("playoff_implication") or fields.get("clinching_scenario"))
    if playoff:
        mult *= 1.18

    news_severity = str(fields.get("severity", "")).lower()
    if news_severity == "high":
        mult *= 1.15
    elif news_severity == "medium":
        mult *= 1.06

    return max(0.4, min(mult, 2.5))


def _fingerprint(blob: dict[str, Any]) -> str:
    tf = ((blob.get("facts") or {}).get("typed_fields") or {})
    entities = blob.get("entities") or {}
    raw = "|".join(
        [
            str(blob.get("sport", "")),
            str(blob.get("blob_type", "")),
            str(entities.get("game_id", "")),
            ",".join(str(x) for x in (entities.get("team_ids") or [])),
            str(tf.get("headline", "")).lower().strip(),
            str(tf.get("title", "")).lower().strip(),
        ]
    )
    return sha1(raw.encode("utf-8")).hexdigest()


def score_blobs(
    blobs: list[dict[str, Any]],
    sport_base: dict[str, float] | None = None,
    phase_mult: dict[str, float] | None = None,
    blob_type_mult: dict[str, float] | None = None,
    now_et: datetime | None = None,
) -> list[dict[str, Any]]:
    now_et = now_et or datetime.now(tz=ET)
    sport_base = sport_base or DEFAULT_SPORT_BASE
    phase_mult = phase_mult or DEFAULT_PHASE_MULT
    blob_type_mult = blob_type_mult or DEFAULT_BLOB_TYPE_MULT

    fps = [_fingerprint(b) for b in blobs]
    counts = Counter(fps)

    out: list[dict[str, Any]] = []
    for b, fp in zip(blobs, fps):
        sport = str(b.get("sport", "major_international"))
        labels = b.get("labels") or {}
        phase = str(labels.get("season_phase", "OFFSEASON"))
        btype = str(b.get("blob_type", "news"))
        window = str(b.get("window", "season_to_date"))
        event_time_et = str(b.get("event_time_et"))

        base_weight = float(sport_base.get(sport, 35.0))
        p_mult = float(phase_mult.get(phase, 1.0))
        r_mult = recency_multiplier(event_time_et, now_et, window)
        i_mult = impact_multiplier(b)
        bt_mult = float(blob_type_mult.get(btype, 1.0))
        uniq_mult = 1.0 / max(1.0, float(counts.get(fp, 1)))
        # soften duplicate penalty a bit
        uniq_mult = 0.8 + (0.2 * uniq_mult)

        final = base_weight * p_mult * r_mult * i_mult * bt_mult * uniq_mult
        scored = dict(b)
        scored["scoring"] = {
            "base_weight": round(base_weight, 3),
            "phase_multiplier": round(p_mult, 3),
            "recency_multiplier": round(r_mult, 3),
            "impact_multiplier": round(i_mult, 3),
            "uniqueness_multiplier": round(uniq_mult, 3),
            "blob_type_multiplier": round(bt_mult, 3),
            "final_priority_score": round(final, 6),
        }
        out.append(scored)

    out.sort(key=lambda x: float((x.get("scoring") or {}).get("final_priority_score", 0.0)), reverse=True)
    return out
