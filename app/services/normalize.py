import logging

from app.db import delete_normalized_events, fetch_ingest_rows, upsert_normalized_event
from app.storage import ObjectStore

logger = logging.getLogger(__name__)


def _is_example_source(url: str) -> bool:
    u = (url or "").lower()
    return ".example/" in u or ".example." in u


def _normalize_sports(run_date: str, payload: dict) -> list[dict]:
    events = []
    for g in payload.get("scores", []):
        margin = abs(g["home_score"] - g["away_score"])
        events.append(
            {
                "event_id": g["id"],
                "run_date": run_date,
                "league": g["league"],
                "event_type": "score",
                "title": f"{g['away_team']} at {g['home_team']}",
                "summary": f"Final score: {g['home_team']} {g['home_score']} - {g['away_team']} {g['away_score']}",
                "entities": [g["home_team"], g["away_team"], g["league"]],
                "metrics": {
                    "home_team": g.get("home_team", ""),
                    "away_team": g.get("away_team", ""),
                    "home_score": g["home_score"],
                    "away_score": g["away_score"],
                    "home_record": g.get("home_record", ""),
                    "away_record": g.get("away_record", ""),
                    "home_rank": g.get("home_rank"),
                    "away_rank": g.get("away_rank"),
                    "score_margin": margin,
                    "status": g.get("status", ""),
                    "game_date": g.get("game_date", ""),
                },
                "citations": [g["source_url"]],
            }
        )

    for m in payload.get("upcoming", []):
        events.append(
            {
                "event_id": m["id"],
                "run_date": run_date,
                "league": m["league"],
                "event_type": "upcoming",
                "title": f"Upcoming: {m['away_team']} at {m['home_team']}",
                "summary": f"Scheduled for {m['scheduled_time_utc']} UTC.",
                "entities": [m["home_team"], m["away_team"], m["league"]],
                "metrics": {
                    "home_team": m.get("home_team", ""),
                    "away_team": m.get("away_team", ""),
                    "scheduled_time_utc": m["scheduled_time_utc"],
                    "home_record": m.get("home_record", ""),
                    "away_record": m.get("away_record", ""),
                    "home_rank": m.get("home_rank"),
                    "away_rank": m.get("away_rank"),
                },
                "citations": [m["source_url"]],
            }
        )
    return events


def _normalize_news(run_date: str, payload: dict) -> list[dict]:
    def _infer_news_league(title: str, summary: str, source_url: str) -> str:
        text = f"{title} {summary} {source_url}".lower()
        if any(k in text for k in (" nfl ", "/nfl/", "combine", "pro day", "franchise tag", "draft")):
            return "NFL"
        if any(k in text for k in (" wnba ", "/wnba/", "commissioner's cup")):
            return "WNBA"
        if any(k in text for k in (" nba ", "/nba/", "western conference", "eastern conference")):
            return "NBA"
        if any(k in text for k in (" mlb ", "/mlb/", "world series", "opening day", "spring training")):
            return "MLB"
        if any(k in text for k in (" nhl ", "/nhl/", "stanley cup", "hockey")):
            return "NHL"
        if any(k in text for k in ("womens college basketball", "women's college basketball", "ncaaw", "/womens-college-basketball/")):
            return "NCAAW"
        if any(k in text for k in ("college basketball", "march madness", "ncaa tournament", "final four", "/mens-college-basketball/", "ncaam")):
            return "NCAAM"
        if any(k in text for k in ("college football", "cfp", "heisman", "bowl")):
            return "NCAAF"
        if any(k in text for k in ("olympic", "fifa", "world cup", "premier league", "champions league")):
            return "INTERNATIONAL"
        return "MULTI"

    events = []
    for h in payload.get("headlines", []):
        title = str(h.get("title", ""))
        summary = str(h.get("summary", ""))
        source_url = str(h.get("source_url", ""))
        events.append(
            {
                "event_id": h["id"],
                "run_date": run_date,
                "league": _infer_news_league(title, summary, source_url),
                "event_type": "headline",
                "title": title,
                "summary": summary,
                "entities": ["sports"],
                "metrics": {"published_at": h["published_at"]},
                "citations": [source_url],
            }
        )
    return events


def run(run_date: str) -> dict:
    rows = fetch_ingest_rows(run_date)
    # Avoid stale cross-run contamination for same run_date.
    delete_normalized_events(run_date)
    normalized = []
    for row in rows:
        payload = row["payload"]
        if row["source"] == "sports":
            normalized.extend(_normalize_sports(run_date, payload))
        if row["source"] == "news":
            normalized.extend(_normalize_news(run_date, payload))

    cleaned = []
    for event in normalized:
        citations = [c for c in event.get("citations", []) if isinstance(c, str) and not _is_example_source(c)]
        if not citations:
            # Drop synthetic/placeholder events without real citations.
            continue
        event["citations"] = citations
        cleaned.append(event)

    for event in cleaned:
        upsert_normalized_event(event)

    store = ObjectStore()
    store.put_json(f"normalized/{run_date}/events.json", {"run_date": run_date, "events": cleaned})

    logger.info("normalize_complete", extra={"run_date": run_date, "events": len(cleaned)})
    return {"events": len(cleaned)}
