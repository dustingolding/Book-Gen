from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import re
from typing import Any, Callable

from app.clients.sports_api import SportsClient
from app.services.providers.common import (
    audience_weight_for_sport,
    classify_news_category,
    infer_importance,
    is_non_game_news,
    make_blob_id,
    parse_event_time_et,
    phase_for_sport,
)


@dataclass
class ProviderConfig:
    sport: str
    league_match: Callable[[str], bool]


class BaseSportProvider:
    def __init__(self, cfg: ProviderConfig, sports_client: SportsClient | None = None) -> None:
        self.cfg = cfg
        self.sports_client = sports_client or SportsClient()

    def accepts(self, league: str) -> bool:
        return self.cfg.league_match(str(league or ""))

    def build(self, run_date: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        d = date.fromisoformat(run_date)
        phase = phase_for_sport(self.cfg.sport, d)
        out: list[dict[str, Any]] = []
        for row in rows:
            league = str(row.get("league", ""))
            event_type = str(row.get("event_type", "")).lower()
            if event_type == "score":
                if not self.accepts(league):
                    continue
                blob = self._score_blob(row, run_date, phase)
                if blob:
                    out.append(blob)
                    player_blobs = self._player_line_blobs_from_score(row, run_date, phase)
                    out.extend(player_blobs)
            elif event_type == "upcoming":
                if not self.accepts(league):
                    continue
                blob = self._matchup_blob(row, run_date, phase)
                if blob:
                    out.append(blob)
            elif event_type == "headline":
                # For explicitly classified rows, require strict league ownership.
                # Keyword fallback is allowed only for unknown/multi-tagged rows.
                if league and league.upper().strip() not in {"MULTI", "UNKNOWN"}:
                    if not self.accepts(league):
                        continue
                elif not self._headline_matches_provider(row):
                    continue
                blob = self._news_blob(row, run_date, phase)
                if blob:
                    out.append(blob)

        accepted_rows = [r for r in rows if self.accepts(str(r.get("league", "")))]
        out.extend(self._team_trend_blobs(run_date, phase, accepted_rows))
        out.extend(self._outlook_signal_blobs(run_date, phase, accepted_rows))
        return out

    def _common_labels(self, category: str, row_score: float, phase: str) -> dict[str, Any]:
        return {
            "season_phase": phase,
            "category": category,
            "importance": infer_importance(row_score),
            "audience_weight": audience_weight_for_sport(self.cfg.sport),
        }

    def _base_blob(self, row: dict[str, Any], run_date: str, phase: str, blob_type: str, category: str) -> dict[str, Any]:
        metrics = row.get("metrics") or {}
        title = str(row.get("title", "")).strip()
        summary = str(row.get("summary", "")).strip()
        event_id = str(row.get("event_id", "")).strip()
        source_ids = [c for c in (row.get("citations") or []) if isinstance(c, str) and c.strip()]
        blob_id = make_blob_id(f"{self.cfg.sport}|{blob_type}|{event_id}|{title}")
        return {
            "blob_id": blob_id,
            "sport": self.cfg.sport,
            "league": str(row.get("league", "")),
            "blob_type": blob_type,
            "event_time_et": parse_event_time_et(metrics, run_date),
            "window": "season_to_date",
            "entities": {
                "game_id": event_id or None,
                "team_ids": [str(metrics.get("away_team", "")).strip(), str(metrics.get("home_team", "")).strip()],
                "player_ids": [],
            },
            "facts": {
                "typed_fields": {
                    "title": title,
                    "summary": summary,
                }
            },
            "labels": self._common_labels(category=category, row_score=float(row.get("score", 0.0) or 0.0), phase=phase),
            "scoring": {
                "base_weight": 0.0,
                "phase_multiplier": 0.0,
                "recency_multiplier": 0.0,
                "impact_multiplier": 0.0,
                "uniqueness_multiplier": 0.0,
                "blob_type_multiplier": 0.0,
                "final_priority_score": 0.0,
            },
            "provenance": {
                "source_ids": source_ids[:8] if source_ids else [f"event:{event_id or title}"],
                "retrieved_at_et": parse_event_time_et(metrics, run_date),
            },
        }

    def _score_blob(self, row: dict[str, Any], run_date: str, phase: str) -> dict[str, Any] | None:
        metrics = row.get("metrics") or {}
        if str(metrics.get("status", "")).upper() != "FINAL":
            return None
        blob = self._base_blob(row, run_date, phase, blob_type="game_result", category="scores")
        away = str(metrics.get("away_team", "")).strip()
        home = str(metrics.get("home_team", "")).strip()
        away_score = int(metrics.get("away_score", 0) or 0)
        home_score = int(metrics.get("home_score", 0) or 0)
        margin = abs(home_score - away_score)
        winner = home if home_score >= away_score else away
        loser = away if winner == home else home
        blob["window"] = "yesterday"
        blob["facts"]["typed_fields"].update(
            {
                "status": "FINAL",
                "away_team": away,
                "home_team": home,
                "away_rank": metrics.get("away_rank"),
                "home_rank": metrics.get("home_rank"),
                "away_score": away_score,
                "home_score": home_score,
                "winner": winner,
                "loser": loser,
                "margin": margin,
                "total_points": away_score + home_score,
                "playoff_implication": phase in {"POSTSEASON", "CHAMPIONSHIP_WINDOW"},
            }
        )
        return blob

    def _matchup_blob(self, row: dict[str, Any], run_date: str, phase: str) -> dict[str, Any] | None:
        blob = self._base_blob(row, run_date, phase, blob_type="matchup", category="preview")
        blob["window"] = "today"
        metrics = row.get("metrics") or {}
        blob["facts"]["typed_fields"].update(
            {
                "title": str(row.get("title", "")).strip(),
                "away_team": str(metrics.get("away_team", "")).strip(),
                "home_team": str(metrics.get("home_team", "")).strip(),
                "scheduled_time_utc": str(metrics.get("scheduled_time_utc", "")).strip(),
                "away_record": str(metrics.get("away_record", "")).strip(),
                "home_record": str(metrics.get("home_record", "")).strip(),
                "away_rank": metrics.get("away_rank"),
                "home_rank": metrics.get("home_rank"),
                "playoff_implication": phase in {"POSTSEASON", "CHAMPIONSHIP_WINDOW"},
                "matchup_context_quality": "pending_standings_enrichment",
            }
        )
        return blob

    def _news_blob(self, row: dict[str, Any], run_date: str, phase: str) -> dict[str, Any] | None:
        if not is_non_game_news(row):
            return None
        title = str(row.get("title", ""))
        summary = str(row.get("summary", ""))
        source_url = str(((row.get("citations") or [""])[0] or "")).lower()
        # Hard block preview/recap/betting content from major-news pipeline.
        if any(token in source_url for token in ("/preview?", "/recap?", "actionnetwork.com")):
            return None
        text = f"{title} {summary}".lower()
        if any(token in text for token in ("best bets", "prediction", "odds")):
            return None
        cat = classify_news_category(title, summary)
        if cat == "other" and float(row.get("score", 0.0) or 0.0) < 0.6:
            return None
        blob = self._base_blob(row, run_date, phase, blob_type="news", category=cat)
        blob["window"] = "season_to_date"
        severity = "high" if float(row.get("score", 0.0) or 0.0) >= 0.7 else "medium"
        fact_points = self._extract_fact_points(summary)
        blob["facts"]["typed_fields"].update(
            {
                "headline": title,
                "severity": severity,
                "fact_points": fact_points[:6],
                "summary_word_count": len(summary.split()),
            }
        )
        return blob

    def _player_line_blobs_from_score(self, row: dict[str, Any], run_date: str, phase: str) -> list[dict[str, Any]]:
        event_id = str(row.get("event_id", "")).strip()
        league = str(row.get("league", "")).strip()
        if not event_id:
            return []
        lines = self.sports_client.fetch_top_performer_lines(league=league, event_id=event_id, limit=3)
        out: list[dict[str, Any]] = []
        for ln in lines:
            blob = self._base_blob(row, run_date, phase, blob_type="player_line", category="scores")
            blob["window"] = "yesterday"
            line = str(ln).strip()
            parsed = self._parse_player_line(line)
            blob["facts"]["typed_fields"].update(
                {
                    "player_line": line,
                    "player_name": parsed.get("name", ""),
                    "stats": parsed.get("stats", {}),
                }
            )
            out.append(blob)
        return out

    def _team_trend_blobs(self, run_date: str, phase: str, rows: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        team_filter = self._extract_team_filter(rows or [])
        standings = self.sports_client.fetch_team_trends_snapshot(self.cfg.sport, limit=30)
        standings_map: dict[str, dict[str, Any]] = {}
        for s in standings:
            name = str(s.get("name", "")).upper().strip()
            if name:
                standings_map[name] = s

        recent = self.sports_client.fetch_recent_team_form(
            self.cfg.sport,
            team_filter=team_filter or None,
            on_date=date.fromisoformat(run_date),
            days_back=120 if self.cfg.sport in {"college_basketball", "womens_college_basketball", "college_football"} else 60,
            max_games=10,
            limit=max(16, len(team_filter) + 4 if team_filter else 16),
        )
        trends: list[dict[str, Any]] = []
        if recent:
            for row in recent:
                name = str(row.get("name", "")).upper().strip()
                st = standings_map.get(name, {})
                rr = str(row.get("recent_record", "")).strip()
                rg = int(row.get("recent_games", 0) or 0)
                window_type = "last10" if rg >= 10 else "season_start_window"
                trends.append(
                    {
                        "name": name,
                        "record": str(st.get("record", "")).strip() or str(row.get("record", "")).strip() or rr or "N/A",
                        "rank": st.get("rank") or row.get("rank"),
                        "last10": str(row.get("last10", "")).strip(),
                        "recent_record": rr,
                        "recent_games": rg,
                        "window_type": window_type,
                        "streak": str(st.get("streak", "")).strip() or str(row.get("streak", "")).strip() or "N/A",
                        "point_diff": row.get("point_diff", st.get("point_diff", "N/A")),
                        "points_for": row.get("points_for", st.get("points_for", "N/A")),
                        "points_against": row.get("points_against", st.get("points_against", "N/A")),
                        "trend_note": str(row.get("trend_note", "")).strip(),
                    }
                )
        else:
            trends = standings[:16]

        out: list[dict[str, Any]] = []
        for t in trends:
            seed = f"{self.cfg.sport}|team_trend|{t.get('name','')}|{run_date}"
            out.append(
                {
                    "blob_id": make_blob_id(seed),
                    "sport": self.cfg.sport,
                    "league": self.cfg.sport.upper(),
                    "blob_type": "team_trend",
                    "event_time_et": datetime.now().astimezone().isoformat(),
                    "window": "last10",
                    "entities": {
                        "game_id": None,
                        "team_ids": [str(t.get("name", ""))],
                        "player_ids": [],
                    },
                    "facts": {"typed_fields": dict(t)},
                    "labels": self._common_labels("standings", 0.5, phase),
                    "scoring": {
                        "base_weight": 0.0,
                        "phase_multiplier": 0.0,
                        "recency_multiplier": 0.0,
                        "impact_multiplier": 0.0,
                        "uniqueness_multiplier": 0.0,
                        "blob_type_multiplier": 0.0,
                        "final_priority_score": 0.0,
                    },
                    "provenance": {
                        "source_ids": ["espn:standings"],
                        "retrieved_at_et": datetime.now().astimezone().isoformat(),
                    },
                }
            )
        return out

    def _outlook_signal_blobs(self, run_date: str, phase: str, rows: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        if phase not in {"OFFSEASON", "PRESEASON"}:
            return []
        seed = f"{self.cfg.sport}|outlook|{run_date}"
        return [
            {
                "blob_id": make_blob_id(seed),
                "sport": self.cfg.sport,
                "league": self.cfg.sport.upper(),
                "blob_type": "outlook_signal",
                "event_time_et": datetime.now().astimezone().isoformat(),
                "window": "offseason",
                "entities": {"game_id": None, "team_ids": [], "player_ids": []},
                "facts": {
                    "typed_fields": {
                        "phase": phase,
                        "signal": f"{self.cfg.sport} phase={phase}; prioritize confirmed transactions, injuries, standings movement, and award shifts.",
                    }
                },
                "labels": self._common_labels("other", 0.4, phase),
                "scoring": {
                    "base_weight": 0.0,
                    "phase_multiplier": 0.0,
                    "recency_multiplier": 0.0,
                    "impact_multiplier": 0.0,
                    "uniqueness_multiplier": 0.0,
                    "blob_type_multiplier": 0.0,
                    "final_priority_score": 0.0,
                },
                "provenance": {
                    "source_ids": ["phase:seasonality"],
                    "retrieved_at_et": datetime.now().astimezone().isoformat(),
                },
            }
        ]

    def _extract_team_filter(self, rows: list[dict[str, Any]]) -> set[str]:
        out: set[str] = set()
        for row in rows:
            metrics = row.get("metrics") or {}
            for key in ("away_team", "home_team"):
                token = str(metrics.get(key, "")).upper().strip()
                if token:
                    out.add(token)
            if len(out) >= 24:
                break
        return out

    @staticmethod
    def _extract_fact_points(summary: str) -> list[str]:
        text = str(summary or "").strip()
        if not text:
            return []
        sentence_bits = [
            b.strip(" -;,.")
            for b in re.split(r"(?<=[a-z0-9\)])\.\s+(?=[A-Z\"'])|;\s+", text)
            if b.strip()
        ]
        bits: list[str] = []
        for sentence in sentence_bits:
            bits.append(sentence)
            comma_clauses = [
                clause.strip(" -;,.")
                for clause in re.split(r",\s+", sentence)
                if clause.strip()
            ]
            if len(comma_clauses) <= 1:
                continue
            for clause in comma_clauses[1:]:
                low = clause.lower()
                if len(clause.split()) < 4:
                    continue
                subclauses = [
                    sub.strip(" -;,.")
                    for sub in re.split(r"(?<=[a-z0-9\)])\.\s+(?=[A-Z\"'])", clause)
                    if sub.strip()
                ]
                for sub in subclauses:
                    sub_low = sub.lower()
                    if len(sub.split()) < 4:
                        continue
                    if not (
                        re.search(r"\d", sub)
                        or any(
                            token in sub_low
                            for token in (
                                "trade",
                                "combine",
                                "draft",
                                "injury",
                                "contract",
                                "extension",
                                "signed",
                                "free agent",
                                "free agency",
                                "workout",
                                "measurable",
                                "survey",
                                "report card",
                                "deadline",
                                "lawsuit",
                                "suspension",
                                "playoff",
                                "seed",
                                "ranked",
                                "finished last",
                                "finished first",
                                "died",
                                "dies",
                            )
                        )
                    ):
                        continue
                    bits.append(sub)

        out: list[str] = []
        seen: set[str] = set()
        for b in bits:
            clean = re.sub(r"\s+", " ", b).strip(" -;,.")
            if len(clean.split()) < 4:
                continue
            norm = re.sub(r"[^a-z0-9 ]+", "", clean.lower()).strip()
            if not norm or norm in seen:
                continue
            seen.add(norm)
            out.append(clean)
        if not out and text:
            out.append(text)
        return out[:6]

    def _headline_matches_provider(self, row: dict[str, Any]) -> bool:
        text = f"{str(row.get('title', ''))} {str(row.get('summary', ''))}".lower()
        source_url = str(row.get("source_url", "")).lower()
        sport = self.cfg.sport
        if sport == "nfl":
            if any(token in source_url for token in ("/nfl/", "nfl.com/")):
                return True
            return any(k in text for k in (" nfl", "combine", "pro day", "free agency", "franchise tag", "quarterback", "super bowl", "draft"))
        if sport == "nba":
            if "/nba/" in source_url:
                return True
            return any(k in text for k in (" nba", "play-in", "western conference", "eastern conference", "all-star", "mvp", "dpoy"))
        if sport == "college_basketball":
            if any(token in source_url for token in ("/college-basketball/", "/mens-college-basketball/")):
                return True
            return any(k in text for k in ("ncaa", "march madness", "final four", "selection sunday", "ap top 25", "big ten", "sec", "big 12", "acc"))
        if sport == "womens_college_basketball":
            if any(token in source_url for token in ("/womens-college-basketball/", "/women's-basketball/", "/wbb/")):
                return True
            return any(k in text for k in ("women's basketball", "womens basketball", "wcbb", "ncaaw", "women's tournament", "wbb"))
        if sport == "nhl":
            if any(token in source_url for token in ("/nhl/", "nhl.com/")):
                return True
            return any(k in text for k in (" nhl", "stanley cup", "hockey", "trade deadline", "conn smythe", "hart trophy"))
        if sport == "mlb":
            if any(token in source_url for token in ("/mlb/", "mlb.com/")):
                return True
            return any(k in text for k in (" mlb", "spring training", "world series", "cy young", "home run", "opening day", "all-star game"))
        if sport == "wnba":
            if "/wnba/" in source_url:
                return True
            return any(k in text for k in (" wnba", "commissioner's cup", "all-wnba", "fiba break"))
        if sport == "college_football":
            if any(token in source_url for token in ("/college-football/", "/ncaaf/")):
                return True
            return any(k in text for k in ("college football", "cfp", "heisman", "bowl game", "sec football", "big ten football"))
        if sport == "major_international":
            if any(token in source_url for token in ("/olympics/", "/soccer/", "/world-cup/")):
                return True
            return any(k in text for k in ("olympic", "fifa", "world cup", "premier league", "champions league"))
        return False

    @staticmethod
    def _parse_player_line(line: str) -> dict[str, Any]:
        m = re.match(r"^(?P<name>.+?)\s-\s(?P<stats>.+)$", str(line or "").strip())
        if not m:
            return {"name": "", "stats": {}}
        stats: dict[str, int] = {}
        for token in m.group("stats").split(","):
            tm = re.search(r"(\d+)\s+([A-Z]{2,6})", token.strip().upper())
            if not tm:
                continue
            stats[tm.group(2)] = int(tm.group(1))
        return {"name": m.group("name").strip(), "stats": stats}
