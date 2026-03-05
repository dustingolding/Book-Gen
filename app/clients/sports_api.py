from datetime import date, timedelta
import logging
import re
from typing import Any

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)

POWER4_TEAM_ABBREVS = {
    # ACC
    "BC", "CLEM", "DUKE", "FSU", "GT", "LOU", "MIA", "UNC", "NCST", "ND", "PITT", "SMU", "STAN", "CAL", "SYR", "UVA", "VT", "WAKE",
    # SEC
    "ALA", "ARK", "AUB", "UGA", "UK", "LSU", "MISS", "MSST", "MIZ", "OU", "SC", "TENN", "TEX", "TA&M", "TAMU", "VAN", "FLA",
    # Big Ten
    "ILL", "IND", "IOWA", "UMD", "MICH", "MSU", "MINN", "NEB", "NW", "OSU", "ORE", "PSU", "PUR", "RUTG", "UCLA", "USC", "WASH", "WIS",
    # Big 12
    "ARIZ", "ASU", "BAY", "BYU", "CIN", "COLO", "HOU", "ISU", "KU", "KSU", "OKST", "TCU", "TTU", "UCF", "UTAH", "WVU",
}


class SportsClient:
    def __init__(self) -> None:
        cfg = get_settings()
        self.api_url = (cfg.sports_api_url or "").rstrip("/")
        self.api_key = cfg.sports_api_key or ""
        self.sportsdb_api_url = (cfg.sportsdb_api_url or "").rstrip("/")
        self.sportsdb_api_key = cfg.sportsdb_api_key or ""
        self.espn_site_api_url = (cfg.espn_site_api_url or "").rstrip("/")
        self.espn_sports = [s.strip() for s in (cfg.espn_sports or "").split(",") if s.strip()]
        self.allow_synthetic_fallback = cfg.allow_synthetic_fallback
        self.include_preseason_scores = cfg.include_preseason_scores
        self.include_spring_training_scores = cfg.include_spring_training_scores

    def _should_use_generic(self) -> bool:
        if not self.api_url:
            return False
        # If generic URL is actually TheSportsDB base, skip generic /scores,/upcoming.
        return "thesportsdb.com" not in self.api_url

    def _fallback_scores(self, run_date: str) -> list[dict]:
        d = date.fromisoformat(run_date)
        return [
            {
                "id": f"nba-{d.isoformat()}-lal-gsw",
                "league": "NBA",
                "home_team": "LAL",
                "away_team": "GSW",
                "home_score": 112,
                "away_score": 104,
                "status": "final",
                "game_date": d.isoformat(),
                "source_url": "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
            },
            {
                "id": f"nfl-{d.isoformat()}-phi-dal",
                "league": "NFL",
                "home_team": "PHI",
                "away_team": "DAL",
                "home_score": 24,
                "away_score": 17,
                "status": "final",
                "game_date": d.isoformat(),
                "source_url": "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard",
            },
            {
                "id": f"mlb-{d.isoformat()}-nyy-bos",
                "league": "MLB",
                "home_team": "NYY",
                "away_team": "BOS",
                "home_score": 6,
                "away_score": 5,
                "status": "final",
                "game_date": d.isoformat(),
                "source_url": "https://www.thesportsdb.com/api/v1/json/3/eventsday.php",
            },
        ]

    def _fallback_upcoming(self, run_date: str) -> list[dict]:
        d = date.fromisoformat(run_date)
        return [
            {
                "id": f"nba-next-{d.isoformat()}-mia-nyk",
                "league": "NBA",
                "home_team": "MIA",
                "away_team": "NYK",
                "scheduled_time_utc": f"{d.isoformat()}T23:30:00Z",
                "source_url": "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
            },
            {
                "id": f"nhl-next-{d.isoformat()}-bos-tbl",
                "league": "NHL",
                "home_team": "BOS",
                "away_team": "TBL",
                "scheduled_time_utc": f"{d.isoformat()}T01:00:00Z",
                "source_url": "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard",
            },
        ]

    def _headers(self) -> dict:
        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
            headers["x-api-key"] = self.api_key
        return headers

    @staticmethod
    def _safe_int(v: Any) -> int:
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _looks_synthetic(row: dict) -> bool:
        src = str(row.get("source_url") or "")
        return "api.sports.example" in src or "sportsnews.example" in src or ".example/" in src or ".example." in src

    @staticmethod
    def _is_preseason_or_exhibition(row: dict) -> bool:
        league = str(row.get("league") or "").lower()
        season_name = str(row.get("season_name") or "").lower()
        season_label = str(row.get("season_label") or "").lower()
        season_type = row.get("season_type")
        text = " ".join([league, season_name, season_label]).lower()

        if any(term in text for term in ("preseason", "pre-season", "exhibition")):
            return True
        try:
            if int(season_type) == 1:
                return True
        except Exception:
            pass
        return False

    @staticmethod
    def _is_spring_training(row: dict) -> bool:
        league = str(row.get("league") or "").lower()
        season_name = str(row.get("season_name") or "").lower()
        season_label = str(row.get("season_label") or "").lower()
        text = " ".join([league, season_name, season_label]).lower()
        return "spring training" in text

    def _dedupe(self, rows: list[dict]) -> list[dict]:
        out: list[dict] = []
        seen: set[str] = set()
        for row in rows:
            if SportsClient._looks_synthetic(row):
                continue
            if not self.include_spring_training_scores and SportsClient._is_spring_training(row):
                continue
            if not self.include_preseason_scores and SportsClient._is_preseason_or_exhibition(row):
                continue
            row_id = str(row.get("id", "")).strip()
            if not row_id or row_id in seen:
                continue
            seen.add(row_id)
            out.append(row)
        return out

    def _fetch_generic_scores(self, run_date: str) -> list[dict]:
        if not self.api_url:
            return []
        with httpx.Client(timeout=20.0) as client:
            resp = client.get(
                f"{self.api_url}/scores",
                params={"date": run_date},
                headers=self._headers(),
            )
            resp.raise_for_status()
            payload = resp.json()
        rows = payload.get("scores", payload)
        if not isinstance(rows, list):
            raise ValueError("sports scores payload must be list-like")
        return rows

    def _fetch_generic_upcoming(self, run_date: str) -> list[dict]:
        if not self.api_url:
            return []
        with httpx.Client(timeout=20.0) as client:
            resp = client.get(
                f"{self.api_url}/upcoming",
                params={"date": run_date},
                headers=self._headers(),
            )
            resp.raise_for_status()
            payload = resp.json()
        rows = payload.get("upcoming", payload)
        if not isinstance(rows, list):
            raise ValueError("sports upcoming payload must be list-like")
        return rows

    def _fetch_espn(self, run_date: str, completed: bool) -> list[dict]:
        if not self.espn_site_api_url:
            return []
        rows: list[dict] = []
        dates = run_date.replace("-", "")
        with httpx.Client(timeout=20.0) as client:
            for sport_path in self.espn_sports:
                url = f"{self.espn_site_api_url}/apis/site/v2/sports/{sport_path}/scoreboard"
                try:
                    resp = client.get(url, params={"dates": dates})
                    resp.raise_for_status()
                    payload = resp.json()
                except Exception as exc:
                    logger.warning("espn_scoreboard_fetch_failed", extra={"sport": sport_path, "error": str(exc)})
                    continue

                league = str(payload.get("leagues", [{}])[0].get("abbreviation") or sport_path.split("/")[-1]).upper()
                for event in payload.get("events", []):
                    status_type = (
                        event.get("status", {})
                        .get("type", {})
                        .get("state", "")
                        .lower()
                    )
                    is_completed = status_type == "post"
                    if completed != is_completed:
                        continue

                    competitions = event.get("competitions", [])
                    if not competitions:
                        continue
                    comp = competitions[0]
                    competitors = comp.get("competitors", [])
                    home = next((c for c in competitors if c.get("homeAway") == "home"), None)
                    away = next((c for c in competitors if c.get("homeAway") == "away"), None)
                    if not home or not away:
                        continue

                    def _overall_record(competitor: dict[str, Any]) -> str:
                        for record in competitor.get("records") or []:
                            if str(record.get("type") or "").strip().lower() == "total":
                                return str(record.get("summary") or "").strip()
                        return ""

                    def _rank(competitor: dict[str, Any]) -> int | None:
                        try:
                            current = ((competitor.get("curatedRank") or {}).get("current"))
                            if current is None:
                                return None
                            value = int(current)
                            return value if 0 < value <= 25 else None
                        except Exception:
                            return None

                    event_id = f"espn-{event.get('id')}"
                    if completed:
                        rows.append(
                            {
                                "id": event_id,
                                "league": league,
                                "home_team": home.get("team", {}).get("abbreviation", "HOME"),
                                "away_team": away.get("team", {}).get("abbreviation", "AWAY"),
                                "home_score": self._safe_int(home.get("score")),
                                "away_score": self._safe_int(away.get("score")),
                                "status": "final",
                                "game_date": run_date,
                                "home_record": _overall_record(home),
                                "away_record": _overall_record(away),
                                "home_rank": _rank(home),
                                "away_rank": _rank(away),
                                "season_type": event.get("season", {}).get("type"),
                                "season_name": event.get("season", {}).get("slug"),
                                "season_label": comp.get("type", {}).get("abbreviation"),
                                "source_url": url,
                            }
                        )
                    else:
                        rows.append(
                            {
                                "id": event_id,
                                "league": league,
                                "home_team": home.get("team", {}).get("abbreviation", "HOME"),
                                "away_team": away.get("team", {}).get("abbreviation", "AWAY"),
                                "scheduled_time_utc": event.get("date"),
                                "home_record": _overall_record(home),
                                "away_record": _overall_record(away),
                                "home_rank": _rank(home),
                                "away_rank": _rank(away),
                                "season_type": event.get("season", {}).get("type"),
                                "season_name": event.get("season", {}).get("slug"),
                                "season_label": comp.get("type", {}).get("abbreviation"),
                                "source_url": url,
                            }
                        )
        return rows

    def _fetch_sportsdb_scores(self, run_date: str) -> list[dict]:
        if not self.sportsdb_api_url:
            return []

        rows: list[dict] = []
        sports = ["Soccer", "Basketball", "American Football", "Baseball", "Ice Hockey"]
        base_url = self.sportsdb_api_url.rstrip("/")
        # Normalize to: https://www.thesportsdb.com/api/v1/json/<key>
        if self.sportsdb_api_key:
            if "/api/v1/json" in base_url:
                tail = base_url.split("/api/v1/json", 1)[1].strip("/")
                if tail != self.sportsdb_api_key:
                    base_url = f"{base_url.split('/api/v1/json', 1)[0]}/api/v1/json/{self.sportsdb_api_key}"
            else:
                base_url = f"{base_url}/api/v1/json/{self.sportsdb_api_key}"
        elif "/api/v1/json" not in base_url:
            # Public test key fallback keeps the integration useful out-of-box.
            base_url = f"{base_url}/api/v1/json/3"

        with httpx.Client(timeout=20.0) as client:
            for sport in sports:
                try:
                    resp = client.get(
                        f"{base_url}/eventsday.php",
                        params={"d": run_date, "s": sport},
                    )
                    resp.raise_for_status()
                    payload = resp.json()
                except Exception as exc:
                    logger.info("sportsdb_fetch_failed", extra={"sport": sport, "error": str(exc)})
                    continue

                for event in payload.get("events") or []:
                    event_id = str(event.get("idEvent") or "").strip()
                    if not event_id:
                        continue
                    rows.append(
                        {
                            "id": f"sportsdb-{event_id}",
                            "league": (event.get("strLeague") or sport).upper(),
                            "home_team": event.get("strHomeTeam", "HOME"),
                            "away_team": event.get("strAwayTeam", "AWAY"),
                            "home_score": self._safe_int(event.get("intHomeScore")),
                            "away_score": self._safe_int(event.get("intAwayScore")),
                            "status": "final",
                            "game_date": run_date,
                            "season_name": event.get("strSeason") or "",
                            "season_label": event.get("strEvent") or "",
                            "source_url": f"{base_url}/eventsday.php",
                        }
                    )
        return rows

    @staticmethod
    def _league_to_espn_sport_path(league: str) -> str | None:
        raw = (league or "").upper().strip()
        if raw == "NBA":
            return "basketball/nba"
        if raw in {"NCAA", "NCAAM", "NCAAB"}:
            return "basketball/mens-college-basketball"
        if raw in {"NCAAW", "WNCAA", "WNCAAB"}:
            return "basketball/womens-college-basketball"
        if raw == "WNBA":
            return "basketball/wnba"
        if raw == "NFL":
            return "football/nfl"
        if raw == "MLB":
            return "baseball/mlb"
        return None

    @staticmethod
    def _sport_to_espn_path(sport: str) -> str | None:
        raw = (sport or "").strip().lower()
        mapping = {
            "nba": "basketball/nba",
            "wnba": "basketball/wnba",
            "nfl": "football/nfl",
            "mlb": "baseball/mlb",
            "nhl": "hockey/nhl",
            "college_basketball": "basketball/mens-college-basketball",
            "womens_college_basketball": "basketball/womens-college-basketball",
            "college_football": "football/college-football",
        }
        return mapping.get(raw)

    @staticmethod
    def _fmt_player_line(name: str, stats: list[str]) -> str | None:
        clean_name = (name or "").strip()
        clean_stats = [str(s).strip() for s in stats if str(s).strip()]
        if not clean_name or not clean_stats:
            return None
        return f"{clean_name} - {', '.join(clean_stats[:3])}"

    @staticmethod
    def _to_stat_abbrev(label: str) -> str:
        raw = (label or "").strip().lower()
        mapping = {
            "points": "PTS",
            "assists": "AST",
            "rebounds": "REB",
            "steals": "STL",
            "blocks": "BLK",
            "turnovers": "TOV",
            "hits": "H",
            "home runs": "HR",
            "runs batted in": "RBI",
            "strikeouts": "SO",
            "passing yards": "PASS YDS",
            "rushing yards": "RUSH YDS",
            "receiving yards": "REC YDS",
            "touchdowns": "TD",
        }
        return mapping.get(raw, raw.upper())

    @staticmethod
    def _to_numeric_value(display: str) -> float:
        m = re.search(r"-?\d+(?:\.\d+)?", str(display or ""))
        if not m:
            return 0.0
        try:
            return float(m.group(0))
        except ValueError:
            return 0.0

    @staticmethod
    def _norm_stat_key(label: str) -> str:
        raw = (label or "").upper().strip()
        mapping = {
            "PTS": "PTS",
            "POINTS": "PTS",
            "REB": "REB",
            "REBOUNDS": "REB",
            "AST": "AST",
            "ASSISTS": "AST",
            "STL": "STL",
            "STEALS": "STL",
            "BLK": "BLK",
            "BLOCKS": "BLK",
            "TO": "TOV",
            "TOV": "TOV",
            "TURNOVERS": "TOV",
            "MIN": "MIN",
            "+/-": "PLUS_MINUS",
            "PLUS_MINUS": "PLUS_MINUS",
        }
        return mapping.get(raw, raw)

    @classmethod
    def _extract_weighted_basketball_lines(cls, payload: dict[str, Any], limit: int) -> list[str]:
        boxscore = payload.get("boxscore") or {}
        candidates: list[tuple[float, str]] = []
        seen_names: set[str] = set()

        for team_block in boxscore.get("players", []) or []:
            for stat_block in team_block.get("statistics", []) or []:
                labels = [cls._norm_stat_key(x) for x in (stat_block.get("labels", []) or [])]
                for athlete_row in stat_block.get("athletes", []) or []:
                    athlete = athlete_row.get("athlete") or {}
                    name = (athlete.get("displayName") or athlete.get("shortName") or "").strip()
                    vals = athlete_row.get("stats", []) or []
                    if not name or not vals or name in seen_names:
                        continue

                    m: dict[str, float] = {}
                    for i, v in enumerate(vals):
                        if i >= len(labels):
                            continue
                        key = labels[i]
                        m[key] = cls._to_numeric_value(str(v))

                    pts = m.get("PTS", 0.0)
                    reb = m.get("REB", 0.0)
                    ast = m.get("AST", 0.0)
                    stl = m.get("STL", 0.0)
                    blk = m.get("BLK", 0.0)
                    tov = m.get("TOV", 0.0)
                    min_played = m.get("MIN", 0.0)
                    plus_minus = m.get("PLUS_MINUS", 0.0)

                    # Require real contribution so we don't surface low-impact stat crumbs.
                    if min_played < 10 and (pts + reb + ast) < 12:
                        continue

                    weighted = (
                        pts * 1.0
                        + reb * 0.9
                        + ast * 1.1
                        + stl * 1.5
                        + blk * 1.4
                        - tov * 0.7
                        + plus_minus * 0.06
                    )

                    parts = [f"{int(pts)} PTS", f"{int(reb)} REB", f"{int(ast)} AST"]
                    if stl >= 2:
                        parts.append(f"{int(stl)} STL")
                    if blk >= 2:
                        parts.append(f"{int(blk)} BLK")
                    line = cls._fmt_player_line(name, parts)
                    if line:
                        candidates.append((weighted, line))
                        seen_names.add(name)

        out: list[str] = []
        for _, line in sorted(candidates, key=lambda t: t[0], reverse=True):
            out.append(line)
            if len(out) >= limit:
                break
        return out

    def fetch_top_performer_lines(self, league: str, event_id: str, limit: int = 3) -> list[str]:
        if not self.espn_site_api_url:
            return []
        if not str(event_id).startswith("espn-"):
            return []

        espn_event_id = str(event_id).split("espn-", 1)[1].strip()
        sport_path = self._league_to_espn_sport_path(league)
        if not espn_event_id or not sport_path:
            return []

        url = f"{self.espn_site_api_url}/apis/site/v2/sports/{sport_path}/summary"
        try:
            with httpx.Client(timeout=20.0) as client:
                resp = client.get(url, params={"event": espn_event_id})
                resp.raise_for_status()
                payload = resp.json()
        except Exception as exc:
            logger.info("espn_summary_fetch_failed", extra={"event_id": event_id, "error": str(exc)})
            return []

        # Primary: weighted full stat-lines from boxscore rows.
        if sport_path.startswith("basketball/"):
            weighted = self._extract_weighted_basketball_lines(payload, limit=limit)
            if weighted:
                return weighted

        lines: list[str] = []
        seen: set[str] = set()

        # Preferred path: summary leaders.
        player_stats: dict[str, dict[str, str]] = {}
        player_score: dict[str, float] = {}
        stat_weight = {
            "PTS": 1.0,
            "AST": 0.8,
            "REB": 0.7,
            "STL": 0.5,
            "BLK": 0.5,
            "RBI": 0.6,
            "HR": 0.8,
            "SO": 0.5,
            "TD": 1.2,
            "PASS YDS": 0.02,
            "RUSH YDS": 0.02,
            "REC YDS": 0.02,
        }
        for group in payload.get("leaders", []) or []:
            for leader_bucket in group.get("leaders", []) or []:
                abbrev = self._to_stat_abbrev(leader_bucket.get("displayName") or leader_bucket.get("name") or "")
                for athlete_row in leader_bucket.get("leaders", []) or []:
                    athlete = athlete_row.get("athlete") or {}
                    name = athlete.get("displayName") or athlete.get("shortName") or ""
                    display_val = athlete_row.get("displayValue") or athlete_row.get("value")
                    stat_value = str(display_val or "").strip()
                    if not name or not abbrev or not stat_value:
                        continue
                    pstats = player_stats.setdefault(name, {})
                    pstats[abbrev] = stat_value
                    numeric = self._to_numeric_value(stat_value)
                    player_score[name] = player_score.get(name, 0.0) + (numeric * stat_weight.get(abbrev, 0.2))

        if player_stats:
            for name, _ in sorted(player_score.items(), key=lambda kv: kv[1], reverse=True):
                stat_order = ["PTS", "AST", "REB", "STL", "BLK", "TD", "PASS YDS", "RUSH YDS", "REC YDS", "HR", "RBI", "SO"]
                pstats = player_stats.get(name, {})
                stat_parts = [f"{pstats[s]} {s}" for s in stat_order if s in pstats]
                if not stat_parts:
                    continue
                line = self._fmt_player_line(name, stat_parts)
                if line and line not in seen:
                    seen.add(line)
                    lines.append(line)
                    if len(lines) >= limit:
                        return lines

        # Secondary fallback: generic boxscore extraction for non-basketball paths.
        boxscore = payload.get("boxscore") or {}
        for team_block in boxscore.get("players", []) or []:
            for stat_block in team_block.get("statistics", []) or []:
                labels = stat_block.get("labels", []) or []
                for athlete_row in stat_block.get("athletes", []) or []:
                    athlete = athlete_row.get("athlete") or {}
                    name = athlete.get("displayName") or athlete.get("shortName") or ""
                    vals = athlete_row.get("stats", []) or []
                    stat_parts: list[str] = []
                    for i, v in enumerate(vals):
                        if i >= len(labels):
                            break
                        label = str(labels[i]).upper()
                        if label in {"PTS", "REB", "AST", "HR", "RBI", "ERA", "SO", "IP", "TD", "YDS", "INT"}:
                            stat_parts.append(f"{v} {label}")
                    if not stat_parts and vals:
                        stat_parts = [str(vals[0])]
                    line = self._fmt_player_line(name, stat_parts)
                    if line and line not in seen:
                        seen.add(line)
                        lines.append(line)
                        if len(lines) >= limit:
                            return lines

        return lines

    @staticmethod
    def _extract_standings_entries(payload: dict[str, Any]) -> list[dict[str, Any]]:
        if not isinstance(payload, dict):
            return []
        if isinstance(payload.get("standings"), dict):
            entries = payload.get("standings", {}).get("entries", [])
            if isinstance(entries, list):
                return [e for e in entries if isinstance(e, dict)]
        children = payload.get("children") or []
        if isinstance(children, list):
            for child in children:
                if not isinstance(child, dict):
                    continue
                standings = child.get("standings") or {}
                entries = standings.get("entries", [])
                if isinstance(entries, list) and entries:
                    return [e for e in entries if isinstance(e, dict)]
        return []

    @staticmethod
    def _parse_stat_fields(stats: list[dict[str, Any]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for s in stats:
            if not isinstance(s, dict):
                continue
            keys = [
                str(s.get("name") or "").strip().lower(),
                str(s.get("abbreviation") or "").strip().lower(),
                str(s.get("shortDisplayName") or "").strip().lower(),
            ]
            display = str(s.get("displayValue") or s.get("value") or "").strip()
            if not display:
                continue
            for k in keys:
                if k:
                    out[k] = display
        return out

    def fetch_team_trends_snapshot(self, sport: str, limit: int = 12) -> list[dict[str, Any]]:
        if not self.espn_site_api_url:
            return []
        sport_path = self._sport_to_espn_path(sport)
        if not sport_path:
            return []
        url = f"{self.espn_site_api_url}/apis/site/v2/sports/{sport_path}/standings"
        try:
            with httpx.Client(timeout=20.0) as client:
                resp = client.get(url)
                resp.raise_for_status()
                payload = resp.json()
        except Exception as exc:
            logger.info(
                "espn_standings_fetch_failed",
                extra={"sport": sport, "url": url, "error": str(exc)},
            )
            return []

        rows: list[dict[str, Any]] = []
        for entry in self._extract_standings_entries(payload):
            team = entry.get("team") or {}
            name = str(team.get("abbreviation") or team.get("displayName") or "").strip()
            if not name:
                continue
            stat_map = self._parse_stat_fields(entry.get("stats") or [])

            wins = stat_map.get("wins")
            losses = stat_map.get("losses")
            record = stat_map.get("record") or stat_map.get("summary")
            if not record and wins and losses:
                record = f"{wins}-{losses}"
            last10 = (
                stat_map.get("last ten")
                or stat_map.get("lastten")
                or stat_map.get("last-10")
                or stat_map.get("last10")
                or ""
            )
            m = re.match(r"^\s*(\d+)-(\d+)\s*$", str(last10 or ""))
            if not m or (int(m.group(1)) + int(m.group(2)) != 10):
                last10 = ""
            streak = stat_map.get("streak") or "N/A"
            point_diff = (
                stat_map.get("point differential")
                or stat_map.get("differential")
                or stat_map.get("pointdiff")
                or "N/A"
            )
            points_for = stat_map.get("points for") or stat_map.get("pointsfor") or stat_map.get("pf") or "N/A"
            points_against = stat_map.get("points against") or stat_map.get("pointsagainst") or stat_map.get("pa") or "N/A"
            rows.append(
                {
                    "name": name,
                    "record": record or "N/A",
                    "last10": last10,
                    "streak": streak,
                    "point_diff": point_diff,
                    "points_for": points_for,
                    "points_against": points_against,
                }
            )
            if len(rows) >= limit:
                break
        return rows

    def fetch_recent_team_form(
        self,
        sport: str,
        team_filter: set[str] | None = None,
        on_date: date | None = None,
        days_back: int = 45,
        max_games: int = 10,
        limit: int = 12,
    ) -> list[dict[str, Any]]:
        if not self.espn_site_api_url:
            return []
        sport_path = self._sport_to_espn_path(sport)
        if not sport_path:
            return []

        selected = {t.upper().strip() for t in (team_filter or set()) if t and str(t).strip()}
        if selected and len(selected) > 32:
            selected = set(sorted(selected)[:32])
        scoreboard_url = f"{self.espn_site_api_url}/apis/site/v2/sports/{sport_path}/scoreboard"
        team_games: dict[str, dict[str, Any]] = {}

        anchor_date = on_date or date.today()
        with httpx.Client(timeout=8.0) as client:
            for offset in range(1, max(1, days_back) + 1):
                d = anchor_date - timedelta(days=offset)
                dates_param = d.strftime("%Y%m%d")
                try:
                    resp = client.get(scoreboard_url, params={"dates": dates_param})
                    resp.raise_for_status()
                    payload = resp.json()
                except Exception:
                    continue

                for event in payload.get("events", []) or []:
                    state = str(((event.get("status") or {}).get("type") or {}).get("state") or "").lower()
                    if state != "post":
                        continue
                    comps = event.get("competitions") or []
                    if not comps:
                        continue
                    comp = comps[0]
                    competitors = comp.get("competitors") or []
                    home = next((c for c in competitors if c.get("homeAway") == "home"), None)
                    away = next((c for c in competitors if c.get("homeAway") == "away"), None)
                    if not home or not away:
                        continue
                    home_team = home.get("team") or {}
                    away_team = away.get("team") or {}
                    home_abbr = str(home_team.get("abbreviation") or "").upper().strip()
                    away_abbr = str(away_team.get("abbreviation") or "").upper().strip()
                    home_id = str(home_team.get("id") or home_abbr).strip()
                    away_id = str(away_team.get("id") or away_abbr).strip()
                    if not home_abbr or not away_abbr:
                        continue

                    if selected and home_abbr not in selected and away_abbr not in selected:
                        continue

                    try:
                        home_score = int(float(home.get("score") or 0))
                        away_score = int(float(away.get("score") or 0))
                    except Exception:
                        continue

                    home_won = home_score > away_score
                    away_won = away_score > home_score
                    def _overall_record(competitor: dict[str, Any]) -> str:
                        for record in competitor.get("records") or []:
                            if str(record.get("type") or "").strip().lower() == "total":
                                return str(record.get("summary") or "").strip()
                        return ""

                    def _rank(competitor: dict[str, Any]) -> int | None:
                        try:
                            current = ((competitor.get("curatedRank") or {}).get("current"))
                            if current is None:
                                return None
                            value = int(current)
                            return value if 0 < value <= 25 else None
                        except Exception:
                            return None

                    h = team_games.setdefault(home_id, {"abbr": home_abbr, "games": [], "record": "", "rank": None})
                    h["abbr"] = home_abbr or h.get("abbr") or ""
                    if not str(h.get("record") or "").strip():
                        h["record"] = _overall_record(home) or ""
                    rank = _rank(home)
                    if h.get("rank") is None and rank is not None:
                        h["rank"] = rank
                    h["games"].append(
                        {"won": home_won, "pf": home_score, "pa": away_score, "date": dates_param}
                    )
                    a = team_games.setdefault(away_id, {"abbr": away_abbr, "games": [], "record": "", "rank": None})
                    a["abbr"] = away_abbr or a.get("abbr") or ""
                    if not str(a.get("record") or "").strip():
                        a["record"] = _overall_record(away) or ""
                    rank = _rank(away)
                    if a.get("rank") is None and rank is not None:
                        a["rank"] = rank
                    a["games"].append(
                        {"won": away_won, "pf": away_score, "pa": home_score, "date": dates_param}
                    )

        rows: list[dict[str, Any]] = []
        for payload in team_games.values():
            team = str(payload.get("abbr", "")).upper().strip()
            games = payload.get("games", []) or []
            if selected and team not in selected:
                continue
            if not games:
                continue
            all_games = sorted(games, key=lambda g: str(g.get("date", "")), reverse=True)
            sorted_games = all_games[:max_games]
            n = len(sorted_games)
            wins = sum(1 for g in sorted_games if g.get("won"))
            losses = n - wins
            pf_avg = round(sum(float(g.get("pf") or 0.0) for g in sorted_games) / n, 1)
            pa_avg = round(sum(float(g.get("pa") or 0.0) for g in sorted_games) / n, 1)
            diff = round(pf_avg - pa_avg, 1)

            streak_len = 0
            streak_win = bool(all_games[0].get("won"))
            for g in all_games:
                if bool(g.get("won")) == streak_win:
                    streak_len += 1
                else:
                    break
            record_token = str(payload.get("record", "")).strip()
            m = re.match(r"^\s*(\d+)-(\d+)\s*$", record_token)
            if m:
                overall_wins = int(m.group(1))
                overall_losses = int(m.group(2))
                if streak_win and overall_losses == 0:
                    streak_len = max(streak_len, overall_wins)
                if (not streak_win) and overall_wins == 0:
                    streak_len = max(streak_len, overall_losses)
            streak = f"{'W' if streak_win else 'L'}{streak_len}"

            if diff >= 8:
                form_note = f"won {wins} of last {n}; strong recent form (+{diff} avg margin)."
            elif diff <= -8:
                form_note = f"won {wins} of last {n}; currently underwater ({diff} avg margin)."
            elif pf_avg <= 90 and sport in {"nba", "college_basketball", "womens_college_basketball", "wnba"}:
                form_note = f"won {wins} of last {n}; scoring slump ({pf_avg} PPG in span)."
            else:
                form_note = f"won {wins} of last {n}; net {diff:+.1f} over recent stretch."

            rows.append(
                {
                    "name": team,
                    "record": str(payload.get("record", "")).strip(),
                    "rank": payload.get("rank"),
                    "last10": f"{wins}-{losses}" if n >= 10 else "",
                    "recent_record": f"{wins}-{losses}",
                    "recent_games": n,
                    "streak": streak,
                    "point_diff": diff,
                    "points_for": pf_avg,
                    "points_against": pa_avg,
                    "trend_note": form_note,
                }
            )

        def _wins_key(row: dict[str, Any]) -> int:
            token = str(row.get("last10", "")).strip()
            if not token:
                token = str(row.get("recent_record", "")).strip()
            try:
                return int(token.split("-")[0])
            except Exception:
                return 0

        rows.sort(key=lambda x: (_wins_key(x), float(x.get("point_diff", 0.0))), reverse=True)
        return rows[:limit]

    @staticmethod
    def _core_leader_url_candidates(sport: str, on_date: date) -> list[str]:
        s = (sport or "").strip().lower()
        year = on_date.year
        candidates: list[str] = []
        if s == "nba":
            candidates = [
                f"https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/seasons/{year}/types/2/leaders/0?lang=en&region=us",
                f"https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/seasons/{year-1}/types/2/leaders/0?lang=en&region=us",
            ]
        elif s == "college_basketball":
            candidates = [
                f"https://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball/seasons/{year}/types/2/leaders/0?lang=en&region=us",
                f"https://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball/seasons/{year-1}/types/2/leaders/0?lang=en&region=us",
            ]
        elif s == "womens_college_basketball":
            candidates = [
                f"https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/seasons/{year}/types/2/leaders/0?lang=en&region=us",
                f"https://sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/seasons/{year-1}/types/2/leaders/0?lang=en&region=us",
            ]
        elif s == "nfl":
            candidates = [
                f"https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{year-1}/types/2/leaders/0?lang=en&region=us",
                f"https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{year}/types/2/leaders/0?lang=en&region=us",
            ]
        elif s == "mlb":
            candidates = [
                f"https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/seasons/{year}/types/1/leaders/0?lang=en&region=us",
                f"https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/seasons/{year-1}/types/2/leaders/0?lang=en&region=us",
            ]
        elif s == "nhl":
            candidates = [
                f"https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{year}/types/2/leaders/0?lang=en&region=us",
                f"https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/seasons/{year-1}/types/2/leaders/0?lang=en&region=us",
            ]
        return candidates

    def fetch_season_leader_snapshot(self, sport: str, on_date: date, max_categories: int = 4, max_leaders: int = 3) -> dict[str, list[dict[str, str]]]:
        candidates = self._core_leader_url_candidates(sport, on_date)
        if not candidates:
            return {}

        payload: dict[str, Any] | None = None
        with httpx.Client(timeout=6.0) as client:
            for url in candidates:
                try:
                    resp = client.get(url)
                    resp.raise_for_status()
                    payload = resp.json()
                    break
                except Exception:
                    continue
            if not isinstance(payload, dict):
                return {}

            categories = payload.get("categories") or []
            if not isinstance(categories, list):
                return {}

            desired_by_sport = {
                "nba": {
                    "Points Per Game": ["points per game", "points"],
                    "Rebounds Per Game": ["rebounds per game", "rebounds"],
                    "Assists Per Game": ["assists per game", "assists"],
                    "Steals Per Game": ["steals per game", "steals"],
                    "Blocks Per Game": ["blocks per game", "blocks"],
                },
                "college_basketball": {
                    "Points Per Game": ["points per game", "points"],
                    "Rebounds Per Game": ["rebounds per game", "rebounds"],
                    "Assists Per Game": ["assists per game", "assists"],
                    "Steals Per Game": ["steals per game", "steals"],
                    "Blocks Per Game": ["blocks per game", "blocks"],
                },
                "womens_college_basketball": {
                    "Points Per Game": ["points per game", "points"],
                    "Rebounds Per Game": ["rebounds per game", "rebounds"],
                    "Assists Per Game": ["assists per game", "assists"],
                    "Steals Per Game": ["steals per game", "steals"],
                    "Blocks Per Game": ["blocks per game", "blocks"],
                },
                "nfl": {
                    "Passing Yards": ["passing yards"],
                    "Rushing Yards": ["rushing yards"],
                    "Receiving Yards": ["receiving yards"],
                    "Touchdown Passes": ["touchdown passes", "passing touchdowns"],
                },
                "mlb": {
                    "Batting Average": ["batting average"],
                    "Home Runs": ["home runs"],
                    "Runs Batted In": ["runs batted in", "rbi"],
                    "ERA": ["era", "earned run average"],
                },
                "nhl": {
                    "Points": ["points"],
                    "Goals": ["goals"],
                    "Assists": ["assists"],
                    "Shots": ["shots"],
                },
            }
            desired = desired_by_sport.get((sport or "").strip().lower(), {})
            athlete_cache: dict[str, str] = {}
            athlete_team_cache: dict[str, str] = {}
            team_division_cache: dict[str, str] = {}
            out: dict[str, list[dict[str, str]]] = {}

            for name, aliases in desired.items():
                match = None
                for c in categories:
                    if not isinstance(c, dict):
                        continue
                    dn = str(c.get("displayName") or c.get("name") or "")
                    dnl = dn.lower()
                    if any(alias in dnl for alias in aliases):
                        match = c
                        break
                if not match:
                    continue
                leaders = match.get("leaders") or []
                if not leaders:
                    continue
                rows: list[dict[str, str]] = []
                for leader in leaders[: max(20, max_leaders * 8)]:
                    athlete_ref = str((leader.get("athlete") or {}).get("$ref") or "").strip()
                    display_val = str(leader.get("displayValue") or leader.get("value") or "").strip()
                    if not athlete_ref or not display_val:
                        continue
                    athlete_ref_url = athlete_ref.replace("http://sports.core.api.espn.com", "https://sports.core.api.espn.com")
                    athlete_name = athlete_cache.get(athlete_ref)
                    athlete_team = athlete_team_cache.get(athlete_ref)
                    if athlete_name is None:
                        athlete_name = "Unknown"
                        try:
                            a_resp = client.get(athlete_ref_url)
                            a_resp.raise_for_status()
                            a_js = a_resp.json() or {}
                            athlete_name = str(a_js.get("displayName") or a_js.get("fullName") or a_js.get("shortName") or "Unknown").strip()
                            team_ref = str(((a_js.get("team") or {}).get("$ref") or "")).replace(
                                "http://sports.core.api.espn.com",
                                "https://sports.core.api.espn.com",
                            )
                            if team_ref:
                                try:
                                    t_resp = client.get(team_ref)
                                    t_resp.raise_for_status()
                                    t_js = t_resp.json() or {}
                                    athlete_team = str(
                                        t_js.get("abbreviation")
                                        or t_js.get("shortDisplayName")
                                        or t_js.get("displayName")
                                        or ""
                                    ).upper().strip()
                                    grp_ref = str(((t_js.get("groups") or {}).get("$ref") or "")).replace(
                                        "http://sports.core.api.espn.com",
                                        "https://sports.core.api.espn.com",
                                    )
                                    if grp_ref and grp_ref not in team_division_cache:
                                        try:
                                            g_resp = client.get(grp_ref)
                                            g_resp.raise_for_status()
                                            g_js = g_resp.json() or {}
                                            team_division_cache[grp_ref] = str(
                                                g_js.get("name") or g_js.get("shortName") or ""
                                            ).strip()
                                        except Exception:
                                            team_division_cache[grp_ref] = ""
                                    if grp_ref:
                                        division_name = team_division_cache.get(grp_ref, "")
                                        if sport in {"college_basketball", "womens_college_basketball"} and "non-ncaa" in division_name.lower():
                                            athlete_team = ""
                                except Exception:
                                    athlete_team = ""
                        except Exception:
                            m = re.search(r"/athletes/(\\d+)", athlete_ref)
                            if m:
                                athlete_name = f"Athlete {m.group(1)}"
                        athlete_cache[athlete_ref] = athlete_name
                        athlete_team_cache[athlete_ref] = athlete_team or ""
                    rows.append(
                        {
                            "name": athlete_name,
                            "stat": display_val,
                            "team": athlete_team_cache.get(athlete_ref, ""),
                        }
                    )
                    if len(rows) >= max_leaders:
                        break
                if rows:
                    out[name] = rows
                if len(out) >= max_categories:
                    break
            return out

    @staticmethod
    def _extract_award_odds_rows(payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, dict):
            if isinstance(payload.get("data"), list):
                rows = payload.get("data")
            elif isinstance(payload.get("rows"), list):
                rows = payload.get("rows")
            elif isinstance(payload.get("players"), list):
                rows = payload.get("players")
            elif isinstance(payload.get("odds"), list):
                rows = payload.get("odds")
            else:
                rows = []
        elif isinstance(payload, list):
            rows = payload
        else:
            rows = []

        out: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            name = str(
                row.get("name")
                or row.get("player")
                or row.get("participant")
                or row.get("competitor")
                or ""
            ).strip()
            if not name:
                continue
            odds = (
                row.get("american")
                or row.get("american_odds")
                or row.get("price")
                or row.get("odds")
                or row.get("line")
                or ""
            )
            book = str(row.get("bookmaker") or row.get("sportsbook") or row.get("book") or "").strip()
            updated = str(row.get("updated_at") or row.get("last_updated") or row.get("timestamp") or "").strip()
            out.append(
                {
                    "name": name,
                    "odds": str(odds).strip(),
                    "bookmaker": book,
                    "updated_at": updated,
                }
            )
        return out

    def fetch_previous_day_scores(self, run_date: str) -> list[dict]:
        rows: list[dict] = []
        if self._should_use_generic():
            try:
                rows.extend(self._fetch_generic_scores(run_date))
            except Exception as exc:
                logger.info("sports_generic_scores_failed", extra={"error": str(exc)})

        rows.extend(self._fetch_espn(run_date, completed=True))
        rows.extend(self._fetch_sportsdb_scores(run_date))
        rows = self._dedupe(rows)
        if rows:
            return rows
        if self.allow_synthetic_fallback:
            logger.warning("sports_api_fallback_scores_synthetic", extra={"error": "all providers empty"})
            return self._fallback_scores(run_date)
        raise RuntimeError("sports ingest returned no real provider scores; synthetic fallback disabled")

    def fetch_upcoming_matchups(self, run_date: str) -> list[dict]:
        rows: list[dict] = []
        if self._should_use_generic():
            try:
                rows.extend(self._fetch_generic_upcoming(run_date))
            except Exception as exc:
                logger.info("sports_generic_upcoming_failed", extra={"error": str(exc)})

        rows.extend(self._fetch_espn(run_date, completed=False))
        rows = self._dedupe(rows)
        if rows:
            return rows
        if self.allow_synthetic_fallback:
            logger.warning("sports_api_fallback_upcoming_synthetic", extra={"error": "all providers empty"})
            return self._fallback_upcoming(run_date)
        raise RuntimeError("sports ingest returned no real provider upcoming rows; synthetic fallback disabled")
