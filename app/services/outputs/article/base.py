from __future__ import annotations

from datetime import date
import re
from typing import Any

from app.services.outputs.article.common import build_internal_links, slugify, stat_line_from_blob, title_case_sport


class BaseLeagueArticleBuilder:
    sport: str = ""
    section_label: str = "League Context"

    def build(self, *, run_date: str, storyset: dict[str, Any], blob_index: dict[str, dict[str, Any]]) -> dict[str, Any]:
        primary = [blob_index[b] for b in storyset.get("primary_blobs", []) if b in blob_index]
        context = [blob_index[b] for b in storyset.get("context_blobs", []) if b in blob_index]
        title = self.build_title(run_date, storyset, primary)
        slug = slugify(title)
        meta = self.build_meta_description(storyset, primary)
        markdown, teams, players, citations = self.build_markdown(run_date, storyset, primary, context)
        links = build_internal_links(teams, players, self.sport)
        return {
            "sport": self.sport,
            "article_type": storyset.get("article_type"),
            "central_thesis": storyset.get("central_thesis"),
            "title": title,
            "slug": slug,
            "meta_description": meta,
            "markdown": markdown,
            "internal_links": links,
            "citations": citations,
            "schema_org_jsonld": {
                "@context": "https://schema.org",
                "@type": "Article",
                "headline": title,
                "datePublished": run_date,
                "description": meta,
                "articleSection": self.sport,
            },
        }

    def build_title(self, run_date: str, storyset: dict[str, Any], primary: list[dict[str, Any]]) -> str:
        lead = primary[0]
        tf = self._typed(lead)
        outcome = self._resolved_game_outcome(lead)
        team_a = outcome["winner"] or outcome["home_team"] or self._clean_token(tf.get("team"))
        team_b = outcome["loser"] or outcome["away_team"]
        headline = self._clean_token(tf.get("headline") or tf.get("title"))
        d = date.fromisoformat(run_date)
        day = d.strftime("%B %d").replace(" 0", " ")
        if storyset.get("article_type") == "news_focus" and headline:
            return f"{title_case_sport(self.sport)}: {headline} on {day}"
        if team_a and team_b:
            return f"{title_case_sport(self.sport)}: {team_a} vs. {team_b} on {day}"
        return f"{title_case_sport(self.sport)} notebook for {run_date}"

    def build_meta_description(self, storyset: dict[str, Any], primary: list[dict[str, Any]]) -> str:
        thesis = str(storyset.get("central_thesis", "")).strip()
        teams: list[str] = []
        headlines: list[str] = []
        for blob in primary:
            tf = self._typed(blob)
            outcome = self._resolved_game_outcome(blob)
            for token in (outcome["winner"], outcome["loser"], outcome["home_team"], outcome["away_team"]):
                if token and token not in teams:
                    teams.append(token)
            headline = self._clean_token(tf.get("headline") or tf.get("title"))
            if headline and headline not in headlines:
                headlines.append(headline)
        if storyset.get("article_type") == "news_focus" and headlines:
            return f"{thesis} Reporting on {headlines[0]} with league context and downstream impact."[:155]
        return f"{thesis} Key results and context on {', '.join(teams[:3])}."[:155]

    def build_markdown(
        self,
        run_date: str,
        storyset: dict[str, Any],
        primary: list[dict[str, Any]],
        context: list[dict[str, Any]],
    ) -> tuple[str, list[str], list[str], list[str]]:
        thesis = str(storyset.get("central_thesis", "")).strip()
        teams: list[str] = []
        players: list[str] = []
        citations: list[str] = []
        lines = [f"# {self.build_title(run_date, storyset, primary)}", "", thesis, ""]
        lines.extend(self._lede(run_date, storyset, primary, context))
        lines.append("")
        lines.append("## The Story")
        lines.append("")
        if storyset.get("article_type") == "news_focus":
            lines.extend(self._news_section(primary, context, teams, players, citations))
        else:
            lines.extend(self._games_section(primary, context, teams, players, citations))
        secondary = self._secondary_signals(storyset, context, teams, players)
        if secondary:
            lines.append("## Secondary Signals")
            lines.append("")
            lines.extend(secondary)
        lines.append(f"## {self.section_label}")
        lines.append("")
        lines.extend(self._context_section(primary, context, teams, players))
        lines.append("")
        lines.append("## Forward Look")
        lines.append("")
        lines.extend(self._forward_look(primary, context))
        return (
            "\n".join(lines).strip() + "\n",
            list(dict.fromkeys([t for t in teams if t])),
            list(dict.fromkeys([p for p in players if p])),
            list(dict.fromkeys([c for c in citations if c])),
        )

    def _typed(self, blob: dict[str, Any]) -> dict[str, Any]:
        return ((blob.get("facts") or {}).get("typed_fields") or {})

    def _source_ids(self, blob: dict[str, Any]) -> list[str]:
        return list((blob.get("provenance") or {}).get("source_ids") or [])

    def _game_id(self, blob: dict[str, Any]) -> str:
        return str((blob.get("entities") or {}).get("game_id") or "")

    def _clean_token(self, value: Any) -> str:
        text = str(value or "").strip()
        if not text or text.upper() == "N/A":
            return ""
        return text

    def _int_or_none(self, value: Any) -> int | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return int(value)
        text = str(value).strip()
        if text.lstrip("-").isdigit():
            return int(text)
        return None

    def _resolved_game_outcome(self, blob: dict[str, Any]) -> dict[str, Any]:
        tf = self._typed(blob)
        home_team = self._clean_token(tf.get("home_team"))
        away_team = self._clean_token(tf.get("away_team"))
        title = self._clean_token(tf.get("title"))
        if title and (not home_team or not away_team):
            match = re.search(r"(.+?)\s+at\s+(.+)", title, re.IGNORECASE)
            if match:
                away_team = away_team or self._clean_token(match.group(1))
                home_team = home_team or self._clean_token(match.group(2))
        home_score = self._int_or_none(tf.get("home_score"))
        away_score = self._int_or_none(tf.get("away_score"))
        winner = self._clean_token(tf.get("winner"))
        loser = self._clean_token(tf.get("loser"))
        winner_score = None
        loser_score = None
        if home_score is not None and away_score is not None:
            if home_score > away_score:
                winner = winner or home_team
                loser = loser or away_team
                winner_score, loser_score = home_score, away_score
            elif away_score > home_score:
                winner = winner or away_team
                loser = loser or home_team
                winner_score, loser_score = away_score, home_score
            else:
                winner_score, loser_score = home_score, away_score
        return {
            "title": title,
            "home_team": home_team,
            "away_team": away_team,
            "winner": winner,
            "loser": loser,
            "winner_score": winner_score,
            "loser_score": loser_score,
        }

    def _player_lines_for_game(self, game_blob: dict[str, Any], context: list[dict[str, Any]]) -> list[dict[str, Any]]:
        game_id = self._game_id(game_blob)
        if not game_id:
            return []
        return [blob for blob in context if blob.get("blob_type") == "player_line" and self._game_id(blob) == game_id]

    def _trend_for_team(self, team: str, context: list[dict[str, Any]]) -> dict[str, Any] | None:
        target = team.strip().upper()
        if not target:
            return None
        for blob in context:
            if blob.get("blob_type") != "team_trend":
                continue
            if self._clean_token(self._typed(blob).get("name")).upper() == target:
                return blob
        return None

    def _overall_record(self, trend_blob: dict[str, Any] | None) -> str:
        if not trend_blob:
            return ""
        tf = self._typed(trend_blob)
        for key in ("overall_record", "season_record", "record"):
            token = self._clean_token(tf.get(key))
            if token:
                return token
        return ""

    def _recent_record(self, trend_blob: dict[str, Any] | None) -> str:
        if not trend_blob:
            return ""
        return self._clean_token(self._typed(trend_blob).get("last10"))

    def _trend_line(self, trend_blob: dict[str, Any] | None) -> str:
        if not trend_blob:
            return ""
        tf = self._typed(trend_blob)
        parts: list[str] = []
        record = self._overall_record(trend_blob)
        if record:
            parts.append(f"{record} overall")
        recent = self._recent_record(trend_blob)
        if recent:
            parts.append(f"{recent} in the recent sample")
        streak = self._clean_token(tf.get("streak"))
        if streak:
            parts.append(f"{streak} streak")
        point_diff = self._clean_token(tf.get("point_diff"))
        if point_diff:
            parts.append(f"{point_diff} scoring differential")
        return ", ".join(parts)

    def _matchups(self, context: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [blob for blob in context if blob.get("blob_type") == "matchup"]

    def _news(self, context: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [blob for blob in context if blob.get("blob_type") == "news"]

    def _score_term(self) -> str:
        if self.sport in {"mlb"}:
            return "runs"
        if self.sport in {"nhl"}:
            return "goals"
        return "points"

    def _margin_term(self) -> str:
        return self._score_term()

    def _record_clause(self, trend_blob: dict[str, Any] | None) -> str:
        parts: list[str] = []
        overall = self._overall_record(trend_blob)
        recent = self._recent_record(trend_blob)
        if overall:
            parts.append(f"{overall} overall")
        if recent:
            parts.append(f"{recent} lately")
        return ", ".join(parts)

    def _result_sentence(self, blob: dict[str, Any]) -> str:
        outcome = self._resolved_game_outcome(blob)
        winner = outcome["winner"] or outcome["home_team"]
        loser = outcome["loser"] or outcome["away_team"]
        winner_score = outcome["winner_score"]
        loser_score = outcome["loser_score"]
        title = outcome["title"] or "the lead game"
        if winner_score is not None and loser_score is not None:
            if winner_score == loser_score:
                if winner and loser:
                    return f"{winner} and {loser} finished level at {winner_score}-{loser_score}."
                return f"{title} ended {winner_score}-{loser_score}."
            if winner and loser:
                return f"{winner} beat {loser} {winner_score}-{loser_score}."
            return f"{title} finished {winner_score}-{loser_score}."
        if winner and loser:
            return f"{winner} got past {loser}."
        return "The lead game carried enough weight to shape the article."

    def _supporting_sentences(self, context: list[dict[str, Any]]) -> list[str]:
        out: list[str] = []
        for blob in context:
            if blob.get("blob_type") == "team_trend":
                tf = self._typed(blob)
                name = self._clean_token(tf.get("name")) or "This team"
                trend_line = self._trend_line(blob)
                if trend_line:
                    out.append(f"{name} carries {trend_line}.")
            elif blob.get("blob_type") == "player_line":
                line = stat_line_from_blob(blob)
                if line:
                    out.append(f"On the player side, {line}.")
            elif blob.get("blob_type") == "matchup":
                tf = self._typed(blob)
                away_team = self._clean_token(tf.get("away_team")) or "the road side"
                home_team = self._clean_token(tf.get("home_team")) or "the home side"
                out.append(f"The next checkpoint is {away_team} at {home_team}.")
            if len(out) >= 4:
                break
        return out

    def _lede(self, run_date: str, storyset: dict[str, Any], primary: list[dict[str, Any]], context: list[dict[str, Any]]) -> list[str]:
        lead = primary[0]
        tf = self._typed(lead)
        if storyset.get("article_type") == "news_focus":
            headline = self._clean_token(tf.get("headline") or tf.get("title"))
            summary = self._clean_token(tf.get("summary"))
            return [
                f"On {run_date}, the main {title_case_sport(self.sport)} question was whether {headline if headline else 'the lead headline'} would stay confined to the news cycle or start changing the league picture immediately.",
                f"The answer came quickly: {summary if summary else 'the reporting window delivered a story substantial enough to alter the next decision cycle.'}",
                "The rest of the piece tracks what changed, who it touches next, and which part of the calendar now looks different.",
            ]
        outcome = self._resolved_game_outcome(lead)
        winner = outcome["winner"] or outcome["home_team"]
        loser = outcome["loser"] or outcome["away_team"]
        margin = int(self._typed(lead).get("margin", 0) or 0)
        margin_term = self._margin_term()
        return [
            f"On {run_date}, the lead angle in {title_case_sport(self.sport)} was straightforward: {storyset['central_thesis']}",
            f"The lead result was {winner} over {loser} by {margin} {margin_term if margin != 1 else margin_term[:-1]}, and that score only matters because it fits into current form and the next stretch of the schedule.",
            f"The rest of the article tracks what that result says about {winner or 'the winner'}, {loser or 'the loser'}, and the next set of games around them.",
        ]

    def _games_section(self, primary: list[dict[str, Any]], context: list[dict[str, Any]], teams: list[str], players: list[str], citations: list[str]) -> list[str]:
        lines: list[str] = []
        for blob in primary[:6]:
            tf = self._typed(blob)
            outcome = self._resolved_game_outcome(blob)
            winner = outcome["winner"] or outcome["home_team"]
            loser = outcome["loser"] or outcome["away_team"]
            winner_score = outcome["winner_score"]
            loser_score = outcome["loser_score"]
            margin = int(tf.get("margin", 0) or 0)
            if winner:
                teams.append(winner)
            if loser:
                teams.append(loser)
            lines.append(f"## {winner} vs. {loser}" if winner and loser else f"## {outcome['title'] or 'Game'}")
            lines.append("")
            lines.append(self._result_sentence(blob))
            player_lines = self._player_lines_for_game(blob, context)
            stat_sentences = []
            for pl in player_lines[:3]:
                line = stat_line_from_blob(pl)
                if not line:
                    continue
                name = self._clean_token(self._typed(pl).get("player_name"))
                if name:
                    players.append(name)
                stat_sentences.append(line)
            if stat_sentences:
                lines.append(f"The box score hinge was {'; '.join(stat_sentences)}.")
            lines.append(self._impact_sentence(blob, context))
            lines.append(self._game_shape_sentence(blob, context))
            winner_trend = self._trend_for_team(winner, context)
            loser_trend = self._trend_for_team(loser, context)
            winner_line = self._trend_line(winner_trend)
            loser_line = self._trend_line(loser_trend)
            if winner_line and loser_line:
                lines.append(
                    f"{winner or 'The winner'} brings {winner_line}, while {loser or 'the loser'} comes in with {loser_line}. Together those form lines show where both teams stand after the result."
                )
            elif winner_line:
                lines.append(f"{winner or 'The winner'} now carries {winner_line}, which gives the result a clearer place in the current league picture.")
            lines.append("")
            citations.extend(self._source_ids(blob))
            for pl in player_lines[:3]:
                citations.extend(self._source_ids(pl))
        return lines

    def _news_section(self, primary: list[dict[str, Any]], context: list[dict[str, Any]], teams: list[str], players: list[str], citations: list[str]) -> list[str]:
        lines: list[str] = []
        for blob in primary[:5]:
            tf = self._typed(blob)
            headline = self._clean_token(tf.get("headline") or tf.get("title")) or "Story"
            summary = self._clean_token(tf.get("summary"))
            lines.append(f"## {headline}")
            lines.append("")
            if summary:
                lines.append(summary if summary.endswith(".") else f"{summary}.")
            for key in ("winner", "loser", "home_team", "away_team", "team"):
                token = self._clean_token(tf.get(key))
                if token and token not in teams:
                    teams.append(token)
            player_name = self._clean_token(tf.get("player_name"))
            if player_name:
                players.append(player_name)
            lines.append(self._impact_sentence(blob, context))
            for extra in self._supporting_sentences(context)[:2]:
                lines.append(extra)
            lines.append(self._news_shape_sentence(blob))
            lines.append("")
            citations.extend(self._source_ids(blob))
        return lines

    def _impact_sentence(self, blob: dict[str, Any], context: list[dict[str, Any]]) -> str:
        tf = self._typed(blob)
        winner = self._clean_token(tf.get("winner") or tf.get("home_team") or tf.get("team"))
        trend = self._trend_for_team(winner, context)
        line = self._trend_line(trend)
        if line and winner:
            return f"{winner} now carries {line}, which gives this result a firmer place in the wider league picture."
        if str(blob.get("blob_type")) == "news":
            headline = self._clean_token(tf.get("headline") or tf.get("title")) or "the story"
            return f"{headline} changes the next round of decisions by forcing a real adjustment on the league calendar."
        margin = int(tf.get("margin", 0) or 0)
        margin_term = self._margin_term()
        unit = margin_term[:-1] if abs(margin) == 1 else margin_term
        if margin == 0:
            return "The final score stayed level, so the result matters more for the surrounding form line than for any separation on the scoreboard."
        return f"A {margin}-{unit} margin gave {winner or 'the winner'} the clearer result by the end of the game."

    def _game_shape_sentence(self, blob: dict[str, Any], context: list[dict[str, Any]]) -> str:
        tf = self._typed(blob)
        outcome = self._resolved_game_outcome(blob)
        winner = outcome["winner"] or outcome["home_team"]
        loser = outcome["loser"] or outcome["away_team"]
        margin = int(tf.get("margin", 0) or 0)
        total_points = 0
        for key in ("home_score", "away_score"):
            value = self._int_or_none(tf.get(key))
            if value is not None:
                total_points += value
        next_matchup = next(
            (
                b
                for b in self._matchups(context)
                if winner
                and winner in {
                    self._clean_token(self._typed(b).get("away_team")),
                    self._clean_token(self._typed(b).get("home_team")),
                }
            ),
            None,
        )
        score_term = self._score_term()
        margin_term = self._margin_term()
        total_unit = score_term[:-1] if total_points == 1 else score_term
        margin_unit = margin_term[:-1] if abs(margin) == 1 else margin_term
        if margin == 0:
            sentence = (
                f"The game finished tied inside a {total_points}-{total_unit} scoring profile, so the more useful read is how both teams carry this form into the next date."
            )
        else:
            sentence = (
                f"{winner or 'The winner'} built a {margin}-{margin_unit} margin in a {total_points}-{total_unit} game, "
                f"which shows the result held up across the full night rather than turning on one late swing."
            )
        if next_matchup:
            mtf = self._typed(next_matchup)
            away_team = self._clean_token(mtf.get("away_team")) or "the next opponent"
            home_team = self._clean_token(mtf.get("home_team")) or "the next site"
            sentence += f" The next checkpoint is {away_team} at {home_team}, where that result gets tested again immediately."
        elif loser and margin != 0:
            sentence += f" For {loser}, the next game will show whether this was a one-night dip or the start of a larger slide."
        return sentence

    def _news_shape_sentence(self, blob: dict[str, Any]) -> str:
        tf = self._typed(blob)
        headline = self._clean_token(tf.get("headline") or tf.get("title")) or "this update"
        return f"{headline} creates a concrete decision point for the next part of the league calendar."

    def _context_section(self, primary: list[dict[str, Any]], context: list[dict[str, Any]], teams: list[str], players: list[str]) -> list[str]:
        lines: list[str] = []
        for trend in [b for b in context if b.get("blob_type") == "team_trend"][:6]:
            tf = self._typed(trend)
            name = self._clean_token(tf.get("name"))
            if name and name not in teams:
                teams.append(name)
            trend_line = self._trend_line(trend)
            if not name or not trend_line:
                continue
            lines.append(f"{name} enters the next stretch with {trend_line}.")
            lines.append(f"That profile shows whether {name} is stabilizing, drifting, or building toward a more consequential stretch on the calendar.")
        for news in self._news(context)[:3]:
            tf = self._typed(news)
            headline = self._clean_token(tf.get("headline"))
            summary = self._clean_token(tf.get("summary"))
            if headline:
                detail = summary if summary else "it shifts the next decision window."
                lines.append(f"{headline} remains relevant because {detail}")
        return lines[:8]

    def _secondary_signals(self, storyset: dict[str, Any], context: list[dict[str, Any]], teams: list[str], players: list[str]) -> list[str]:
        lines: list[str] = []
        for counter in storyset.get("counterpoints") or []:
            text = str(counter).strip()
            if not text:
                continue
            lines.append(f"{text} remained part of the night and added another layer to the broader picture.")
        for blob in [b for b in context if b.get("blob_type") == "player_line"][:2]:
            line = stat_line_from_blob(blob)
            if not line:
                continue
            player = self._clean_token(self._typed(blob).get("player_name"))
            if player:
                players.append(player)
            lines.append(f"One supporting stat line was {line}, which adds a second layer of evidence beyond the headline result.")
        for blob in [b for b in context if b.get("blob_type") == "team_trend"][:2]:
            tf = self._typed(blob)
            team = self._clean_token(tf.get("name"))
            if team:
                teams.append(team)
            trend_line = self._trend_line(blob)
            if trend_line:
                lines.append(f"{team or 'A nearby team trend'} adds context here. {trend_line} gives the lead result a clearer place in the wider table.")
        return lines[:8]

    def _forward_look(self, primary: list[dict[str, Any]], context: list[dict[str, Any]]) -> list[str]:
        lines: list[str] = []
        for matchup in self._matchups(context)[:3]:
            tf = self._typed(matchup)
            away_team = self._clean_token(tf.get("away_team")) or "the road side"
            home_team = self._clean_token(tf.get("home_team")) or "the home side"
            away_record = self._clean_token(tf.get("away_record"))
            home_record = self._clean_token(tf.get("home_record"))
            record_clause = f", with {away_record} against {home_record}," if away_record and home_record else ""
            lines.append(f"The next checkpoint is {away_team} at {home_team}{record_clause} and it should clarify whether the current trend line holds.")
            lines.append("The next result should show whether the current trend line holds or starts to turn.")
        if not lines:
            lines.append("The next 48 hours matter because the strongest teams from this cycle now carry either momentum to protect or damage to clean up.")
            lines.append("The next slate should clarify whether these trends are holding or beginning to turn.")
        return lines
