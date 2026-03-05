from __future__ import annotations

from typing import Any

from app.services.outputs.article.base import BaseLeagueArticleBuilder
from app.services.outputs.article.common import stat_line_from_blob


class CollegeBasketballArticleBuilder(BaseLeagueArticleBuilder):
    sport = "college_basketball"
    section_label = "Conference and Tournament Context"

    def _lede(self, run_date: str, storyset: dict[str, Any], primary: list[dict[str, Any]], context: list[dict[str, Any]]) -> list[str]:
        lead = primary[0]
        tf = self._typed(lead)
        if storyset.get("article_type") == "news_focus":
            headline = str(tf.get("headline") or tf.get("title") or "").strip()
            summary = str(tf.get("summary") or "").strip()
            return [
                f"On {run_date}, the men’s college basketball story was whether {headline if headline else 'the lead report'} would affect seeding, bubble pressure, or conference tournament positioning in a real way.",
                f"{summary if summary else 'The reporting window produced a college basketball development strong enough to alter how the next set of bids and seed lines should be discussed.'}",
                "That is why the article moves quickly from the headline itself to the bracket consequence it implies. At this point in the season, context is not optional; it is the story.",
            ]
        winner = str(tf.get("winner") or tf.get("home_team") or "").strip()
        loser = str(tf.get("loser") or tf.get("away_team") or "").strip()
        margin = int(tf.get("margin", 0) or 0)
        return [
            f"On {run_date}, the men’s college basketball board tightened around one idea: {storyset['central_thesis']}",
            f"The lead result was {winner} over {loser} by {margin}, but the real value of the night was in how those points changed conference leverage, NCAA seeding pressure, and the quality of each team’s closing argument.",
            "That is the only way to treat this part of the calendar. The score matters, but the article has to explain how that score changes the path into March rather than pretending the game lived in isolation.",
        ]

    def _impact_sentence(self, blob: dict[str, Any], context: list[dict[str, Any]]) -> str:
        tf = self._typed(blob)
        if blob.get("blob_type") == "news":
            return super()._impact_sentence(blob, context)
        winner = str(tf.get("winner") or tf.get("home_team") or "").strip()
        trend = self._trend_for_team(winner, context)
        if not trend:
            return f"The tournament implication is that {winner or 'the winner'} added a result that should matter to selection and seeding discussions."
        record_clause = self._record_clause(trend)
        streak = self._clean_token(self._typed(trend).get("streak"))
        extras = f" and a {streak} streak" if streak else ""
        return (
            f"The tournament implication is that {winner} now carries {record_clause or 'a strong recent form line'}{extras}, "
            "which gives the resume more shape than one final score by itself ever could."
        )

    def _game_shape_sentence(self, blob: dict[str, Any], context: list[dict[str, Any]]) -> str:
        tf = self._typed(blob)
        winner = str(tf.get("winner") or tf.get("home_team") or "").strip()
        loser = str(tf.get("loser") or tf.get("away_team") or "").strip()
        margin = int(tf.get("margin", 0) or 0)
        total = sum(int(tf.get(key, 0) or 0) for key in ("home_score", "away_score"))
        return (
            f"In college basketball terms, a {margin}-point gap inside a {total}-point game matters because it clarifies whether {winner or 'the winner'} merely survived or actually controlled the terms of the night. "
            f"For {loser or 'the loser'}, that distinction can be the difference between a respectable loss and a result the committee room will read as warning tape."
        )

    def _context_section(self, primary: list[dict[str, Any]], context: list[dict[str, Any]], teams: list[str], players: list[str]) -> list[str]:
        lines: list[str] = []
        for trend in [b for b in context if b.get("blob_type") == "team_trend"][:5]:
            name = self._clean_token(self._typed(trend).get("name"))
            if name and name not in teams:
                teams.append(name)
            trend_line = self._trend_line(trend)
            if not name or not trend_line:
                continue
            lines.append(f"{name} now shows {trend_line}.")
            lines.append(
                f"That matters because men’s college basketball selection and seeding are driven by accumulation: if {name or 'this team'} keeps stacking that profile, the bracket language around it changes."
            )
        for blob in [b for b in context if b.get("blob_type") == "player_line"][:3]:
            line = stat_line_from_blob(blob)
            if not line:
                continue
            player = str(self._typed(blob).get("player_name", "")).strip()
            if player:
                players.append(player)
            lines.append(f"At the player level, {line} gave the result a recognizable driving force rather than leaving it as a faceless box score.")
        return lines[:8]

    def _forward_look(self, primary: list[dict[str, Any]], context: list[dict[str, Any]]) -> list[str]:
        lines: list[str] = []
        for matchup in self._matchups(context)[:3]:
            tf = self._typed(matchup)
            away_team = self._clean_token(tf.get("away_team")) or "the road side"
            home_team = self._clean_token(tf.get("home_team")) or "the home side"
            away_record = self._clean_token(tf.get("away_record"))
            home_record = self._clean_token(tf.get("home_record"))
            record_clause = f", where {away_record} meets {home_record}," if away_record and home_record else ", where another seeding data point gets added,"
            lines.append(
                f"Next comes {away_team} at {home_team}{record_clause} and another seeding or bubble data point gets added immediately."
            )
            lines.append(
                "That is the proper forward frame in this sport: the next game is rarely just the next game once conference tournaments and bracket lines are close enough to see."
            )
        if not lines:
            lines.append("The next men’s college basketball window matters because every strong late-season result starts pulling on conference tournament placement and NCAA seed lines at the same time.")
            lines.append("The useful question from here is which teams can keep adding clean evidence to the resume instead of leaving the committee room to guess.")
        return lines
