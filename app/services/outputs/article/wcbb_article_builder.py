from __future__ import annotations

from typing import Any

from app.services.outputs.article.base import BaseLeagueArticleBuilder
from app.services.outputs.article.common import stat_line_from_blob


class WomensCollegeBasketballArticleBuilder(BaseLeagueArticleBuilder):
    sport = "womens_college_basketball"
    section_label = "Tournament Race Context"

    def _lede(self, run_date: str, storyset: dict[str, Any], primary: list[dict[str, Any]], context: list[dict[str, Any]]) -> list[str]:
        lead = primary[0]
        tf = self._typed(lead)
        if storyset.get("article_type") == "news_focus":
            headline = str(tf.get("headline") or tf.get("title") or "").strip()
            summary = str(tf.get("summary") or "").strip()
            return [
                f"On {run_date}, the women’s college basketball conversation centered on whether {headline if headline else 'the lead report'} would change the shape of the national race as the bracket picture tightened.",
                f"{summary if summary else 'The reporting window carried enough weight to affect how the next phase of the women’s season should be read.'}",
                "That is the only useful frame now. The headline itself matters, but the bigger issue is whether it changes who controls the top of the bracket and which teams have enough substance to hold position.",
            ]
        winner = str(tf.get("winner") or tf.get("home_team") or "").strip()
        loser = str(tf.get("loser") or tf.get("away_team") or "").strip()
        margin = int(tf.get("margin", 0) or 0)
        return [
            f"On {run_date}, the women’s college basketball board pushed toward one conclusion: {storyset['central_thesis']}",
            f"The lead result was {winner} over {loser} by {margin}, but the actual story lives in what that game says about top-seed control, conference leverage, and how close each team is to entering March with real authority.",
            "The score is only step one. The stronger question is what changed because of it and who now carries a more credible tournament profile.",
        ]

    def _impact_sentence(self, blob: dict[str, Any], context: list[dict[str, Any]]) -> str:
        if blob.get("blob_type") == "news":
            return super()._impact_sentence(blob, context)
        tf = self._typed(blob)
        winner = str(tf.get("winner") or tf.get("home_team") or "").strip()
        trend = self._trend_for_team(winner, context)
        if not trend:
            return f"{winner or 'The winner'} added another result to its profile at the point in the season when staying power matters most."
        record_clause = self._record_clause(trend)
        streak = self._clean_token(self._typed(trend).get("streak"))
        extras = f" and a {streak} streak" if streak else ""
        return (
            f"{winner} now sits at {record_clause or 'a strong recent form line'}{extras}, "
            "which is the kind of form that changes seeding conversations and title credibility at the same time."
        )

    def _game_shape_sentence(self, blob: dict[str, Any], context: list[dict[str, Any]]) -> str:
        tf = self._typed(blob)
        winner = str(tf.get("winner") or tf.get("home_team") or "").strip()
        loser = str(tf.get("loser") or tf.get("away_team") or "").strip()
        margin = int(tf.get("margin", 0) or 0)
        total = sum(int(tf.get(key, 0) or 0) for key in ("home_score", "away_score"))
        return (
            f"A {margin}-point result inside a {total}-point game shows whether {winner or 'the winner'} looked like a team protecting top-end status or just escaping the night. "
            f"For {loser or 'the loser'}, it also creates a more urgent question about whether the next stretch is about repair or reassertion."
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
            lines.append(f"{name} carries {trend_line}.")
            lines.append(
                "The strongest teams are not just winning; they are building a profile that leaves little doubt about bracket placement and title viability."
            )
        for blob in [b for b in context if b.get("blob_type") == "player_line"][:3]:
            line = stat_line_from_blob(blob)
            if not line:
                continue
            player = str(self._typed(blob).get("player_name", "")).strip()
            if player:
                players.append(player)
            lines.append(f"At the player level, {line} supplied the kind of star-led evidence that separates a national contender from a team merely stacking wins.")
        return lines[:8]

    def _forward_look(self, primary: list[dict[str, Any]], context: list[dict[str, Any]]) -> list[str]:
        lines: list[str] = []
        for matchup in self._matchups(context)[:3]:
            tf = self._typed(matchup)
            away_team = self._clean_token(tf.get("away_team")) or "the road side"
            home_team = self._clean_token(tf.get("home_team")) or "the home side"
            away_record = self._clean_token(tf.get("away_record"))
            home_record = self._clean_token(tf.get("home_record"))
            record_clause = f", where {away_record} meets {home_record}," if away_record and home_record else ", where another bracket data point gets added,"
            lines.append(
                f"The next pressure point is {away_team} at {home_team}{record_clause} and another piece of bracket evidence gets added."
            )
            lines.append(
                "That is the right lens from here: not generic anticipation, but whether the next game confirms the profile these teams are trying to carry into March."
            )
        if not lines:
            lines.append("The next women’s college basketball window matters because elite records and tournament credibility only stay aligned when teams keep confirming them against real resistance.")
            lines.append("The next question is which teams can keep that authority intact as the bracket picture tightens.")
        return lines
