from __future__ import annotations

from typing import Any

from app.services.outputs.article.base import BaseLeagueArticleBuilder
from app.services.outputs.article.common import stat_line_from_blob


class NBAArticleBuilder(BaseLeagueArticleBuilder):
    sport = "nba"
    section_label = "Standings and Rotation Context"

    def _lede(self, run_date: str, storyset: dict[str, Any], primary: list[dict[str, Any]], context: list[dict[str, Any]]) -> list[str]:
        lead = primary[0]
        tf = self._typed(lead)
        if storyset.get("article_type") == "news_focus":
            headline = str(tf.get("headline") or tf.get("title") or "").strip()
            summary = str(tf.get("summary") or "").strip()
            return [
                f"On {run_date}, the NBA story was not just that {headline if headline else 'the lead report landed'}; it was that the news hit a part of the season where rotation stability, playoff positioning, and awards momentum all overlap.",
                f"{summary if summary else 'The reporting window supplied a development serious enough to alter the way the next week needs to be read.'}",
                "That is why this article opens with consequence rather than noise: the useful question is which teams or players now have to absorb the effect of this update on the next run of games.",
            ]
        winner = str(tf.get("winner") or tf.get("home_team") or "").strip()
        loser = str(tf.get("loser") or tf.get("away_team") or "").strip()
        margin = int(tf.get("margin", 0) or 0)
        return [
            f"On {run_date}, the NBA board sorted itself around one central point: {storyset['central_thesis']}",
            f"The headline result was {winner} over {loser} by {margin}, but the more important question was how that outcome moved the conference picture, team rotation decisions, and the pressure on the teams chasing playoff ground.",
            "From there, the article tracks what the result means for conference positioning, player usage, and the next 72 hours of the schedule.",
        ]

    def _impact_sentence(self, blob: dict[str, Any], context: list[dict[str, Any]]) -> str:
        if blob.get("blob_type") == "news":
            return super()._impact_sentence(blob, context)
        tf = self._typed(blob)
        winner = str(tf.get("winner") or tf.get("home_team") or "").strip()
        trend = self._trend_for_team(winner, context)
        if not trend:
            return f"{winner or 'The winner'} delivered a result strong enough to move the conference conversation."
        record_clause = self._record_clause(trend)
        streak = self._clean_token(self._typed(trend).get("streak"))
        extras = f" and a {streak} streak" if streak else ""
        return (
            f"{winner} now sits at {record_clause or 'a credible current form line'}{extras}, "
            "which shows a team tightening its playoff case and forcing everyone behind it to respond."
        )

    def _game_shape_sentence(self, blob: dict[str, Any], context: list[dict[str, Any]]) -> str:
        tf = self._typed(blob)
        winner = str(tf.get("winner") or tf.get("home_team") or "").strip()
        loser = str(tf.get("loser") or tf.get("away_team") or "").strip()
        margin = int(tf.get("margin", 0) or 0)
        total = sum(int(tf.get(key, 0) or 0) for key in ("home_score", "away_score"))
        matchups = self._matchups(context)
        next_matchup = next(
            (
                b for b in matchups
                if winner
                and winner in {
                    str(self._typed(b).get("away_team") or "").strip(),
                    str(self._typed(b).get("home_team") or "").strip(),
                }
            ),
            None,
        )
        sentence = (
            f"{winner or 'The winner'} controlled the flow of the game too: a {margin}-point spread inside a {total}-point night says this was about leverage over long stretches, "
            "not a lucky closeout in the final minute."
        )
        if next_matchup:
            mtf = self._typed(next_matchup)
            sentence += (
                f" That now feeds directly into {mtf.get('away_team', 'the next opponent')} at {mtf.get('home_team', 'the next site')}, "
                "where the same rotation and seeding questions come back immediately."
            )
        elif loser:
            sentence += f" For {loser}, the reset point is whether the next game looks more like an adjustment or the start of a slide."
        return sentence

    def _context_section(self, primary: list[dict[str, Any]], context: list[dict[str, Any]], teams: list[str], players: list[str]) -> list[str]:
        lines: list[str] = []
        for trend in [b for b in context if b.get("blob_type") == "team_trend"][:5]:
            name = self._clean_token(self._typed(trend).get("name"))
            if name and name not in teams:
                teams.append(name)
            trend_line = self._trend_line(trend)
            if not name or not trend_line:
                continue
            lines.append(f"{name} enters the next stretch with {trend_line}.")
            lines.append(
                f"It shows whether {name or 'this team'} is stacking enough stable possessions and lineup consistency to protect its place in the conference picture."
            )
        for blob in [b for b in context if b.get("blob_type") == "player_line"][:3]:
            line = stat_line_from_blob(blob)
            if not line:
                continue
            player = str(self._typed(blob).get("player_name", "")).strip()
            if player:
                players.append(player)
            lines.append(f"On the player side, {line} gave the night a usable star-level reference point instead of leaving the result abstract.")
        return lines[:8]

    def _forward_look(self, primary: list[dict[str, Any]], context: list[dict[str, Any]]) -> list[str]:
        lines: list[str] = []
        for matchup in self._matchups(context)[:3]:
            tf = self._typed(matchup)
            away_team = self._clean_token(tf.get("away_team")) or "the road side"
            home_team = self._clean_token(tf.get("home_team")) or "the home side"
            away_record = self._clean_token(tf.get("away_record"))
            home_record = self._clean_token(tf.get("home_record"))
            record_clause = f", with {away_record} against {home_record}," if away_record and home_record else ","
            lines.append(
                f"The next checkpoint is {away_team} at {home_team}{record_clause} and another chance to either firm up or damage a playoff track."
            )
            lines.append(
                "The next slate should show whether the teams involved can confirm the signal they just sent."
            )
        if not lines:
            lines.append("The next NBA window carries playoff-positioning weight because strong February and March results stop looking isolated.")
            lines.append("From here, the question is which team can carry that form forward before the standings harden further.")
        return lines
