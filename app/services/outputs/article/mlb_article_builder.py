from __future__ import annotations

from typing import Any

from app.services.outputs.article.base import BaseLeagueArticleBuilder
from app.services.outputs.article.common import stat_line_from_blob


class MLBArticleBuilder(BaseLeagueArticleBuilder):
    sport = "mlb"
    section_label = "Rotation and Series Context"

    def _lede(self, run_date: str, storyset: dict[str, Any], primary: list[dict[str, Any]], context: list[dict[str, Any]]) -> list[str]:
        lead = primary[0]
        tf = self._typed(lead)
        if storyset.get("article_type") == "news_focus":
            headline = self._clean_token(tf.get("headline") or tf.get("title"))
            summary = self._clean_token(tf.get("summary"))
            return [
                f"On {run_date}, the MLB file turned on one roster or camp decision: {headline if headline else 'the lead report'} moved from background noise to a usable story.",
                f"{summary if summary else 'The reporting window produced a move that changes how the next round of roster and rotation decisions should be read.'}",
                "What matters from there is which club has to alter its next starter, bench mix, or bullpen plan because of the update.",
            ]
        outcome = self._resolved_game_outcome(lead)
        winner = outcome["winner"] or outcome["home_team"]
        loser = outcome["loser"] or outcome["away_team"]
        margin = int(tf.get("margin", 0) or 0)
        return [
            f"On {run_date}, the MLB slate centered on one clear result: {storyset['central_thesis']}",
            f"The lead game was {winner} over {loser} by {margin} runs, and the score matters because it gives a cleaner read on current form, pitching stability, and which club is carrying real momentum into the next series.",
            "The rest of the article tracks the top scoreline, the surrounding trend signals, and the next spots where those conclusions can hold or break.",
        ]

    def _impact_sentence(self, blob: dict[str, Any], context: list[dict[str, Any]]) -> str:
        if blob.get("blob_type") == "news":
            return super()._impact_sentence(blob, context)
        tf = self._typed(blob)
        winner = self._clean_token(tf.get("winner") or tf.get("home_team"))
        loser = self._clean_token(tf.get("loser") or tf.get("away_team"))
        trend = self._trend_for_team(winner, context)
        line = self._trend_line(trend)
        if line and winner:
            return f"{winner} now carries {line}, so this win fits into a broader run instead of reading like a one-day spike."
        if winner and loser:
            return f"{winner} put cleaner innings together than {loser}, and the margin held long enough to matter beyond one swing."
        return "The score matters because one club created real separation instead of surviving a coin-flip finish."

    def _game_shape_sentence(self, blob: dict[str, Any], context: list[dict[str, Any]]) -> str:
        tf = self._typed(blob)
        outcome = self._resolved_game_outcome(blob)
        winner = outcome["winner"] or outcome["home_team"]
        loser = outcome["loser"] or outcome["away_team"]
        margin = int(tf.get("margin", 0) or 0)
        total_runs = sum(int(tf.get(key, 0) or 0) for key in ("home_score", "away_score"))
        if margin == 0:
            return (
                f"The game finished level inside a {total_runs}-run profile, so the better signal is whether either club carries sharper form into the next series."
            )
        sentence = (
            f"A {margin}-run margin inside a {total_runs}-run game suggests {winner or 'the winner'} got enough out of both the mound plan and the middle innings to stay clear of a late scramble."
        )
        if loser:
            sentence += f" For {loser}, the next game should show whether this was a brief drift or the start of a rougher stretch."
        return sentence

    def _secondary_signals(self, storyset: dict[str, Any], context: list[dict[str, Any]], teams: list[str], players: list[str]) -> list[str]:
        lines: list[str] = []
        primary_ids = set(storyset.get("primary_blobs") or [])
        lead_games = [b for b in context if str(b.get("blob_id")) in primary_ids and b.get("blob_type") == "game_result"]
        if not lead_games:
            lead_games = [b for b in context if b.get("blob_type") == "game_result"]
        for blob in lead_games[:2]:
            outcome = self._resolved_game_outcome(blob)
            winner = outcome["winner"] or outcome["home_team"]
            loser = outcome["loser"] or outcome["away_team"]
            winner_trend = self._trend_line(self._trend_for_team(winner, context))
            loser_trend = self._trend_line(self._trend_for_team(loser, context))
            if winner and winner_trend:
                teams.append(winner)
                lines.append(f"{winner} also enters the next series with {winner_trend}, which keeps the lead result from standing on one box score alone.")
            if loser and loser_trend:
                teams.append(loser)
                lines.append(f"{loser} counters that with {loser_trend}, so the next two or three games matter more than the single final line.")
        for blob in [b for b in context if b.get("blob_type") == "team_trend"][:3]:
            name = self._clean_token(self._typed(blob).get("name"))
            trend_line = self._trend_line(blob)
            if not name or not trend_line:
                continue
            teams.append(name)
            lines.append(f"A nearby club worth tracking is {name}, which comes in with {trend_line}.")
            lines.append(f"That matters because MLB form is easier to trust when it survives different starters, different parks, and the grind of a full series.")
        for blob in [b for b in context if b.get("blob_type") == "player_line"][:3]:
            line = stat_line_from_blob(blob)
            if not line:
                continue
            player = self._clean_token(self._typed(blob).get("player_name"))
            if player:
                players.append(player)
            lines.append(f"One supporting stat line came from {line}, which helps explain how the run environment actually tilted.")
        for news in self._news(context)[:2]:
            headline = self._clean_token(self._typed(news).get("headline") or self._typed(news).get("title"))
            summary = self._clean_token(self._typed(news).get("summary"))
            if headline and summary:
                lines.append(f"{headline} stayed relevant because {summary}")
        return lines[:12]

    def _context_section(self, primary: list[dict[str, Any]], context: list[dict[str, Any]], teams: list[str], players: list[str]) -> list[str]:
        lines: list[str] = []
        if primary:
            lead = primary[0]
            outcome = self._resolved_game_outcome(lead)
            winner = outcome["winner"] or outcome["home_team"]
            loser = outcome["loser"] or outcome["away_team"]
            winner_trend = self._trend_line(self._trend_for_team(winner, context))
            loser_trend = self._trend_line(self._trend_for_team(loser, context))
            if winner and winner_trend:
                teams.append(winner)
                lines.append(f"{winner} carries {winner_trend} into the next series, which is the kind of profile that makes one result easier to trust.")
            if loser and loser_trend:
                teams.append(loser)
                lines.append(f"{loser} answers with {loser_trend}, so the losing side still has a measurable baseline for the next two dates.")
        for trend in [b for b in context if b.get("blob_type") == "team_trend"][:5]:
            name = self._clean_token(self._typed(trend).get("name"))
            if name and name not in teams:
                teams.append(name)
            trend_line = self._trend_line(trend)
            if not name or not trend_line:
                continue
            lines.append(f"{name} enters the next series with {trend_line}.")
            lines.append(
                f"That line matters because MLB results only gain weight when the same club keeps producing them across multiple pitching looks and multiple nights."
            )
        for blob in [b for b in context if b.get("blob_type") == "player_line"][:4]:
            line = stat_line_from_blob(blob)
            if not line:
                continue
            player = self._clean_token(self._typed(blob).get("player_name"))
            if player:
                players.append(player)
            lines.append(f"On the player side, {line} gave the game a usable performance marker instead of leaving the box score flat.")
        for news in self._news(context)[:3]:
            tf = self._typed(news)
            headline = self._clean_token(tf.get("headline"))
            summary = self._clean_token(tf.get("summary"))
            if headline and summary:
                lines.append(f"{headline} still matters because {summary}")
        return lines[:14]

    def _forward_look(self, primary: list[dict[str, Any]], context: list[dict[str, Any]]) -> list[str]:
        lines: list[str] = []
        for matchup in self._matchups(context)[:4]:
            tf = self._typed(matchup)
            away_team = self._clean_token(tf.get("away_team")) or "the road side"
            home_team = self._clean_token(tf.get("home_team")) or "the home side"
            away_record = self._clean_token(tf.get("away_record"))
            home_record = self._clean_token(tf.get("home_record"))
            if away_record and home_record:
                lines.append(
                    f"The next series checkpoint is {away_team} at {home_team}, with {away_record} meeting {home_record} and another chance to confirm which club is carrying cleaner form."
                )
            else:
                away_trend = self._trend_line(self._trend_for_team(away_team, context))
                home_trend = self._trend_line(self._trend_for_team(home_team, context))
                if away_trend or home_trend:
                    trend_bits = "; ".join(bit for bit in (f"{away_team} brings {away_trend}" if away_trend else "", f"{home_team} brings {home_trend}" if home_trend else "") if bit)
                    lines.append(f"The next series checkpoint is {away_team} at {home_team}. {trend_bits}.")
                else:
                    lines.append(
                        f"The next series checkpoint is {away_team} at {home_team}, where the latest result gets tested against a fresh pitching setup."
                    )
            lines.append(
                "The next MLB window should show whether these clubs are building something durable or just living off one sharp night."
            )
        if not lines:
            if primary:
                lead = primary[0]
                outcome = self._resolved_game_outcome(lead)
                winner = outcome["winner"] or outcome["home_team"] or "the winning club"
                loser = outcome["loser"] or outcome["away_team"] or "the other side"
                lines.append(f"The first follow-up question is whether {winner} can carry the same shape into the next series or whether {loser} answers quickly.")
                lines.append(f"That matters because early MLB momentum only becomes useful when it survives a new starter, a new bullpen plan, and a different park context.")
            lines.append("The next MLB window matters because series momentum only counts when it survives a new starter, a new bullpen game, or a new travel spot.")
            lines.append("The next few dates should show which results were real signals and which ones were only one-night swings.")
        return lines
