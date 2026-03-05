from __future__ import annotations

from typing import Any

from app.services.outputs.article.base import BaseLeagueArticleBuilder


class NHLArticleBuilder(BaseLeagueArticleBuilder):
    sport = "nhl"
    section_label = "Standings Context"

    def _lede(self, run_date: str, storyset: dict[str, Any], primary: list[dict[str, Any]], context: list[dict[str, Any]]) -> list[str]:
        lead = primary[0]
        tf = self._typed(lead)
        if storyset.get("article_type") == "news_focus":
            headline = self._clean_token(tf.get("headline") or tf.get("title"))
            summary = self._clean_token(tf.get("summary"))
            return [
                f"On {run_date}, the NHL file centered on {headline if headline else 'the lead report'}, not because it filled space but because it changes how the next stretch of the season needs to be read.",
                f"{summary if summary else 'The reporting window produced a development strong enough to move trade, injury, or roster expectations immediately.'}",
                "That is the useful frame from here: not abstract buzz, but which teams now have to answer the change on the ice and in the standings race.",
            ]
        winner = self._clean_token(tf.get("winner") or tf.get("home_team"))
        loser = self._clean_token(tf.get("loser") or tf.get("away_team"))
        margin = int(tf.get("margin", 0) or 0)
        return [
            f"On {run_date}, the NHL board narrowed to one lead result: {storyset['central_thesis']}",
            f"The headline game was {winner} over {loser} by {margin} goals, but the stronger question was how that margin altered the standings picture and the pressure on the next few games.",
            "From there, the article follows the top result, the nearby form lines, and the next spots where that signal can hold or fade.",
        ]

    def _impact_sentence(self, blob: dict[str, Any], context: list[dict[str, Any]]) -> str:
        if blob.get("blob_type") == "news":
            return super()._impact_sentence(blob, context)
        tf = self._typed(blob)
        winner = self._clean_token(tf.get("winner") or tf.get("home_team"))
        trend = self._trend_for_team(winner, context)
        line = self._trend_line(trend)
        if line and winner:
            return f"{winner} now carries {line}, which gives the result clearer standing beyond a single night."
        return f"{winner or 'The winner'} did enough to turn one game into a usable standings signal."

    def _game_shape_sentence(self, blob: dict[str, Any], context: list[dict[str, Any]]) -> str:
        tf = self._typed(blob)
        outcome = self._resolved_game_outcome(blob)
        winner = outcome["winner"] or outcome["home_team"]
        loser = outcome["loser"] or outcome["away_team"]
        margin = int(tf.get("margin", 0) or 0)
        total_goals = sum(int(tf.get(key, 0) or 0) for key in ("home_score", "away_score"))
        if margin == 0:
            return (
                f"The game finished level inside a {total_goals}-goal profile, so the more useful takeaway is which club carries the steadier form into the next date."
            )
        sentence = (
            f"A {margin}-goal gap inside a {total_goals}-goal game shows {winner or 'the winner'} had control long enough to keep the night from turning into a late coin flip."
        )
        if loser:
            sentence += f" For {loser}, the next game should show whether this was a short slide or a larger form problem."
        return sentence
