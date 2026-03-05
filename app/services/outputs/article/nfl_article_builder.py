from __future__ import annotations

from typing import Any

from app.services.outputs.article.base import BaseLeagueArticleBuilder


class NFLArticleBuilder(BaseLeagueArticleBuilder):
    sport = "nfl"
    section_label = "Roster and Calendar Context"

    def _lede(self, run_date: str, storyset: dict[str, Any], primary: list[dict[str, Any]], context: list[dict[str, Any]]) -> list[str]:
        lead = primary[0]
        tf = self._typed(lead)
        headline = str(tf.get("headline") or tf.get("title") or "").strip()
        summary = str(tf.get("summary") or "").strip()
        return [
            f"On {run_date}, the NFL file centered on one practical question: how much does {headline if headline else 'the lead development'} change the league’s next decision window?",
            f"{summary if summary else 'The headline cycle carried enough substance to alter draft, roster, or free-agency leverage across the next phase of the offseason calendar.'}",
            "The offseason question is whether the headline changes which teams gain leverage before free agency, the draft, or the next round of evaluations.",
        ]

    def _news_shape_sentence(self, blob: dict[str, Any]) -> str:
        tf = self._typed(blob)
        headline = str(tf.get("headline") or tf.get("title") or "this update").strip()
        category = str((blob.get("labels") or {}).get("category") or "news").replace("_", " ")
        return (
            f"{headline} landed in the {category} cycle, where one real data point can change the order of the next calls, visits, tags, or board moves."
        )

    def _context_section(self, primary: list[dict[str, Any]], context: list[str | dict[str, Any]], teams: list[str], players: list[str]) -> list[str]:
        lines: list[str] = []
        news_blobs = [b for b in context if isinstance(b, dict) and b.get("blob_type") == "news"]
        for news in news_blobs[:5]:
            tf = self._typed(news)
            headline = str(tf.get("headline") or tf.get("title") or "").strip()
            summary = str(tf.get("summary") or "").strip()
            category = str((news.get("labels") or {}).get("category") or "news").replace("_", " ")
            if headline:
                lines.append(
                    f"{headline} sits in the {category} lane, and {summary if summary else 'teams now have to adjust the next stage of the offseason calendar around it.'}"
                )
        for matchup in self._matchups([b for b in context if isinstance(b, dict)])[:2]:
            tf = self._typed(matchup)
            lines.append(
                f"Even on an offseason-heavy week, the next checkpoint is still {tf.get('away_team', 'TBD')} at {tf.get('home_team', 'TBD')}, because roster and evaluation news never lives separately from the next game environment."
            )
        return lines[:8]

    def _forward_look(self, primary: list[dict[str, Any]], context: list[dict[str, Any]]) -> list[str]:
        lines: list[str] = []
        for news in [b for b in context if b.get("blob_type") == "news"][:3]:
            tf = self._typed(news)
            headline = str(tf.get("headline") or tf.get("title") or "").strip()
            lines.append(
                f"The next test is whether {headline if headline else 'this development'} triggers a measurable follow-on move in free agency, draft positioning, or coaching decisions."
            )
            lines.append(
                "At this point on the NFL calendar, one confirmed shift can force the rest of the market to answer it."
            )
        if not lines:
            lines.append("The next NFL window matters because offseason leverage is cumulative, and the clearest stories are the ones that force the rest of the league to adjust.")
            lines.append("The proper follow-up is to watch for who acts next, not just who produced the loudest headline first.")
        return lines
