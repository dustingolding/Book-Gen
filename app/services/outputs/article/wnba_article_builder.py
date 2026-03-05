from __future__ import annotations

from app.services.outputs.article.base import BaseLeagueArticleBuilder


class WNBAArticleBuilder(BaseLeagueArticleBuilder):
    sport = "wnba"
    section_label = "WNBA Context"
