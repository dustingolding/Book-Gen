from __future__ import annotations

from app.services.outputs.article.base import BaseLeagueArticleBuilder


class InternationalArticleBuilder(BaseLeagueArticleBuilder):
    sport = "major_international"
    section_label = "Global Context"
