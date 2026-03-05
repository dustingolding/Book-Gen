from __future__ import annotations

from app.services.outputs.article.base import BaseLeagueArticleBuilder


class CollegeFootballArticleBuilder(BaseLeagueArticleBuilder):
    sport = "college_football"
    section_label = "College Football Context"
