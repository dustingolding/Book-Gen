from __future__ import annotations

from app.services.outputs.article.cbb_article_builder import CollegeBasketballArticleBuilder
from app.services.outputs.article.cfb_article_builder import CollegeFootballArticleBuilder
from app.services.outputs.article.international_article_builder import InternationalArticleBuilder
from app.services.outputs.article.mlb_article_builder import MLBArticleBuilder
from app.services.outputs.article.nba_article_builder import NBAArticleBuilder
from app.services.outputs.article.nfl_article_builder import NFLArticleBuilder
from app.services.outputs.article.nhl_article_builder import NHLArticleBuilder
from app.services.outputs.article.wcbb_article_builder import WomensCollegeBasketballArticleBuilder
from app.services.outputs.article.wnba_article_builder import WNBAArticleBuilder


BUILDERS = {
    "college_football": CollegeFootballArticleBuilder,
    "nfl": NFLArticleBuilder,
    "college_basketball": CollegeBasketballArticleBuilder,
    "nba": NBAArticleBuilder,
    "mlb": MLBArticleBuilder,
    "womens_college_basketball": WomensCollegeBasketballArticleBuilder,
    "wnba": WNBAArticleBuilder,
    "nhl": NHLArticleBuilder,
    "major_international": InternationalArticleBuilder,
}


def build_article(*, run_date: str, storyset: dict, blob_index: dict):
    builder_cls = BUILDERS.get(str(storyset.get("sport", "")))
    if not builder_cls:
        raise KeyError(f"No article builder for sport={storyset.get('sport')}")
    return builder_cls().build(run_date=run_date, storyset=storyset, blob_index=blob_index)
