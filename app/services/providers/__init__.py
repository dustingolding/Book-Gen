from app.services.providers.cbb_provider import CollegeBasketballProvider
from app.services.providers.cfb_provider import CollegeFootballProvider
from app.services.providers.international_provider import MajorInternationalProvider
from app.services.providers.mlb_provider import MLBProvider
from app.services.providers.nba_provider import NBAProvider
from app.services.providers.nfl_provider import NFLProvider
from app.services.providers.nhl_provider import NHLProvider
from app.services.providers.wcbb_provider import WomensCollegeBasketballProvider
from app.services.providers.wnba_provider import WNBAProvider


def provider_registry():
    return [
        CollegeFootballProvider(),
        NFLProvider(),
        CollegeBasketballProvider(),
        NBAProvider(),
        MLBProvider(),
        WomensCollegeBasketballProvider(),
        WNBAProvider(),
        NHLProvider(),
        MajorInternationalProvider(),
    ]
