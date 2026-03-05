from app.services.providers.base_provider import BaseSportProvider, ProviderConfig


class CollegeFootballProvider(BaseSportProvider):
    def __init__(self) -> None:
        super().__init__(ProviderConfig(sport="college_football", league_match=lambda l: l.upper().strip() in {"NCAAF", "COLLEGE_FOOTBALL"}))
