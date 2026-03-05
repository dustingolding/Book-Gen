from app.services.providers.base_provider import BaseSportProvider, ProviderConfig


class WNBAProvider(BaseSportProvider):
    def __init__(self) -> None:
        super().__init__(ProviderConfig(sport="wnba", league_match=lambda l: l.upper().strip() == "WNBA"))
