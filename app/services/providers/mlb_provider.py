from app.services.providers.base_provider import BaseSportProvider, ProviderConfig


class MLBProvider(BaseSportProvider):
    def __init__(self) -> None:
        super().__init__(ProviderConfig(sport="mlb", league_match=lambda l: "MLB" in l.upper()))
