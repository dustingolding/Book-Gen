from app.services.providers.base_provider import BaseSportProvider, ProviderConfig


class NHLProvider(BaseSportProvider):
    def __init__(self) -> None:
        super().__init__(
            ProviderConfig(
                sport="nhl",
                league_match=lambda l: str(l or "").upper().strip() == "NHL",
            )
        )
