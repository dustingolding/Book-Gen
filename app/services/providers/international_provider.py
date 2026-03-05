from app.services.providers.base_provider import BaseSportProvider, ProviderConfig


class MajorInternationalProvider(BaseSportProvider):
    def __init__(self) -> None:
        super().__init__(ProviderConfig(sport="major_international", league_match=lambda l: l.upper().strip() in {"INTERNATIONAL", "MAJOR_INTERNATIONAL", "OLYMPICS", "FIFA"}))
