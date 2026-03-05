from __future__ import annotations

from app.services.providers.base_provider import BaseSportProvider, ProviderConfig


class CollegeBasketballProvider(BaseSportProvider):
    def __init__(self) -> None:
        super().__init__(ProviderConfig(sport="college_basketball", league_match=lambda l: l.upper().strip() in {"NCAA", "NCAAM", "NCAAB"}))
