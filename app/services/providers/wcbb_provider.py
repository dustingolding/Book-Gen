from __future__ import annotations

from app.services.providers.base_provider import BaseSportProvider, ProviderConfig


class WomensCollegeBasketballProvider(BaseSportProvider):
    def __init__(self) -> None:
        super().__init__(ProviderConfig(sport="womens_college_basketball", league_match=lambda l: l.upper().strip() in {"NCAAW", "WNCAA", "WNCAAB"}))
