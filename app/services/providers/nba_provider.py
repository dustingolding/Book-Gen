from __future__ import annotations

from app.services.providers.base_provider import BaseSportProvider, ProviderConfig


class NBAProvider(BaseSportProvider):
    def __init__(self) -> None:
        super().__init__(ProviderConfig(sport="nba", league_match=lambda l: "NBA" in l.upper() and "G LEAGUE" not in l.upper()))
