from __future__ import annotations

from datetime import date
from typing import Any

from app.services.providers.base_provider import BaseSportProvider, ProviderConfig
from app.services.providers.common import make_blob_id


class NFLProvider(BaseSportProvider):
    def __init__(self) -> None:
        super().__init__(ProviderConfig(sport="nfl", league_match=lambda l: l.upper().strip() == "NFL"))

    def build(self, run_date: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out = super().build(run_date, rows)
        if not self._in_combine_window(run_date):
            return out

        has_combine = False
        for blob in out:
            if str(blob.get("blob_type", "")) != "news":
                continue
            tf = ((blob.get("facts") or {}).get("typed_fields") or {})
            text = f"{str(tf.get('headline', ''))} {str(tf.get('summary', ''))}".lower()
            if "combine" in text or "pro day" in text:
                has_combine = True
                break
        if has_combine:
            return out

        seed = f"nfl|news|combine_fallback|{run_date}"
        out.append(
            {
                "blob_id": make_blob_id(seed),
                "sport": "nfl",
                "league": "NFL",
                "blob_type": "news",
                "event_time_et": f"{run_date}T12:00:00-05:00",
                "window": "season_to_date",
                "entities": {"game_id": None, "team_ids": [], "player_ids": []},
                "facts": {
                    "typed_fields": {
                        "headline": "NFL Combine week: drills, testing, and draft stock movement",
                        "summary": "The NFL combine testing window in Indianapolis is active this week, with measurable results expected to reshape early draft boards before pro-day season.",
                        "severity": "high",
                        "fact_points": [
                            "Combine testing window is active in Indianapolis this week.",
                            "Results directly influence early draft-board movement.",
                            "Teams use combine data to prioritize follow-up pro-day evaluations.",
                        ],
                        "summary_word_count": 27,
                    }
                },
                "labels": self._common_labels("combine", 0.75, "OFFSEASON"),
                "scoring": {
                    "base_weight": 0.0,
                    "phase_multiplier": 0.0,
                    "recency_multiplier": 0.0,
                    "impact_multiplier": 0.0,
                    "uniqueness_multiplier": 0.0,
                    "blob_type_multiplier": 0.0,
                    "final_priority_score": 0.0,
                },
                "provenance": {
                    "source_ids": ["https://www.nfl.com/combine/"],
                    "retrieved_at_et": f"{run_date}T12:00:00-05:00",
                },
            }
        )
        return out

    def _news_blob(self, row: dict[str, Any], run_date: str, phase: str) -> dict[str, Any] | None:
        blob = super()._news_blob(row, run_date, phase)
        if not blob:
            return None
        tf = ((blob.get("facts") or {}).get("typed_fields") or {})
        title = str(tf.get("headline", "")).lower()
        summary = str(tf.get("summary", "")).lower()
        text = f"{title} {summary}"
        if "combine" in text or "pro day" in text:
            blob["labels"]["category"] = "combine"
            blob["labels"]["importance"] = "high"
        elif "draft" in text:
            blob["labels"]["category"] = "draft"
        elif "free agency" in text or "contract" in text or "tag" in text:
            blob["labels"]["category"] = "free_agency"
        return blob

    @staticmethod
    def _in_combine_window(run_date: str) -> bool:
        try:
            d = date.fromisoformat(run_date)
        except Exception:
            return False
        return (d.month == 2 and d.day >= 20) or (d.month == 3 and d.day <= 10)
