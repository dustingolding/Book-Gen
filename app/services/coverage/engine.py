from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import yaml


@dataclass(frozen=True)
class SportState:
    sport: str
    phase: str
    base_weight: float
    seasonal_multiplier: float
    news_multiplier: float
    recency_factor: float
    coverage_score: float
    offseason: bool


@dataclass(frozen=True)
class CoverageAllocation:
    sport: str
    phase: str
    score: float
    share: float
    minutes: float
    word_budget: int
    caps_applied: bool


def load_yaml(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _in_range(d: date, start: str, end: str) -> bool:
    return _parse_date(start) <= d <= _parse_date(end)


def determine_phase(sport: str, seasonality: dict[str, Any], on_date: date) -> str:
    cfg = seasonality.get(sport, {})
    if not cfg:
        return "OFFSEASON"
    if "championship_window" in cfg and _in_range(on_date, cfg["championship_window"]["start"], cfg["championship_window"]["end"]):
        return "CHAMPIONSHIP_WINDOW"
    if "postseason" in cfg and _in_range(on_date, cfg["postseason"]["start"], cfg["postseason"]["end"]):
        return "POSTSEASON"
    if "regular_season" in cfg and _in_range(on_date, cfg["regular_season"]["start"], cfg["regular_season"]["end"]):
        return "REGULAR_SEASON"
    if "preseason" in cfg and _in_range(on_date, cfg["preseason"]["start"], cfg["preseason"]["end"]):
        return "PRESEASON"
    return "OFFSEASON"


def _recency_activity_factor(game_count: int, buckets_cfg: dict[str, float]) -> float:
    if game_count <= 0:
        return float(buckets_cfg.get("0", 0.3))
    if 1 <= game_count <= 3:
        return float(buckets_cfg.get("1-3", 0.8))
    if 4 <= game_count <= 10:
        return float(buckets_cfg.get("4-10", 1.0))
    return float(buckets_cfg.get("11+", 1.2))


def compute_sport_states(
    *,
    on_date: date,
    seasonality_cfg: dict[str, Any],
    weights_cfg: dict[str, Any],
    game_counts_yesterday: dict[str, int],
    big_news_flags: dict[str, bool],
    major_international_event_flag: bool,
) -> list[SportState]:
    sports_cfg = weights_cfg["sports"]
    phase_mult = weights_cfg["phase_multipliers"]
    news_mult_cfg = weights_cfg.get("news_multiplier", {"enabled": True, "value": 1.5})
    recency_cfg = weights_cfg["recency_activity_factor"]["buckets"]

    states: list[SportState] = []
    for sport, spec in sports_cfg.items():
        base = float(spec["base_weight"])
        phase = determine_phase(sport, seasonality_cfg, on_date)
        if sport == "major_international" and not major_international_event_flag and not big_news_flags.get(sport, False):
            phase = "OFFSEASON"

        seasonal = float(phase_mult.get(phase, 0.25))
        offseason = phase == "OFFSEASON"
        news_multiplier = 1.0
        if news_mult_cfg.get("enabled", True) and offseason and big_news_flags.get(sport, False):
            news_multiplier = float(news_mult_cfg.get("value", 1.5))

        recency = _recency_activity_factor(int(game_counts_yesterday.get(sport, 0)), recency_cfg)
        score = base * seasonal * news_multiplier * recency
        states.append(
            SportState(
                sport=sport,
                phase=phase,
                base_weight=base,
                seasonal_multiplier=seasonal,
                news_multiplier=news_multiplier,
                recency_factor=recency,
                coverage_score=score,
                offseason=offseason,
            )
        )

    states.sort(key=lambda x: x.coverage_score, reverse=True)
    return states


def allocate_show_time(
    *,
    sport_states: list[SportState],
    weights_cfg: dict[str, Any],
    runtime_minutes_target: float,
) -> list[CoverageAllocation]:
    caps = weights_cfg.get("offseason_caps", {"max_show_share": 0.08, "max_show_share_big_news": 0.25})
    wpm = int(weights_cfg["show_rules"]["wpm_baseline"])
    total_score = sum(s.coverage_score for s in sport_states) or 1.0

    allocations: list[CoverageAllocation] = []
    for s in sport_states:
        share = s.coverage_score / total_score
        minutes = share * runtime_minutes_target
        allocations.append(
            CoverageAllocation(
                sport=s.sport,
                phase=s.phase,
                score=s.coverage_score,
                share=share,
                minutes=minutes,
                word_budget=int(minutes * wpm),
                caps_applied=False,
            )
        )

    freed_share = 0.0
    adjusted: list[CoverageAllocation] = []
    for a, s in zip(allocations, sport_states):
        if s.offseason:
            max_share = float(caps["max_show_share_big_news"] if s.news_multiplier > 1.0 else caps["max_show_share"])
            if a.share > max_share:
                freed_share += a.share - max_share
                minutes = max_share * runtime_minutes_target
                a = CoverageAllocation(
                    sport=a.sport,
                    phase=a.phase,
                    score=a.score,
                    share=max_share,
                    minutes=minutes,
                    word_budget=int(minutes * wpm),
                    caps_applied=True,
                )
        adjusted.append(a)

    if freed_share > 0:
        in_season_idx = [i for i, s in enumerate(sport_states) if not s.offseason]
        denom = sum(adjusted[i].share for i in in_season_idx) or 1.0
        redistributed: list[CoverageAllocation] = []
        for i, a in enumerate(adjusted):
            if i in in_season_idx:
                bump = freed_share * (a.share / denom)
                share = a.share + bump
                minutes = share * runtime_minutes_target
                a = CoverageAllocation(
                    sport=a.sport,
                    phase=a.phase,
                    score=a.score,
                    share=share,
                    minutes=minutes,
                    word_budget=int(minutes * wpm),
                    caps_applied=a.caps_applied,
                )
            redistributed.append(a)
        adjusted = redistributed

    total_share = sum(a.share for a in adjusted) or 1.0
    normalized: list[CoverageAllocation] = []
    for a in adjusted:
        share = a.share / total_share
        minutes = share * runtime_minutes_target
        normalized.append(
            CoverageAllocation(
                sport=a.sport,
                phase=a.phase,
                score=a.score,
                share=share,
                minutes=minutes,
                word_budget=int(minutes * wpm),
                caps_applied=a.caps_applied,
            )
        )

    max_int_share = float(weights_cfg.get("show_rules", {}).get("max_major_international_share", 0.0) or 0.0)
    if max_int_share > 0:
        int_idx = next((i for i, a in enumerate(normalized) if a.sport == "major_international"), None)
        if int_idx is not None and normalized[int_idx].share > max_int_share:
            excess = normalized[int_idx].share - max_int_share
            capped_minutes = max_int_share * runtime_minutes_target
            normalized[int_idx] = CoverageAllocation(
                sport=normalized[int_idx].sport,
                phase=normalized[int_idx].phase,
                score=normalized[int_idx].score,
                share=max_int_share,
                minutes=capped_minutes,
                word_budget=int(capped_minutes * wpm),
                caps_applied=True,
            )
            eligible_idx = [i for i, a in enumerate(normalized) if i != int_idx and a.sport != "major_international"]
            denom = sum(normalized[i].share for i in eligible_idx) or 1.0
            for i in eligible_idx:
                bump = excess * (normalized[i].share / denom)
                share = normalized[i].share + bump
                minutes = share * runtime_minutes_target
                normalized[i] = CoverageAllocation(
                    sport=normalized[i].sport,
                    phase=normalized[i].phase,
                    score=normalized[i].score,
                    share=share,
                    minutes=minutes,
                    word_budget=int(minutes * wpm),
                    caps_applied=normalized[i].caps_applied,
                )

    normalized.sort(key=lambda x: x.share, reverse=True)
    return normalized
