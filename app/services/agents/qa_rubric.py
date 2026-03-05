# services/agents/qa_rubric.py

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RuntimeBand:
    target_min: float = 30.0
    target_max: float = 45.0
    soft_min: float = 28.0
    soft_max: float = 47.0


def estimate_runtime_minutes(word_count: int, wpm: int = 145) -> float:
    return float(word_count) / float(wpm)


def runtime_status(minutes: float, band: RuntimeBand) -> str:
    if band.target_min <= minutes <= band.target_max:
        return "target"
    if band.soft_min <= minutes <= band.soft_max:
        return "soft"
    return "hard"
