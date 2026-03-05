import json
from typing import Any

from .prompt_loader import load_prompt

WPM_BASELINE = 145
EXPECTED_SEGMENTS = {
    "cold_open",
    "scoreboard_roundup",
    "major_storylines",
    "deep_dive_games",
    "today_matchups",
    "closing",
}
OPTIONAL_SEGMENTS = {"optional_international"}
DEFAULT_SECTION_BUDGETS: dict[str, dict[str, int]] = {
    "cold_open": {"min_words": 400, "max_words": 600},
    "scoreboard_roundup": {"min_words": 1600, "max_words": 2200},
    "major_storylines": {"min_words": 1300, "max_words": 1500},
    "deep_dive_games": {"min_words": 800, "max_words": 1200},
    "today_matchups": {"min_words": 800, "max_words": 1200},
    "optional_international": {"min_words": 400, "max_words": 700},
    "closing": {"min_words": 250, "max_words": 400},
}
DEFAULT_TARGET_WORDS = 6000

PLANNER_SYSTEM_PROMPT = load_prompt("planner_prompt.txt")


def planner_prompt(briefpack: dict[str, Any]) -> str:
    seasonality = (briefpack.get("flags") or {}).get("seasonality", {})
    return (
        "Create the show plan from this BriefPack JSON. "
        "Return valid JSON only."
        "\nSeasonality control:"
        "\n- If league_focus_weights exists, lean additional coverage toward boosted leagues."
        "\n- Keep balance: no single league should dominate more than half of rundown attention."
        f"\nSeasonality:\n{json.dumps(seasonality, ensure_ascii=True)}"
        f"\nBriefpack:\n{json.dumps(briefpack, ensure_ascii=True)}\n"
    )


def default_plan() -> dict[str, list[dict[str, Any]]]:
    return {
        "episode_length_target": "30-45 minutes",
        "target_word_count": DEFAULT_TARGET_WORDS,
        "wpm_baseline": WPM_BASELINE,
        "voices": 2,
        "section_word_budgets": DEFAULT_SECTION_BUDGETS,
        "sections": [
            "cold_open",
            "scoreboard_roundup",
            "major_storylines",
            "deep_dive_games",
            "today_matchups",
            "optional_international",
            "closing",
        ],
    }


def valid_plan(plan: Any) -> bool:
    if not isinstance(plan, dict):
        return False
    sections = plan.get("sections")
    if not isinstance(sections, list) or not sections:
        return False
    names = {s for s in sections if isinstance(s, str)}
    required_ok = EXPECTED_SEGMENTS.issubset(names)
    optional_ok = OPTIONAL_SEGMENTS.issubset(names) or True
    target_words = plan.get("target_word_count")
    target_ok = isinstance(target_words, int) and 4300 <= target_words <= 7000
    budgets = plan.get("section_word_budgets")
    if not isinstance(budgets, dict):
        return False
    for key in DEFAULT_SECTION_BUDGETS:
        if key not in budgets or not isinstance(budgets[key], dict):
            return False
        min_words = budgets[key].get("min_words")
        max_words = budgets[key].get("max_words")
        if not isinstance(min_words, int) or not isinstance(max_words, int) or min_words <= 0 or max_words < min_words:
            return False
    return required_ok and optional_ok and target_ok


def build_outline(llm: Any, briefpack: dict[str, Any]) -> dict[str, Any]:
    raw = llm.complete(PLANNER_SYSTEM_PROMPT, planner_prompt(briefpack))
    try:
        plan = json.loads(raw)
        if not valid_plan(plan):
            raise ValueError("invalid planner output")
        return plan
    except Exception:
        return default_plan()
