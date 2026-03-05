import json
from typing import Any

from .planner import EXPECTED_SEGMENTS
from .prompt_loader import load_prompt

WRITER_SYSTEM_PROMPT = load_prompt("writer_prompt.txt")


def writer_prompt(plan: dict[str, Any], briefpack: dict[str, Any]) -> str:
    budgets = plan.get("section_word_budgets", {})
    target_words = int(plan.get("target_word_count", 6000))
    return (
        "INPUT:\n"
        "- Show plan from Planner agent\n"
        "- BriefPack data\n\n"
        "Return valid JSON with keys: segment, text, citations.\n"
        f"Length target for this draft: {target_words} words across all segment text.\n"
        "Section text length must be within each section budget.\n"
        "Use only 1-3 citations per segment and only from BriefPack citations.\n"
        "Every segment text must include multiple Evan Cole / Marcus Reed turns.\n"
        "Never claim a game happened unless it appears in top_scores."
        f"\nSection budgets:\n{json.dumps(budgets, ensure_ascii=True)}"
        f"\nPlan:\n{json.dumps(plan, ensure_ascii=True)}"
        f"\nBriefpack:\n{json.dumps(briefpack, ensure_ascii=True)}"
    )


def default_writer_output(briefpack: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    segments: list[dict[str, Any]] = []
    scores_lines = [s["summary"] for s in briefpack["top_scores"]]
    headlines_lines = [h["title"] for h in briefpack["headlines"]]
    upcoming_lines = [u["summary"] for u in briefpack["upcoming_matchups"]]
    scores_citations = list(
        dict.fromkeys([c for item in briefpack.get("top_scores", []) for c in item.get("citations", [])])
    )[:3]
    headlines_citations = list(
        dict.fromkeys([c for item in briefpack.get("headlines", []) for c in item.get("citations", [])])
    )[:3]
    upcoming_citations = list(
        dict.fromkeys([c for item in briefpack.get("upcoming_matchups", []) for c in item.get("citations", [])])
    )[:3]

    segments.append(
        {
            "segment": "cold_open",
            "text": (
                "Evan Cole: Welcome in, this is Side Line Wire Dailycast and we're focusing on the biggest results and storylines. "
                "Marcus Reed: We'll keep it tight, conversational, and centered on what actually changed the landscape last night."
            ),
            "citations": headlines_citations[:1],
        }
    )
    segments.append(
        {
            "segment": "scoreboard_roundup",
            "text": (
                "Evan Cole: Let's start with the scoreboard that mattered. "
                + " ".join(scores_lines[:3])
                + " Marcus Reed: Those aren't just numbers, those results shift momentum and playoff pressure heading into the week."
            ),
            "citations": scores_citations,
        }
    )
    segments.append(
        {
            "segment": "major_storylines",
            "text": (
                "Evan Cole: On the major storylines, "
                + " ".join(headlines_lines[:3])
                + " Marcus Reed: The real takeaway is how those updates reshape roster decisions and postseason outlook."
            ),
            "citations": headlines_citations,
        }
    )
    segments.append(
        {
            "segment": "deep_dive_games",
            "text": (
                "Evan Cole: Let's zoom in on one or two games that were more complex than the final margin. "
                "Marcus Reed: We care about game script, adjustments, and who controlled tempo in winning time."
            ),
            "citations": scores_citations[:2],
        }
    )
    segments.append(
        {
            "segment": "today_matchups",
            "text": (
                "Evan Cole: Looking ahead to today, "
                + " ".join(upcoming_lines[:3])
                + " Marcus Reed: Those are the matchups worth your time if you're watching for playoff leverage and form."
            ),
            "citations": upcoming_citations,
        }
    )
    segments.append(
        {
            "segment": "closing",
            "text": (
                "Evan Cole: That's the show for today. "
                "Marcus Reed: Back tomorrow with the next slate and the biggest developments only."
            ),
            "citations": [],
        }
    )
    return {"segments": segments}


def valid_draft(draft: Any) -> bool:
    if not isinstance(draft, dict):
        return False
    segments = draft.get("segments")
    if not isinstance(segments, list) or not segments:
        return False

    names = set()
    non_close_citations = 0
    for seg in segments:
        if not isinstance(seg, dict):
            return False
        name = seg.get("segment")
        text = seg.get("text")
        citations = seg.get("citations")
        if not isinstance(name, str) or not isinstance(text, str) or not isinstance(citations, list):
            return False
        names.add(name)
        if name not in {"closing", "cold_open"}:
            non_close_citations += len(citations)

    return EXPECTED_SEGMENTS.issubset(names) and non_close_citations > 0


def write_segments(llm: Any, plan: dict[str, Any], briefpack: dict[str, Any]) -> dict[str, Any]:
    raw = llm.complete(WRITER_SYSTEM_PROMPT, writer_prompt(plan, briefpack))
    try:
        draft = json.loads(raw)
        if not valid_draft(draft):
            raise ValueError("invalid writer output")
        return draft
    except Exception:
        return default_writer_output(briefpack)
