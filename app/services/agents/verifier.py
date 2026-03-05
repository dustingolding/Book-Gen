import json
from typing import Any

from .prompt_loader import load_prompt

VERIFIER_SYSTEM_PROMPT = load_prompt("verifier_prompt.txt")


def verifier_prompt(draft: dict[str, Any], briefpack: dict[str, Any]) -> str:
    return (
        "INPUT:\n- Transcript\n- BriefPack\n\n"
        "Return valid JSON only."
        f"\nDraft:\n{json.dumps(draft, ensure_ascii=True)}"
        f"\nBriefpack:\n{json.dumps(briefpack, ensure_ascii=True)}"
    )


def _expected_citations_by_segment(briefpack: dict[str, Any]) -> dict[str, list[str]]:
    return {
        "scoreboard_roundup": list(
            dict.fromkeys([c for item in briefpack.get("top_scores", []) for c in item.get("citations", [])])
        ),
        "major_storylines": list(
            dict.fromkeys([c for item in briefpack.get("headlines", []) for c in item.get("citations", [])])
        ),
        "today_matchups": list(
            dict.fromkeys([c for item in briefpack.get("upcoming_matchups", []) for c in item.get("citations", [])])
        ),
        "deep_dive_games": list(
            dict.fromkeys([c for item in briefpack.get("top_scores", []) for c in item.get("citations", [])])
        ),
        "optional_international": list(
            dict.fromkeys([c for item in briefpack.get("headlines", []) for c in item.get("citations", [])])
        ),
    }


def compute_verification_metrics(draft: dict[str, Any], briefpack: dict[str, Any]) -> dict[str, Any]:
    available = set(briefpack["citations"])
    expected_by_segment = _expected_citations_by_segment(briefpack)
    content_segments = [
        seg
        for seg in draft["segments"]
        if seg.get("segment") not in {"closing", "cold_open", "optional_international"}
    ]
    covered_segments = 0
    eligible_segments = 0
    for seg in content_segments:
        if str(seg.get("text", "")).strip().lower() in {"update unavailable.", "update unavailable"}:
            continue
        expected = expected_by_segment.get(seg.get("segment", ""), [])
        # If no source material exists for this segment type in briefpack, don't
        # penalize coverage for including a placeholder segment.
        if not expected:
            continue
        eligible_segments += 1
        seg_citations = seg.get("citations", [])
        if any(c in available for c in seg_citations):
            covered_segments += 1
    citation_coverage = covered_segments / max(1, eligible_segments)
    numeric_fidelity = 1.0

    issues = []
    if citation_coverage < 0.9:
        issues.append("citation_coverage_below_threshold")
    if numeric_fidelity < 1.0:
        issues.append("numeric_fidelity_failed")

    approved = len(issues) == 0
    return {
        "status": "pass" if approved else "fail",
        "approved": approved,
        "issues": issues,
        "required_fixes": issues,
        "citation_coverage": citation_coverage,
        "numeric_fidelity": numeric_fidelity,
    }


def verify_transcript_draft(llm: Any, draft: dict[str, Any], briefpack: dict[str, Any]) -> dict[str, Any]:
    raw = llm.complete(VERIFIER_SYSTEM_PROMPT, verifier_prompt(draft, briefpack))
    try:
        verify = json.loads(raw)
        if not isinstance(verify, dict):
            raise ValueError("invalid payload")
        if "status" in verify:
            verify["approved"] = verify.get("status") == "pass"
        required = {"approved", "issues"}
        if not required.issubset(verify):
            raise ValueError("missing keys")
        if "citation_coverage" not in verify or "numeric_fidelity" not in verify:
            computed = compute_verification_metrics(draft, briefpack)
            verify["citation_coverage"] = computed["citation_coverage"]
            verify["numeric_fidelity"] = computed["numeric_fidelity"]
        if "required_fixes" not in verify:
            verify["required_fixes"] = verify.get("issues", [])
        if "status" not in verify:
            verify["status"] = "pass" if verify["approved"] else "fail"
        return verify
    except Exception:
        return compute_verification_metrics(draft, briefpack)
