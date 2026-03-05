from typing import Any

from app.config import get_settings
from app.services.agents.qa_rubric import RuntimeBand, estimate_runtime_minutes, runtime_status
from app.services.agents.transcript_builder import analyze_script_text
from app.services.agents.verifier import compute_verification_metrics
from app.storage import ObjectStore

SOFT_ISSUES = {
    "runtime_soft_fail",
    "repetitive_sentence_starters_detected",
}


def _effective_min_turns(cfg: Any, spoken_words: int) -> int:
    # Avoid over-constraining long-form scripts with naturally longer turns.
    dynamic_floor = max(60, int(spoken_words / 55))
    return min(cfg.transcript_min_host_turns, dynamic_floor)


def _effective_repetitive_starter_limit(cfg: Any, host_turns: int) -> int:
    # Absolute threshold is too strict for long scripts; allow moderate repetition
    # proportional to turn count while preserving the configured floor.
    return max(cfg.transcript_max_repetitive_starter_hits, int(host_turns * 0.22))


def _detailed_report(
    run_date: str,
    transcript: dict[str, Any],
    briefpack: dict[str, Any],
    verification: dict[str, Any],
) -> dict[str, Any]:
    available = set(briefpack.get("citations", []))
    cited = [c for seg in transcript.get("segments", []) for c in seg.get("citations", [])]
    cited_set = set(cited)
    missing_citations = sorted(cited_set - available)
    used_citations = sorted(cited_set & available)

    return {
        "run_date": run_date,
        "approved": bool(verification.get("approved", False)),
        "issues": verification.get("issues", []),
        "citation_coverage": float(verification.get("citation_coverage", 0.0)),
        "numeric_fidelity": float(verification.get("numeric_fidelity", 0.0)),
        "segment_count": len(transcript.get("segments", [])),
        "spoken_word_count": int(transcript.get("spoken_word_count", 0)),
        "estimated_minutes": float(transcript.get("estimated_minutes", 0.0)),
        "host_turn_count": int(transcript.get("host_turn_count", 0)),
        "unique_hosts": transcript.get("unique_hosts", []),
        "robotic_phrase_hits": int(transcript.get("robotic_phrase_hits", 0)),
        "final_score_phrase_hits": int(transcript.get("final_score_phrase_hits", 0)),
        "repetitive_starter_hits": int(transcript.get("repetitive_starter_hits", 0)),
        "available_citation_count": len(available),
        "used_citation_count": len(used_citations),
        "missing_citation_count": len(missing_citations),
        "missing_citations": missing_citations,
        "llm_degraded_mode": bool(transcript.get("llm_degraded_mode", False)),
        "llm_fallback_count": int(transcript.get("llm_fallback_count", 0)),
        "transcript_verified_flag": bool(transcript.get("verified", False)),
    }


def run(run_date: str) -> dict[str, Any]:
    store = ObjectStore()
    cfg = get_settings()
    briefpack = store.get_json(f"briefpacks/{run_date}/briefpack.json")
    transcript = store.get_json(f"transcripts/{run_date}/transcript.json")

    verification = compute_verification_metrics({"segments": transcript["segments"]}, briefpack)
    metrics = analyze_script_text(transcript.get("script_text", ""))
    runtime_minutes = estimate_runtime_minutes(metrics["spoken_word_count"], cfg.transcript_wpm_baseline)
    min_turns_required = _effective_min_turns(cfg, metrics["spoken_word_count"])
    repetitive_starter_limit = _effective_repetitive_starter_limit(cfg, metrics["host_turn_count"])
    band = RuntimeBand(
        cfg.transcript_runtime_target_min,
        cfg.transcript_runtime_target_max,
        cfg.transcript_runtime_soft_min,
        cfg.transcript_runtime_soft_max,
    )
    issues = list(verification.get("issues", []))
    if metrics["spoken_word_count"] < cfg.transcript_min_words:
        issues.append("spoken_length_below_minimum")
    if metrics["host_turn_count"] < min_turns_required:
        issues.append("host_turn_count_below_minimum")
    if metrics["unique_host_count"] < 2:
        issues.append("insufficient_host_variety")
    script_text = transcript.get("script_text", "")
    if "Evan Cole:" not in script_text or "Marcus Reed:" not in script_text:
        issues.append("locked_host_names_missing")
    if metrics["robotic_phrase_hits"] > cfg.transcript_max_robotic_phrase_hits:
        issues.append("robotic_phrase_density_high")
    if metrics["final_score_phrase_hits"] > cfg.transcript_max_final_score_hits:
        issues.append("scoreboard_list_reading_detected")
    if metrics["repetitive_starter_hits"] > repetitive_starter_limit:
        issues.append("repetitive_sentence_starters_detected")
    state = runtime_status(runtime_minutes, band)
    if state == "hard":
        issues.append("runtime_hard_fail")
    elif state == "soft":
        issues.append("runtime_soft_fail")

    verification["issues"] = issues
    verification["required_fixes"] = issues
    hard_issues = [issue for issue in issues if issue not in SOFT_ISSUES]
    verification["hard_issues"] = hard_issues
    verification["approved"] = len(hard_issues) == 0
    verification["status"] = "pass" if verification["approved"] else "fail"
    report = _detailed_report(run_date, transcript, briefpack, verification)
    report["runtime_minutes_145wpm"] = round(runtime_minutes, 1)
    report["min_turns_required"] = min_turns_required
    report["repetitive_starter_limit"] = repetitive_starter_limit

    store.put_json(f"transcripts/{run_date}/verification_detailed.json", report)
    return report
