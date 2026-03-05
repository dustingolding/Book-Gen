import json
import logging
import re
from pathlib import Path
from typing import Any

import jsonschema

from app.clients.llm import LLMClient
from app.config import get_settings
from app.services.agents import (
    TRANSCRIPT_BUILDER_SYSTEM_PROMPT,
    analyze_script_text,
    build_outline,
    build_transcript_payload,
    verify_transcript_draft,
)
from app.services.agents.verifier import compute_verification_metrics
from app.storage import ObjectStore

logger = logging.getLogger(__name__)


def _normalize_host_labels(script: str) -> str:
    text = script or ""
    text = re.sub(r"\bHost A:\s*", "Evan Cole: ", text)
    text = re.sub(r"\bHost B:\s*", "Marcus Reed: ", text)
    return text


def _extract_matchup_pair(title: str) -> tuple[str, str] | None:
    raw = (title or "").strip()
    raw = re.sub(r"^(Upcoming:\s*)", "", raw, flags=re.IGNORECASE)
    m = re.match(r"^(.*?)\s+at\s+(.*)$", raw, flags=re.IGNORECASE)
    if not m:
        return None
    away = m.group(1).strip()
    home = m.group(2).strip()
    if not away or not home:
        return None
    return (away, home)


def _norm_team_token(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (text or "").lower())


def _extract_sentence_matchup_pairs(sentence: str) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    sent = sentence or ""
    # Capture explicit matchup claims only.
    pattern = re.compile(
        r"\b([A-Za-z0-9][A-Za-z0-9 .&'/-]{1,32}?)\s+(?:at|vs\.?|versus)\s+([A-Za-z0-9][A-Za-z0-9 .&'/-]{1,32}?)\b",
        flags=re.IGNORECASE,
    )
    for away, home in pattern.findall(sent):
        away_n = _norm_team_token(away)
        home_n = _norm_team_token(home)
        if away_n and home_n and away_n != home_n:
            pairs.append((away_n, home_n))
    return pairs


def _unsupported_last_night_matchups(script: str, briefpack: dict[str, Any]) -> list[str]:
    score_pairs: set[frozenset[str]] = set()
    for item in briefpack.get("top_scores", []):
        pair = _extract_matchup_pair(item.get("title", ""))
        if not pair:
            continue
        away, home = pair
        score_pairs.add(frozenset((_norm_team_token(away), _norm_team_token(home))))

    if not score_pairs:
        return []

    sentences = re.split(r"(?<=[.!?])\s+|\n+", script or "")
    unsupported: list[str] = []
    for sent in sentences:
        lower = sent.lower()
        if "last night" not in lower and "yesterday" not in lower:
            continue

        # Ignore generic references like "last night's scores" with no explicit matchup claim.
        for away_n, home_n in _extract_sentence_matchup_pairs(sent):
            pair = frozenset((away_n, home_n))
            if pair not in score_pairs:
                unsupported.append(sent.strip())
                break
    return unsupported


def _builder_prompt(plan: dict[str, Any], draft: dict[str, Any], verify: dict[str, Any]) -> str:
    return (
        "INPUT:\n"
        "- Approved show plan\n"
        "- Writer transcript\n"
        "- Verifier feedback\n\n"
        f"Plan:\n{json.dumps(plan, ensure_ascii=True)}\n"
        f"Writer Transcript:\n{json.dumps(draft, ensure_ascii=True)}\n"
        f"Verifier Feedback:\n{json.dumps(verify, ensure_ascii=True)}\n"
    )


def _append_expansion_prompt(
    current_script: str,
    briefpack: dict[str, Any],
    min_additional_words: int,
    current_words: int,
) -> str:
    return (
        "Continue this podcast transcript by APPENDING new dialogue only.\n"
        "Do not rewrite or restate the existing transcript.\n"
        f"Current draft is about {current_words} words.\n"
        f"Append at least {min_additional_words} NEW spoken words.\n"
        "Keep factual alignment with BriefPack only. Do not invent facts.\n"
        "Use Evan Cole / Marcus Reed labels and conversational back-and-forth.\n"
        "Expand by adding deeper analysis, implications, tactical breakdowns, and host reactions.\n"
        "Do not pad with filler or repeated score recitation.\n"
        "Do not include URLs, metadata, or bullet lists.\n"
        "Existing transcript context:\n"
        f"{current_script}\n\n"
        "Return ONLY the new continuation block.\n\n"
        "BriefPack JSON (facts allowed):\n"
        f"{json.dumps(briefpack, ensure_ascii=True)}\n"
    )


def _style_rewrite_prompt(current_script: str, briefpack: dict[str, Any]) -> str:
    return (
        "Rewrite this sports podcast transcript for conversational variation and natural pacing.\n"
        "Hard constraints:\n"
        "- Keep facts aligned with BriefPack only.\n"
        "- Keep host names locked to Evan Cole and Marcus Reed.\n"
        "- Do not introduce any matchup not in BriefPack top_scores as a completed result.\n"
        "- Keep spoken word count in 4500-7000 range.\n"
        "- Do not output URLs, citations, JSON, or metadata.\n"
        "Style constraints:\n"
        "- Reduce repetitive sentence starters.\n"
        "- Avoid repetitive openers like 'Absolutely', 'Right', 'For sure', 'Exactly', 'Alright'.\n"
        "- Use varied openings, sentence lengths, and turn pacing.\n"
        "- Keep authentic disagreement/reaction without hype cliches.\n\n"
        "Current transcript:\n"
        f"{current_script}\n\n"
        "BriefPack JSON:\n"
        f"{json.dumps(briefpack, ensure_ascii=True)}\n"
    )


def _word_count(text: str) -> int:
    return len(re.findall(r"\b[\w']+\b", text or ""))


def _section_budgets(plan: dict[str, Any]) -> dict[str, dict[str, int]]:
    defaults = {
        "cold_open": {"min_words": 400, "max_words": 600},
        "scoreboard_roundup": {"min_words": 1600, "max_words": 2200},
        "major_storylines": {"min_words": 1300, "max_words": 1500},
        "deep_dive_games": {"min_words": 800, "max_words": 1200},
        "today_matchups": {"min_words": 800, "max_words": 1200},
        "optional_international": {"min_words": 400, "max_words": 700},
        "closing": {"min_words": 250, "max_words": 400},
    }
    raw = plan.get("section_word_budgets")
    if not isinstance(raw, dict):
        return defaults
    merged = defaults.copy()
    for k, v in raw.items():
        if k in merged and isinstance(v, dict):
            mn = v.get("min_words")
            mx = v.get("max_words")
            if isinstance(mn, int) and isinstance(mx, int) and mn > 0 and mx >= mn:
                merged[k] = {"min_words": mn, "max_words": mx}
    return merged


def _rewrite_section_prompt(
    section: str,
    text: str,
    min_words: int,
    max_words: int,
    briefpack: dict[str, Any],
) -> str:
    return (
        f"Rewrite and calibrate section '{section}' as podcast dialogue.\n"
        f"Target length: {min_words}-{max_words} words.\n"
        "Use Evan Cole and Marcus Reed turns, naturally conversational.\n"
        "Keep facts aligned with BriefPack only.\n"
        "No URLs, no lists, no metadata.\n"
        "Current section:\n"
        f"{text}\n\n"
        "BriefPack JSON:\n"
        f"{json.dumps(briefpack, ensure_ascii=True)}\n"
    )


def _enforce_section_budgets(
    llm: LLMClient,
    draft: dict[str, Any],
    plan: dict[str, Any],
    briefpack: dict[str, Any],
) -> dict[str, Any]:
    budgets = _section_budgets(plan)
    out_segments: list[dict[str, Any]] = []
    for seg in draft.get("segments", []):
        name = seg.get("segment", "")
        if name not in budgets:
            out_segments.append(seg)
            continue
        min_words = budgets[name]["min_words"]
        max_words = budgets[name]["max_words"]
        text = seg.get("text", "")
        for _ in range(1):
            words = _word_count(text)
            if min_words <= words <= max_words:
                break
            text = llm.complete(
                TRANSCRIPT_BUILDER_SYSTEM_PROMPT,
                _rewrite_section_prompt(name, text, min_words, max_words, briefpack),
                max_completion_tokens=2200,
                temperature=0.25,
            ).strip()
        seg = {**seg, "text": text}
        out_segments.append(seg)
    return {"segments": out_segments}


def _enforce_style_quality(
    llm: LLMClient,
    script_text: str,
    briefpack: dict[str, Any],
    cfg: Any,
) -> str:
    text = script_text
    for _ in range(2):
        metrics = analyze_script_text(text)
        if (
            metrics["robotic_phrase_hits"] <= cfg.transcript_max_robotic_phrase_hits
            and metrics["final_score_phrase_hits"] <= cfg.transcript_max_final_score_hits
            and metrics["repetitive_starter_hits"] <= cfg.transcript_max_repetitive_starter_hits
        ):
            break
        rewritten = llm.complete(
            TRANSCRIPT_BUILDER_SYSTEM_PROMPT,
            _style_rewrite_prompt(text, briefpack),
            max_completion_tokens=12000,
            temperature=0.3,
        ).strip()
        if not rewritten:
            break
        text = _normalize_host_labels(rewritten)
    return text


def _force_two_host_prompt(script_text: str, briefpack: dict[str, Any]) -> str:
    return (
        "Convert this transcript into strict two-host dialogue using ONLY these labels:\n"
        "Evan Cole:\n"
        "Marcus Reed:\n\n"
        "Hard rules:\n"
        "- Preserve facts and numbers exactly.\n"
        "- Do not add new facts.\n"
        "- Keep conversational pacing and alternating turns.\n"
        "- No URLs, bullets, or metadata.\n\n"
        "Current transcript:\n"
        f"{script_text}\n\n"
        "BriefPack JSON:\n"
        f"{json.dumps(briefpack, ensure_ascii=True)}\n"
    )


def _deterministic_two_host_relabel(script_text: str) -> str:
    lines = [ln.strip() for ln in (script_text or "").splitlines() if ln.strip()]
    # If no clear lines, split by sentence.
    if not lines:
        lines = [s.strip() for s in re.split(r"(?<=[.!?])\s+", script_text or "") if s.strip()]
    if not lines:
        return (
            "Evan Cole: Quick update while we validate the final slate and context.\n"
            "Marcus Reed: We're tightening this script for factual integrity and will continue with verified takes only."
        )

    out: list[str] = []
    speaker_cycle = ["Evan Cole", "Marcus Reed"]
    idx = 0
    for ln in lines:
        if ln.startswith("Evan Cole:") or ln.startswith("Marcus Reed:"):
            out.append(ln)
            continue
        if re.match(r"^(Host [ABC]):", ln):
            ln = _normalize_host_labels(ln)
            out.append(ln)
            continue
        speaker = speaker_cycle[idx % 2]
        out.append(f"{speaker}: {ln}")
        idx += 1
    return "\n".join(out)


def _enforce_two_hosts(llm: LLMClient, script_text: str, briefpack: dict[str, Any]) -> str:
    text = _normalize_host_labels(script_text)
    metrics = analyze_script_text(text)
    if metrics["unique_host_count"] >= 2:
        return text

    rewritten = llm.complete(
        TRANSCRIPT_BUILDER_SYSTEM_PROMPT,
        _force_two_host_prompt(text, briefpack),
        max_completion_tokens=12000,
        temperature=0.2,
    ).strip()
    if rewritten:
        text = _normalize_host_labels(rewritten)
    metrics = analyze_script_text(text)
    if metrics["unique_host_count"] >= 2:
        return text
    return _deterministic_two_host_relabel(text)


def _allowed_citations_by_segment(briefpack: dict[str, Any]) -> dict[str, list[str]]:
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
        "cold_open": list(
            dict.fromkeys([c for item in briefpack.get("headlines", []) for c in item.get("citations", [])])
        )[:1],
        "optional_international": list(
            dict.fromkeys([c for item in briefpack.get("headlines", []) for c in item.get("citations", [])])
        ),
        "closing": [],
    }


def _segment_fact_payload(segment: str, briefpack: dict[str, Any]) -> dict[str, Any]:
    if segment in {"scoreboard_roundup", "deep_dive_games"}:
        return {"top_scores": briefpack.get("top_scores", [])[:8]}
    if segment in {"major_storylines", "cold_open", "optional_international"}:
        return {"headlines": briefpack.get("headlines", [])[:8]}
    if segment == "today_matchups":
        return {"upcoming_matchups": briefpack.get("upcoming_matchups", [])[:8]}
    return {
        "top_scores": briefpack.get("top_scores", [])[:4],
        "headlines": briefpack.get("headlines", [])[:4],
        "upcoming_matchups": briefpack.get("upcoming_matchups", [])[:4],
    }


def _block_prompt(
    segment: str,
    min_words: int,
    max_words: int,
    briefpack: dict[str, Any],
    prior_context: str,
) -> str:
    return (
        "Write one sports podcast section as natural two-host dialogue.\n"
        "Hosts are locked: Evan Cole and Marcus Reed.\n"
        "Output only spoken lines, each prefixed by host name.\n"
        "No bullets, no URLs, no JSON, no metadata.\n"
        "No fabricated games or results.\n"
        "If uncertain, omit.\n"
        f"Section: {segment}\n"
        f"Word target: {min_words}-{max_words}\n"
        "Keep sentence starters varied.\n"
        "For recap references, only mention completed matchups from top_scores.\n\n"
        "Prior accepted context (for continuity):\n"
        f"{prior_context[-1400:]}\n\n"
        "Allowed facts for this section:\n"
        f"{json.dumps(_segment_fact_payload(segment, briefpack), ensure_ascii=True)}\n"
    )


def _fallback_block_text(segment: str, briefpack: dict[str, Any]) -> str:
    evan_openers = [
        "From the board,",
        "One result that stood out,",
        "Stepping back for context,",
        "What matters in this spot,",
    ]
    marcus_openers = [
        "The reaction around that is,",
        "What jumped out right away is,",
        "The bigger takeaway for me is,",
        "The immediate ripple effect is,",
    ]
    if segment in {"scoreboard_roundup", "deep_dive_games"}:
        items = briefpack.get("top_scores", [])[:3]
        if items:
            lines = []
            for idx, item in enumerate(items):
                evan_intro = evan_openers[idx % len(evan_openers)]
                marcus_intro = marcus_openers[idx % len(marcus_openers)]
                lines.append(
                    f"Evan Cole: {evan_intro} {item.get('summary', '')} This shifts momentum and postseason leverage."
                )
                lines.append(
                    f"Marcus Reed: {marcus_intro} that result changes how we're reading this race over the next few days."
                )
            return "\n".join(lines)
    if segment in {"major_storylines", "cold_open", "optional_international"}:
        items = briefpack.get("headlines", [])[:3]
        if items:
            lines = []
            for idx, item in enumerate(items):
                lines.append(
                    f"Evan Cole: {evan_openers[idx % len(evan_openers)]} {item.get('title', '')}."
                )
                lines.append(
                    f"Marcus Reed: {marcus_openers[idx % len(marcus_openers)]} the downstream impact matters more than the headline itself."
                )
            return "\n".join(lines)
    if segment == "today_matchups":
        items = briefpack.get("upcoming_matchups", [])[:3]
        if items:
            lines = []
            for idx, item in enumerate(items):
                lines.append(
                    f"Evan Cole: {evan_openers[idx % len(evan_openers)]} {item.get('summary', '')}"
                )
                lines.append(
                    f"Marcus Reed: {marcus_openers[idx % len(marcus_openers)]} that's a matchup with real stakes and clear tactical angles."
                )
            return "\n".join(lines)
    if segment == "closing":
        return (
            "Evan Cole: That's our show for today, and we kept it focused on verified outcomes and key implications.\n"
            "Marcus Reed: We'll be back with the next slate and the stories that actually move the board."
        )
    return (
        "Evan Cole: We're continuing with verified storylines only.\n"
        "Marcus Reed: And we're keeping the focus on what actually matters for the next stretch."
    )


def _verify_block(
    segment: str,
    text: str,
    briefpack: dict[str, Any],
    min_words: int,
) -> list[str]:
    issues: list[str] = []
    metrics = analyze_script_text(text)
    if metrics["unique_host_count"] < 2:
        issues.append("host_variety")
    if metrics["spoken_word_count"] < max(80, int(min_words * 0.45)):
        issues.append("too_short")
    if _unsupported_last_night_matchups(text, briefpack):
        issues.append("unsupported_last_night_matchup")
    return issues


def _generate_verified_blocks(
    llm: LLMClient,
    plan: dict[str, Any],
    briefpack: dict[str, Any],
) -> list[dict[str, Any]]:
    budgets = _section_budgets(plan)
    allowed_citations = _allowed_citations_by_segment(briefpack)
    ordered_sections = [
        s
        for s in plan.get("sections", [])
        if s in budgets
    ]
    if not ordered_sections:
        ordered_sections = list(budgets.keys())

    accepted: list[dict[str, Any]] = []
    rolling_context = ""
    for section in ordered_sections:
        min_words = budgets[section]["min_words"]
        max_words = budgets[section]["max_words"]
        best_text = ""
        best_issues: list[str] = ["uninitialized"]
        for _ in range(3):
            candidate = llm.complete(
                TRANSCRIPT_BUILDER_SYSTEM_PROMPT,
                _block_prompt(section, min_words, max_words, briefpack, rolling_context),
                max_completion_tokens=4000,
                temperature=0.25,
            ).strip()
            candidate = _normalize_host_labels(candidate)
            issues = _verify_block(section, candidate, briefpack, min_words)
            if not issues:
                best_text = candidate
                best_issues = []
                break
            if not best_text or len(issues) < len(best_issues):
                best_text = candidate
                best_issues = issues

        if best_issues:
            logger.warning(
                "block_generation_fallback",
                extra={"segment": section, "issues": best_issues},
            )
            best_text = _fallback_block_text(section, briefpack)

        citations = allowed_citations.get(section, [])[:3]
        accepted.append({"segment": section, "text": best_text, "citations": citations})
        rolling_context = f"{rolling_context}\n\n{best_text}".strip()
    return accepted


def run(run_date: str) -> dict:
    store = ObjectStore()
    briefpack = store.get_json(f"briefpacks/{run_date}/briefpack.json")
    cfg = get_settings()

    llm = LLMClient()
    plan = build_outline(llm, briefpack)
    segments = _generate_verified_blocks(llm, plan, briefpack)
    draft = {"segments": segments}
    verify = verify_transcript_draft(llm, draft, briefpack)
    # Deterministic verification baseline for stability.
    verify = {**verify, **compute_verification_metrics(draft, briefpack)}

    polished_script = "\n\n".join(seg.get("text", "").strip() for seg in segments if seg.get("text"))
    polished_script = _normalize_host_labels(polished_script)
    polished_script = _enforce_two_hosts(llm, polished_script, briefpack)

    transcript = build_transcript_payload(
        run_date,
        draft,
        verify,
        llm.fallback_count,
        briefpack,
        polished_script_text=polished_script,
    )

    if cfg.transcript_require_llm and llm.fallback_count > 0:
        raise RuntimeError(
            f"Transcript generation refused degraded mode (fallback_count={llm.fallback_count}, reasons={llm.fallback_reasons})."
        )

    script_metrics = analyze_script_text(transcript.get("script_text", ""))
    if script_metrics["unique_host_count"] < 2:
        raise RuntimeError("Transcript generation failed runtime quality target: fewer than 2 host voices")
    script_text = transcript.get("script_text", "")
    if "Evan Cole:" not in script_text or "Marcus Reed:" not in script_text:
        raise RuntimeError(
            "Transcript generation failed quality target: locked hosts Evan Cole and Marcus Reed not both present"
        )
    unsupported = _unsupported_last_night_matchups(script_text, briefpack)
    if unsupported:
        raise RuntimeError(
            "Transcript generation failed factual grounding: unsupported last-night matchup claim detected "
            f"(example: {unsupported[0][:180]})"
        )

    schema_path = Path("schemas/transcript.schema.json")
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    jsonschema.validate(transcript, schema)

    store.put_json(f"transcripts/{run_date}/transcript.plan.json", plan)
    store.put_json(f"transcripts/{run_date}/transcript.draft.json", draft)
    store.put_json(f"transcripts/{run_date}/transcript.json", transcript)
    store.put_json(
        f"transcripts/{run_date}/llm_diagnostics.json",
        {
            "run_date": run_date,
            "fallback_count": llm.fallback_count,
            "fallback_reasons": llm.fallback_reasons,
        },
    )
    store.put_json(f"transcripts/{run_date}/verification.json", verify)
    logger.info(
        "transcript_generation_complete",
        extra={
            "run_date": run_date,
            "llm_degraded_mode": llm.fallback_count > 0,
            "llm_fallback_count": llm.fallback_count,
        },
    )
    return transcript
