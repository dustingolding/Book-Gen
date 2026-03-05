import re
from typing import Any

from .prompt_loader import load_prompt

TRANSCRIPT_BUILDER_SYSTEM_PROMPT = load_prompt("transcript_builder_prompt.txt")


def _clean_spoken_text(text: str) -> str:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    cleaned = []
    for ln in lines:
        lower = ln.lower()
        if lower.startswith("http://") or lower.startswith("https://"):
            continue
        if lower.startswith("- http://") or lower.startswith("- https://"):
            continue
        cleaned.append(ln)
    return " ".join(cleaned).strip()


def _is_example_source(url: str) -> bool:
    u = (url or "").lower()
    return ".example/" in u or ".example." in u


def analyze_script_text(script_text: str) -> dict[str, Any]:
    text = (script_text or "").strip()
    all_words = re.findall(r"\b[\w']+\b", text)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    # Detect turns even when dialogue appears inline on one long line.
    turn_pat = re.compile(r"(Evan Cole|Marcus Reed|Host [ABC]|Host C):")
    matches = list(turn_pat.finditer(text))
    hosts = {m.group(1) for m in matches}

    host_turn_chunks: list[str] = []
    if matches:
        for idx, m in enumerate(matches):
            start = m.end()
            end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
            chunk = text[start:end].strip()
            if chunk:
                host_turn_chunks.append(chunk)
    else:
        host_lines = [
            ln
            for ln in lines
            if re.match(r"^(Host [ABC]|Evan Cole|Marcus Reed|Host C):", ln)
        ]
        hosts = {ln.split(":", 1)[0] for ln in host_lines if ":" in ln}
        host_turn_chunks = [ln.split(":", 1)[1].strip() for ln in host_lines if ":" in ln]

    lowered = text.lower()
    robotic_phrases = [
        "next up",
        "moving on",
        "final score",
        "thrilling matchup",
        "huge win",
        "statement game",
    ]
    robotic_hits = sum(lowered.count(p) for p in robotic_phrases)
    final_score_hits = lowered.count("final score")

    starters: list[str] = []
    cliche_starter_heads = {
        "absolutely",
        "right",
        "exactly",
        "for",
        "yeah",
        "well",
        "alright",
        "okay",
        "look",
        "listen",
        "honestly",
        "definitely",
        "sure",
        "totally",
        "clearly",
    }
    for chunk in host_turn_chunks:
        chunk_words = re.findall(r"[A-Za-z']+", chunk.lower())
        if not chunk_words:
            continue
        # Repetition detection should focus on canned, robotic discourse starters
        # rather than penalizing normal semantic openings like "the defense was...".
        if chunk_words[0] not in cliche_starter_heads:
            continue
        starters.append(" ".join(chunk_words[:3]))
    repetitive_starter_hits = 0
    counts: dict[str, int] = {}
    for s in starters:
        counts[s] = counts.get(s, 0) + 1
    for _, c in counts.items():
        if c > 2:
            repetitive_starter_hits += c - 2

    return {
        "spoken_word_count": len(all_words),
        "estimated_minutes_145wpm": round(len(all_words) / 145.0, 1) if all_words else 0.0,
        "host_turn_count": len(host_turn_chunks),
        "unique_hosts": sorted(hosts),
        "unique_host_count": len(hosts),
        "robotic_phrase_hits": robotic_hits,
        "final_score_phrase_hits": final_score_hits,
        "repetitive_starter_hits": repetitive_starter_hits,
    }


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


def _sanitize_segments(draft: dict[str, Any], briefpack: dict[str, Any]) -> list[dict[str, Any]]:
    allowed_global = set(briefpack.get("citations", []))
    allowed_by_segment = _allowed_citations_by_segment(briefpack)
    sanitized = []
    for seg in draft.get("segments", []):
        segment_name = seg.get("segment", "")
        text = _clean_spoken_text(seg.get("text", ""))
        if not text:
            text = "Update unavailable."

        citations = []
        for c in seg.get("citations", []):
            if isinstance(c, str) and c in allowed_global and not _is_example_source(c):
                citations.append(c)
        if not citations:
            citations = allowed_by_segment.get(segment_name, [])

        citations = list(dict.fromkeys(citations))[:3]
        sanitized.append({"segment": segment_name, "text": text, "citations": citations})
    return sanitized


def _script_text(segments: list[dict[str, Any]]) -> str:
    ordered_lines = []
    for seg in segments:
        name = seg.get("segment", "").strip()
        text = seg.get("text", "").strip()
        if not text:
            continue
        ordered_lines.append(f"{name}. {text}" if name else text)
    return "\n\n".join(ordered_lines)


def build_transcript_payload(
    run_date: str,
    draft: dict[str, Any],
    verify: dict[str, Any],
    llm_fallback_count: int,
    briefpack: dict[str, Any],
    polished_script_text: str | None = None,
) -> dict[str, Any]:
    segments = _sanitize_segments(draft, briefpack)
    script_text = (polished_script_text or "").strip() or _script_text(segments)
    script_metrics = analyze_script_text(script_text)
    return {
        "run_date": run_date,
        "title": f"Side Line Wire Dailycast - {run_date}",
        "language": "en-US",
        "segments": segments,
        "script_text": script_text,
        "spoken_word_count": script_metrics["spoken_word_count"],
        "estimated_minutes": script_metrics["estimated_minutes_145wpm"],
        "host_turn_count": script_metrics["host_turn_count"],
        "unique_hosts": script_metrics["unique_hosts"],
        "robotic_phrase_hits": script_metrics["robotic_phrase_hits"],
        "final_score_phrase_hits": script_metrics["final_score_phrase_hits"],
        "repetitive_starter_hits": script_metrics["repetitive_starter_hits"],
        "citation_coverage": float(verify["citation_coverage"]),
        "numeric_fidelity": float(verify["numeric_fidelity"]),
        "verified": bool(verify["approved"]),
        "llm_degraded_mode": llm_fallback_count > 0,
        "llm_fallback_count": llm_fallback_count,
    }
