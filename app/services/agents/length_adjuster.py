from __future__ import annotations

import logging
import re

from app.clients.llm import LLMClient
from app.config import get_settings
from app.services.agents.qa_rubric import RuntimeBand, estimate_runtime_minutes, runtime_status
from app.services.agents.transcript_builder import analyze_script_text
from app.storage import ObjectStore

logger = logging.getLogger(__name__)


def _expand_prompt(current_script: str, briefpack: dict, min_additional_words: int) -> str:
    return (
        "Append NEW dialogue only. Do not rewrite existing lines.\n"
        f"Append at least {min_additional_words} spoken words.\n"
        "Keep facts aligned with BriefPack only. No new facts.\n"
        "Use Evan Cole / Marcus Reed turns with natural rhythm.\n"
        "No links, bullets, metadata.\n\n"
        f"Current transcript:\n{current_script}\n\n"
        f"BriefPack JSON:\n{briefpack}\n"
    )


def _compress_prompt(current_script: str, target_max_words: int, briefpack: dict) -> str:
    return (
        "Compress this transcript while preserving core narratives and factual correctness.\n"
        f"Target max spoken words: {target_max_words}.\n"
        "Remove repetitive turns and low-priority recap details first.\n"
        "No links, bullets, metadata.\n\n"
        f"Current transcript:\n{current_script}\n\n"
        f"BriefPack JSON:\n{briefpack}\n"
    )


def _deterministic_extension_block(briefpack: dict, idx: int) -> str:
    scores = briefpack.get("top_scores", [])
    headlines = briefpack.get("headlines", [])
    matchups = briefpack.get("upcoming_matchups", [])
    s = scores[idx % max(1, len(scores))] if scores else {}
    h = headlines[idx % max(1, len(headlines))] if headlines else {}
    u = matchups[idx % max(1, len(matchups))] if matchups else {}
    return (
        f"Evan Cole: {s.get('summary', 'A key result shifted momentum in the standings.')}\n"
        "Marcus Reed: The margin alone is not the headline, it's how the game script affected confidence and rotation decisions.\n"
        f"Evan Cole: On news context, {h.get('title', 'front-office decisions are tightening as pressure rises')}.\n"
        "Marcus Reed: That has immediate implications for lineup stability, postseason readiness, and how teams manage risk this week.\n"
        f"Evan Cole: Looking ahead, {u.get('summary', 'today has matchups with direct playoff implications')}.\n"
        "Marcus Reed: That's where preparation, depth, and late-game execution become the real separators.\n"
    )


def _deterministic_expand(script: str, briefpack: dict, min_words: int) -> str:
    text = script
    for i in range(20):
        if analyze_script_text(text)["spoken_word_count"] >= min_words:
            break
        text = f"{text}\n\n{_deterministic_extension_block(briefpack, i)}".strip()
    return text


def _deterministic_compress(script: str, target_max_words: int) -> str:
    words = script.split()
    if len(words) <= target_max_words:
        return script
    return " ".join(words[:target_max_words])


def _de_robotic_cleanup(script: str) -> str:
    text = script or ""
    replacements = [
        (r"\b[Ff]inal score:\s*", ""),
        (r"\bfinal score\b", "result"),
        (r"\b[Nn]ext up\b", "Later in the slate"),
        (r"\b[Mm]oving on\b", "Shifting focus"),
        (r"\b[Tt]hrilling matchup\b", "close matchup"),
        (r"\b[Hh]uge win\b", "important result"),
        (r"\b[Ss]tatement game\b", "defining game"),
    ]
    for pat, repl in replacements:
        text = re.sub(pat, repl, text)
    return text


def _diversify_repetitive_starters(script: str) -> str:
    lines = (script or "").splitlines()
    turn_re = re.compile(r"^(Evan Cole|Marcus Reed):\s*(.*)$")
    seen: dict[str, int] = {}
    evan_variants = [
        "Let's frame this clearly,",
        "From a standings lens,",
        "The key detail here,",
        "One angle worth noting,",
        "Context for listeners,",
    ]
    marcus_variants = [
        "Here's what stood out to me,",
        "The part I keep circling,",
        "Where I push back a bit,",
        "What makes this interesting,",
        "My read on that spot,",
    ]

    out: list[str] = []
    for ln in lines:
        m = turn_re.match(ln.strip())
        if not m:
            out.append(ln)
            continue
        speaker, body = m.group(1), m.group(2).strip()
        words = re.findall(r"[A-Za-z']+", body.lower())
        starter = " ".join(words[:3]) if words else ""
        if starter:
            count = seen.get(starter, 0)
            seen[starter] = count + 1
            if count >= 2:
                variants = evan_variants if speaker == "Evan Cole" else marcus_variants
                prefix = variants[count % len(variants)]
                body = f"{prefix} {body}"
        out.append(f"{speaker}: {body}")
    return "\n".join(out)


def adjust_length_if_needed(process_date: str) -> dict:
    cfg = get_settings()
    store = ObjectStore()
    transcript = store.get_json(f"transcripts/{process_date}/transcript.json")
    briefpack = store.get_json(f"briefpacks/{process_date}/briefpack.json")
    llm = LLMClient()

    min_words = cfg.transcript_min_words
    max_words = cfg.transcript_max_words
    wpm = cfg.transcript_wpm_baseline
    band = RuntimeBand(cfg.transcript_runtime_target_min, cfg.transcript_runtime_target_max, cfg.transcript_runtime_soft_min, cfg.transcript_runtime_soft_max)

    script = transcript.get("script_text", "")
    metrics = analyze_script_text(script)
    words = metrics["spoken_word_count"]

    for _ in range(3):
        minutes = estimate_runtime_minutes(words, wpm)
        state = runtime_status(minutes, band)
        if state == "target" and min_words <= words <= max_words:
            break

        if words < min_words:
            script = _deterministic_expand(script, briefpack, min_words)
        elif words > max_words:
            script = _deterministic_compress(script, max_words)
        else:
            break

        metrics = analyze_script_text(script)
        words = metrics["spoken_word_count"]

    transcript["script_text"] = script
    transcript["script_text"] = _de_robotic_cleanup(transcript["script_text"])
    transcript["script_text"] = _diversify_repetitive_starters(transcript["script_text"])
    script = transcript["script_text"]
    metrics = analyze_script_text(script)
    words = metrics["spoken_word_count"]
    transcript["spoken_word_count"] = words
    transcript["estimated_minutes"] = round(estimate_runtime_minutes(words, wpm), 1)
    transcript["host_turn_count"] = metrics["host_turn_count"]
    transcript["unique_hosts"] = metrics["unique_hosts"]
    transcript["robotic_phrase_hits"] = metrics["robotic_phrase_hits"]
    transcript["final_score_phrase_hits"] = metrics["final_score_phrase_hits"]
    transcript["repetitive_starter_hits"] = metrics["repetitive_starter_hits"]

    store.put_json(f"transcripts/{process_date}/transcript.length_adjusted.json", transcript)
    store.put_json(f"transcripts/{process_date}/transcript.json", transcript)

    if transcript["spoken_word_count"] < min_words:
        raise RuntimeError(
            f"Transcript generation failed runtime quality target: spoken words {transcript['spoken_word_count']} < {min_words}"
        )

    logger.info(
        "length_adjust_complete",
        extra={
            "run_date": process_date,
            "spoken_words": transcript["spoken_word_count"],
            "estimated_minutes": transcript["estimated_minutes"],
            "llm_fallback_count": llm.fallback_count,
        },
    )
    return {
        "run_date": process_date,
        "spoken_words": transcript["spoken_word_count"],
        "estimated_minutes": transcript["estimated_minutes"],
        "llm_fallback_count": llm.fallback_count,
    }
