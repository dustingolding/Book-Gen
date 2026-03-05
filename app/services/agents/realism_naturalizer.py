from __future__ import annotations

import logging

from app.clients.llm import LLMClient
from app.services.agents.prompt_loader import load_prompt
from app.services.agents.transcript_builder import analyze_script_text
from app.storage import ObjectStore

logger = logging.getLogger(__name__)


def run(process_date: str) -> dict:
    store = ObjectStore()
    transcript = store.get_json(f"transcripts/{process_date}/transcript.json")
    briefpack = store.get_json(f"briefpacks/{process_date}/briefpack.json")

    before = analyze_script_text(transcript.get("script_text", ""))
    llm = LLMClient()
    prompt = load_prompt("realism_prompt.txt")
    host_profiles = load_prompt("host_profiles.yaml")

    user_prompt = (
        f"{prompt}\n\n"
        "Host profiles:\n"
        f"{host_profiles}\n\n"
        "BriefPack JSON:\n"
        f"{briefpack}\n\n"
        "Current transcript:\n"
        f"{transcript.get('script_text', '')}\n"
    )

    rewritten = llm.complete(
        "You improve conversational realism while preserving facts exactly.",
        user_prompt,
        max_completion_tokens=12000,
        temperature=0.25,
    ).strip()

    if rewritten:
        transcript["script_text"] = rewritten
        after = analyze_script_text(rewritten)
        transcript["spoken_word_count"] = after["spoken_word_count"]
        transcript["estimated_minutes"] = after["estimated_minutes_145wpm"]
        transcript["host_turn_count"] = after["host_turn_count"]
        transcript["unique_hosts"] = after["unique_hosts"]
        transcript["robotic_phrase_hits"] = after["robotic_phrase_hits"]
        transcript["final_score_phrase_hits"] = after["final_score_phrase_hits"]
        transcript["repetitive_starter_hits"] = after["repetitive_starter_hits"]
        store.put_json(f"transcripts/{process_date}/transcript.realism.json", transcript)
        store.put_json(f"transcripts/{process_date}/transcript.json", transcript)
        logger.info(
            "realism_pass_complete",
            extra={
                "run_date": process_date,
                "words_before": before["spoken_word_count"],
                "words_after": after["spoken_word_count"],
                "llm_fallback_count": llm.fallback_count,
            },
        )
    return {
        "run_date": process_date,
        "status": "completed",
        "llm_fallback_count": llm.fallback_count,
    }
