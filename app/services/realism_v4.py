from __future__ import annotations

import re


def _clean_turn_text(text: str) -> str:
    txt = re.sub(r"\s+", " ", str(text or "")).strip()
    sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", txt) if s.strip()]
    out: list[str] = []
    seen: set[str] = set()
    for sentence in sentences:
        norm = re.sub(r"[^a-z0-9]+", "", sentence.lower())
        if norm in seen:
            continue
        seen.add(norm)
        out.append(sentence)
    return " ".join(out)


def apply_realism(script: dict) -> dict:
    out = dict(script)
    sections_out = []
    last_speaker = None
    for section in script.get("sections", []):
        turns_out = []
        for turn in section.get("turns", []):
            t = dict(turn)
            t["text"] = _clean_turn_text(str(t.get("text", "")))
            speaker = t.get("speaker")
            if speaker == last_speaker and turns_out:
                # Keep strict back-and-forth in the deterministic path.
                speaker = "Marcus Reed" if speaker == "Evan Cole" else "Evan Cole"
                t["speaker"] = speaker
            turns_out.append(t)
            last_speaker = speaker
        s = dict(section)
        s["turns"] = turns_out
        sections_out.append(s)
    out["sections"] = sections_out
    out.setdefault("meta", {})["realism_mode"] = "deterministic_cleanup"
    return out
