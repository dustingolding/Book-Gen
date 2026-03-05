from __future__ import annotations

from app.config import get_settings


def _voice_for(speaker: str, cfg) -> str:
    mapping = {
        "Evan Cole": cfg.eleven_voice_evan,
        "Marcus Reed": cfg.eleven_voice_marcus,
        "Tyler Grant": cfg.eleven_voice_tyler,
        "Darius Vaughn": cfg.eleven_voice_darius,
        "Caleb Mercer": cfg.eleven_voice_caleb,
        "Lucas Bennett": cfg.eleven_voice_lucas,
    }
    return str(mapping.get(speaker) or cfg.eleven_voice_evan or "default")


def _escape_xml(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def build_ssml(script: dict) -> str:
    cfg = get_settings()
    parts = ["<speak>"]
    for sec in script.get("sections", []):
        for turn in sec.get("turns", []):
            speaker = str(turn.get("speaker", "Evan Cole"))
            voice = _voice_for(speaker, cfg)
            text = _escape_xml(str(turn.get("text", "")).strip())
            if not text:
                continue
            parts.append(f'<voice name="{voice}">{text}</voice>')
            parts.append('<break time="280ms"/>')
        parts.append('<break time="420ms"/>')
    parts.append("</speak>")
    return "\n".join(parts)
