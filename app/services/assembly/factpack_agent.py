from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from dataclasses import asdict
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import jsonschema

from app.clients.llm import LLMClient
from app.config import get_settings
from app.services.coverage import allocate_show_time, compute_sport_states, load_yaml

ET = ZoneInfo("America/New_York")
logger = logging.getLogger(__name__)
LOCKED_RANKING = [
    "college_football",
    "nfl",
    "college_basketball",
    "nba",
    "mlb",
    "womens_college_basketball",
    "wnba",
    "nhl",
    "major_international",
]

LOW_VALUE_NEWS_DOMAINS = (
    "nypost.com",
    "outkick.com",
    "steelersdepot.com",
    "elevenwarriors.com",
)
SCORE_HEADLINE_RE = re.compile(r"\b\d{2,3}\s*-\s*\d{2,3}\b")
SOURCE_SUFFIX_RE = re.compile(r"\s[-|]\s([A-Za-z0-9.&' ]{2,40})$")
NON_SPORTS_NEWS_TOKENS = (
    "state of the union",
    "white house",
    "fbi",
    "kash patel",
    "criminal act",
    "director's schedule",
    "politics",
    "senate",
    "congress",
)
THIN_NEWS_PHRASES = (
    "no summary provided",
    "puck drop is",
    "how to watch",
    "best goals",
    "look back",
    "remember that guy",
    "who could",
)
LOW_VALUE_NEWS_TOKENS = (
    "best photos",
    "best photo",
    "best goals",
    "how to watch",
    "observations",
    "look back",
    "remember that guy",
    "has the latest",
    "latest on",
    "hilariously intense facial expressions",
)
LOW_VALUE_MAJOR_NEWS_TOKENS = (
    "mock draft",
    "40-yard dash times by position",
    "best and average 40-yard dash times",
    "viral stars",
)
HARD_REJECT_MAJOR_NEWS_TOKENS = (
    "mock draft",
    "best and average 40-yard dash times",
    "40-yard dash times by position",
)
CONCRETE_NEWS_KEYWORDS = (
    "trade",
    "combine",
    "pro day",
    "draft",
    "injury",
    "contract",
    "extension",
    "re-sign",
    "signed",
    "free agent",
    "free agency",
    "survey",
    "report cards",
    "dies",
    "died",
    "death",
    "lawsuit",
    "medal",
    "olympic",
    "suspension",
    "hired",
    "fired",
)
GENERIC_SUMMARY_PATTERNS = (
    "scheduled matchup with in-season relevance",
    "seeding pressure is increasing",
    "what matters next",
    "pivots to watch",
    "runbook",
    "verified slate",
)


def _has_repeated_summary_clause(text: str) -> bool:
    clauses = [
        _normalize_text_token(clause)
        for clause in re.split(r"(?<=[.!?])\s+|;\s+", str(text or "").strip())
        if _normalize_text_token(clause)
    ]
    seen: set[str] = set()
    for clause in clauses:
        if clause in seen:
            return True
        seen.add(clause)
    return False


def _news_summary_is_strong(summary: str, title: str, fact_points: list[str] | None = None) -> bool:
    raw = str(summary or "").strip()
    if not raw or len(raw.split()) < 12:
        return False
    if "?" in raw:
        return False
    low = raw.lower()
    if any(token in low for token in THIN_NEWS_PHRASES + LOW_VALUE_NEWS_TOKENS):
        return False
    if _summary_repeats_title(raw, title):
        return False
    if _has_repeated_summary_clause(raw):
        return False
    if not _is_concrete_news_text(raw):
        return False
    if fact_points is not None and len([p for p in fact_points if str(p).strip()]) < 1:
        return False
    return True


def _safe_json_loads(text: str) -> dict[str, Any] | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        match = re.search(r"\{.*\}", raw, flags=re.DOTALL)
        if not match:
            return None
        try:
            data = json.loads(match.group(0))
            return data if isinstance(data, dict) else None
        except Exception:
            return None


def _normalize_text_token(text: str) -> str:
    return re.sub(r"[^a-z0-9 ]+", "", re.sub(r"\s+", " ", str(text or "").strip().lower())).strip()


def _dedupe_summary_clauses(parts: list[str]) -> list[str]:
    prepared: list[tuple[str, str]] = []
    for part in parts:
        clean = re.sub(r"\s+", " ", str(part or "").strip()).strip(" .")
        if not clean:
            continue
        norm = _normalize_text_token(clean)
        if not norm:
            continue
        prepared.append((clean, norm))
    unique: list[str] = []
    seen: set[str] = set()
    for clean, norm in sorted(prepared, key=lambda item: len(item[1]), reverse=True):
        if norm in seen:
            continue
        if any(norm and (norm in existing or existing in norm) for existing in seen):
            continue
        seen.add(norm)
        unique.append(clean)
    return unique


def _finalize_news_summary(text: str, fact_points: list[str] | None = None) -> str:
    clauses = re.split(r"(?<=[.!?])\s+|;\s+", str(text or "").strip())
    if fact_points:
        clauses.extend(str(x).strip() for x in fact_points if str(x).strip())
    unique = _dedupe_summary_clauses(clauses)
    out = ". ".join(unique[:3]).strip()
    if out and not out.endswith("."):
        out = f"{out}."
    return out


def _summary_repeats_title(summary: str, title: str) -> bool:
    summary_norm = _normalize_text_token(summary)
    title_norm = _normalize_text_token(title)
    if not summary_norm or not title_norm:
        return False
    if summary_norm == title_norm:
        return True
    summary_clauses = [clause.strip() for clause in re.split(r"[.!?]\s+|;\s+", summary_norm) if clause.strip()]
    return any(clause == title_norm or clause in title_norm or title_norm in clause for clause in summary_clauses)


def _remove_title_equivalent_clauses(text: str, title: str) -> str:
    clauses = [clause.strip() for clause in re.split(r"(?<=[.!?])\s+|;\s+", str(text or "").strip()) if clause.strip()]
    title_norm = _normalize_text_token(title)
    kept: list[str] = []
    for clause in clauses:
        norm = _normalize_text_token(clause)
        if not norm:
            continue
        if title_norm and (norm == title_norm or norm in title_norm or title_norm in norm):
            continue
        kept.append(clause)
    out = ". ".join(kept).strip()
    if out and not out.endswith("."):
        out = f"{out}."
    return out


def _major_news_card_allowed(title: str, summary: str, source_url: str) -> bool:
    text = f"{title} {summary} {source_url}".lower()
    if any(token in text for token in HARD_REJECT_MAJOR_NEWS_TOKENS):
        return False
    return True


def _major_news_backfill_allowed(title: str, summary: str, source_url: str) -> bool:
    text = f"{title} {summary} {source_url}".lower()
    if any(token in text for token in HARD_REJECT_MAJOR_NEWS_TOKENS):
        return False
    if any(token in text for token in (
        "viral stars",
        "how to watch",
        "best goals",
        "takeaways from day 1",
        "lines, notes",
        "preview",
        "game ",
    )):
        return False
    return True


def _is_concrete_news_text(text: str) -> bool:
    low = str(text or "").lower()
    return any(token in low for token in CONCRETE_NEWS_KEYWORDS)


def _title_fact_sentence(title: str) -> str:
    raw = re.sub(r"\s+", " ", str(title or "").strip()).strip(" .")
    low = raw.lower()
    if not raw:
        return ""
    if raw.lower().startswith("source:"):
        raw = raw.split(":", 1)[1].strip()
        low = raw.lower()
    if "report cards" in low and "all 32 teams" in low:
        return "The NFLPA's 2026 report cards publish player-survey results for all 32 teams."
    if " to re-sign " in low:
        return re.sub(r"\bTo Re-Sign\b", "are set to re-sign", raw, flags=re.IGNORECASE).rstrip(".") + "."
    if " to sign " in low:
        return re.sub(r"\bTo Sign\b", "are set to sign", raw, flags=re.IGNORECASE).rstrip(".") + "."
    if "40-yard dash times by position" in low:
        return "The 2026 NFL Combine now has position-by-position 40-yard dash benchmarks on the board."
    if "what we learned" in low and "combine" in low:
        return "Wednesday's combine work in Indianapolis produced a new round of prospect takeaways and evaluation notes."
    if "mock draft" in low and ":" in raw:
        remainder = raw.split(":", 1)[1].strip()
        if remainder:
            return f"An early three-round mock draft projects {remainder.rstrip('.')}."
    if "best workouts" in low or "risers" in low:
        return "Combine workouts are already lifting a group of 2026 draft risers up early boards."
    if any(token in low for token in ("re-sign", "extension", "contract", "free agency", "combine", "injury", "died", "dies")):
        return raw if raw.endswith(".") else f"{raw}."
    return ""


def _news_fact_points(title: str, facts: dict[str, Any]) -> list[str]:
    normalized_title = _normalize_text_token(title)
    points = [
        re.sub(r"\s+", " ", str(x).strip()).strip()
        for x in (facts.get("fact_points") or [])
        if str(x).strip() and str(x).strip().lower() not in {"no summary provided", "no summary provided."}
    ]
    fallback_title_point = _title_fact_sentence(title)
    if fallback_title_point:
        fallback_norm = _normalize_text_token(fallback_title_point)
        if fallback_norm and not any(
            fallback_norm == _normalize_text_token(point)
            or fallback_norm in _normalize_text_token(point)
            or _normalize_text_token(point) in fallback_norm
            or (normalized_title and normalized_title == _normalize_text_token(point))
            for point in points
        ):
            points.append(fallback_title_point)
    cleaned = _dedupe_summary_clauses(points)
    if normalized_title:
        cleaned = [
            point
            for point in cleaned
            if _normalize_text_token(point) != normalized_title
        ]
    return cleaned


def _show_identity() -> dict[str, Any]:
    cfg = load_yaml("config/show_identity.yaml")
    show = cfg.get("show", {})
    hosts = show.get("hosts", {})
    return {
        "name": show.get("name", "SideLine Wire DailyCast"),
        "hosts": {
            "primary": list(hosts.get("primary", ["Evan Cole", "Marcus Reed"])),
            "specialists": dict(hosts.get("specialists", {})),
        },
        "voice_contract": {
            "two_host_format": bool((show.get("rules", {}) or {}).get("two_host_core_format", True)),
            "no_name_variation": bool((show.get("rules", {}) or {}).get("never_rename_hosts", True)),
            "no_rebranding": bool((show.get("rules", {}) or {}).get("never_rename_show", True)),
        },
    }


def _llm_news_summary(llm: LLMClient | None, title: str, facts: dict[str, Any]) -> str:
    fallback = str(facts.get("summary") or facts.get("headline") or title).strip()
    if fallback.lower() in {"no summary provided.", "no summary provided"}:
        fallback = title
    if not llm:
        clean = re.sub(r"\s+", " ", fallback).strip().rstrip("?.")
        fact_points = [re.sub(r"\s+", " ", p).strip("?. ") for p in _news_fact_points(title, facts)]
        clean_cmp = re.sub(r"[^a-z0-9 ]+", "", clean.lower())
        usable_points: list[str] = []
        for point in fact_points:
            point_cmp = re.sub(r"[^a-z0-9 ]+", "", point.lower())
            if not point or "?" in point:
                continue
            if any(token in point.lower() for token in THIN_NEWS_PHRASES + LOW_VALUE_NEWS_TOKENS):
                continue
            if point_cmp and point_cmp != clean_cmp:
                usable_points.append(point)
        concrete_title = _is_concrete_news_text(title)
        if not _is_concrete_news_text(clean) and concrete_title:
            clean = title.strip().rstrip("?.")
        if "?" in clean or any(token in clean.lower() for token in THIN_NEWS_PHRASES + LOW_VALUE_NEWS_TOKENS):
            clean = usable_points[0] if usable_points else clean
        if (not clean or not _is_concrete_news_text(clean)) and concrete_title:
            clean = title.strip().rstrip("?.")
        parts = [clean] if clean else []
        for point in usable_points:
            if len(parts) >= 2:
                break
            parts.append(point)
        return _finalize_news_summary(". ".join(p for p in parts if p), usable_points)

    prompt = (
        "Create a 1-2 sentence factual sports news summary. "
        "Use concrete names and numbers if present. "
        "No hype, no rhetorical questions, no filler. Return plain text only.\n"
        f"TITLE: {title}\n"
        f"FACTS_JSON: {json.dumps(facts, ensure_ascii=True)}"
    )
    out = llm.complete(
        "You are a precise sports desk editor.",
        prompt,
        max_completion_tokens=120,
        temperature=0.1,
    )
    fact_points = _news_fact_points(title, facts)
    return _finalize_news_summary(" ".join(str(out).split()), fact_points)


def _deterministic_news_summary(title: str, facts: dict[str, Any]) -> str:
    fallback = str(facts.get("summary") or facts.get("headline") or title).strip()
    if fallback.lower() in {"no summary provided.", "no summary provided"}:
        fallback = title
    clean = re.sub(r"\s+", " ", fallback).strip().rstrip("?.")
    fact_points = [re.sub(r"\s+", " ", p).strip("?. ") for p in _news_fact_points(title, facts)]
    clean_cmp = re.sub(r"[^a-z0-9 ]+", "", clean.lower())
    usable_points: list[str] = []
    for point in fact_points:
        point_cmp = re.sub(r"[^a-z0-9 ]+", "", point.lower())
        if not point or "?" in point:
            continue
        if any(token in point.lower() for token in THIN_NEWS_PHRASES + LOW_VALUE_NEWS_TOKENS):
            continue
        if point_cmp and point_cmp != clean_cmp:
            usable_points.append(point)
    concrete_title = _is_concrete_news_text(title)
    if not _is_concrete_news_text(clean) and concrete_title:
        clean = title.strip().rstrip("?.")
    if "?" in clean or any(token in clean.lower() for token in THIN_NEWS_PHRASES + LOW_VALUE_NEWS_TOKENS):
        clean = usable_points[0] if usable_points else clean
    if (not clean or not _is_concrete_news_text(clean)) and concrete_title:
        clean = title.strip().rstrip("?.")
    if _summary_repeats_title(clean, title) and usable_points:
        clean = usable_points[0]
    parts = [clean] if clean else []
    for point in usable_points:
        if len(parts) >= 2:
            break
        if _normalize_text_token(point) != _normalize_text_token(clean):
            parts.append(point)
    final = _finalize_news_summary(". ".join(p for p in parts if p), usable_points)
    final = _remove_title_equivalent_clauses(final, title) or final
    if _summary_repeats_title(final, title) and usable_points:
        final = _finalize_news_summary(". ".join(usable_points[:2]), usable_points)
        final = _remove_title_equivalent_clauses(final, title) or final
    if not _news_summary_is_strong(final, title, usable_points):
        fallback_parts = usable_points[:2]
        final = _finalize_news_summary(". ".join(fallback_parts), usable_points)
        final = _remove_title_equivalent_clauses(final, title) or final
    return final


def _rewrite_news_card(llm: LLMClient | None, card: dict[str, Any]) -> dict[str, Any]:
    deterministic = str(card.get("summary", "")).strip()
    fact_points = [str(x).strip() for x in (card.get("fact_points") or []) if str(x).strip()]
    if not llm:
        return card
    payload = {
        "title": str(card.get("title", "")).strip(),
        "sport": str(card.get("sport", "")).strip(),
        "deterministic_summary": deterministic,
        "fact_points": fact_points[:4],
    }
    prompt = (
        "Rewrite this sports news card as strict JSON only.\n"
        "Schema: {\"summary\": string, \"facts_used\": [string]}.\n"
        "Rules: 1-2 factual sentences, no questions, no hype, no invented facts, "
        "must stay within provided facts, and keep concrete names/numbers.\n"
        f"CARD_JSON: {json.dumps(payload, ensure_ascii=True)}"
    )
    out = llm.complete(
        "You are a precise sports desk editor. Return JSON only.",
        prompt,
        max_completion_tokens=140,
        temperature=0.1,
    )
    parsed = _safe_json_loads(out)
    if not parsed:
        return card
    summary = _finalize_news_summary(str(parsed.get("summary", "")), fact_points)
    summary = _remove_title_equivalent_clauses(summary, str(card.get("title", ""))) or summary
    facts_used = [str(x).strip() for x in (parsed.get("facts_used") or []) if str(x).strip()]
    if not _news_summary_is_strong(summary, str(card.get("title", "")), fact_points):
        fallback_summary = _deterministic_news_summary(str(card.get("title", "")), {"summary": deterministic, "fact_points": fact_points})
        if not _news_summary_is_strong(fallback_summary, str(card.get("title", "")), fact_points):
            return card
        summary = fallback_summary
    allowed_tokens = {
        _normalize_text_token(str(card.get("title", ""))),
        *[_normalize_text_token(x) for x in fact_points],
        _normalize_text_token(deterministic),
    }
    for point in facts_used:
        norm = _normalize_text_token(point)
        if norm and norm not in allowed_tokens:
            return card
    rewritten = dict(card)
    rewritten["summary"] = summary
    rewritten["facts_used"] = facts_used[:3]
    return rewritten


def _llm_matchup_summary(llm: LLMClient | None, title: str, facts: dict[str, Any], context: dict[str, Any]) -> str:
    if not llm:
        hr = str(context.get("home_record", "N/A")).strip()
        ar = str(context.get("away_record", "N/A")).strip()
        hs = str(context.get("home_streak", "N/A")).strip()
        aw = str(context.get("away_streak", "N/A")).strip()
        h10 = str(context.get("home_last10", "N/A")).strip()
        a10 = str(context.get("away_last10", "N/A")).strip()
        hrg = context.get("home_rank")
        arg = context.get("away_rank")
        home = str(context.get("home_team", "")).strip()
        away = str(context.get("away_team", "")).strip()
        hdiff = context.get("home_point_diff")
        adiff = context.get("away_point_diff")
        if hr.upper() == "N/A" and ar.upper() == "N/A":
            return ""

        def _rank_text(rank: Any) -> str:
            return str(rank) if isinstance(rank, int) and 0 < rank <= 25 else ""

        def _wins(rec: str) -> int:
            m = re.match(r"^\s*(\d+)-(\d+)\s*$", rec)
            return int(m.group(1)) if m else -1

        def _record_gap_text() -> str:
            hw = _wins(hr)
            aw = _wins(ar)
            if hw >= 0 and aw >= 0:
                gap = abs(hw - aw)
                if gap >= 8:
                    leader = home if hw > aw else away
                    return f"{leader} carries the stronger full-season record into the game"
            return ""

        def _recent_edge_text() -> str:
            if isinstance(hdiff, (int, float)) and isinstance(adiff, (int, float)):
                diff_gap = float(hdiff) - float(adiff)
                if diff_gap >= 3.0:
                    return f"{home} has separated with the stronger recent scoring margin"
                if diff_gap <= -3.0:
                    return f"{away} has separated with the stronger recent scoring margin"
            if hs.startswith("W") and not aw.startswith("W"):
                return f"{home} enters on the stronger streak"
            if aw.startswith("W") and not hs.startswith("W"):
                return f"{away} enters on the stronger streak"
            if h10 != a10 and h10.upper() != "N/A" and a10.upper() != "N/A":
                return f"recent form tilts toward {home if _wins(h10) > _wins(a10) else away}"
            return ""

        away_recent = a10 if a10 and a10.upper() != "N/A" else str(context.get("away_recent_record", "")).strip()
        home_recent = h10 if h10 and h10.upper() != "N/A" else str(context.get("home_recent_record", "")).strip()
        away_rank = _rank_text(arg)
        home_rank = _rank_text(hrg)
        away_label = f"No. {away_rank} {away}" if away_rank else away
        home_label = f"No. {home_rank} {home}" if home_rank else home
        angle = _record_gap_text() or _recent_edge_text() or f"{home} and {away} both bring similar short-term form into the spot"
        return (
            f"{away_label} ({ar}, recent {away_recent or 'N/A'}, streak {aw}) at "
            f"{home_label} ({hr}, recent {home_recent or 'N/A'}, streak {hs}); {angle}."
        )

    prompt = (
        "Write one concise factual preview sentence for this matchup. "
        "Include records and streak context if provided. No boilerplate.\n"
        f"TITLE: {title}\n"
        f"FACTS_JSON: {json.dumps(facts, ensure_ascii=True)}\n"
        f"CONTEXT_JSON: {json.dumps(context, ensure_ascii=True)}"
    )
    out = llm.complete(
        "You are a factual matchup desk editor.",
        prompt,
        max_completion_tokens=90,
        temperature=0.1,
    )
    return " ".join(str(out).split())


def _deterministic_matchup_summary(title: str, facts: dict[str, Any], context: dict[str, Any]) -> str:
    hr = str(context.get("home_record", "N/A")).strip()
    ar = str(context.get("away_record", "N/A")).strip()
    hs = str(context.get("home_streak", "N/A")).strip()
    aw = str(context.get("away_streak", "N/A")).strip()
    h10 = str(context.get("home_last10", "N/A")).strip()
    a10 = str(context.get("away_last10", "N/A")).strip()
    hrg = context.get("home_rank")
    arg = context.get("away_rank")
    home = str(context.get("home_team", "")).strip()
    away = str(context.get("away_team", "")).strip()
    hdiff = context.get("home_point_diff")
    adiff = context.get("away_point_diff")
    if hr.upper() == "N/A" and ar.upper() == "N/A":
        return ""

    def _rank_text(rank: Any) -> str:
        return str(rank) if isinstance(rank, int) and 0 < rank <= 25 else ""

    def _wins(rec: str) -> int:
        m = re.match(r"^\s*(\d+)-(\d+)\s*$", rec)
        return int(m.group(1)) if m else -1

    def _record_gap_text() -> str:
        hw = _wins(hr)
        awn = _wins(ar)
        if hw >= 0 and awn >= 0:
            gap = abs(hw - awn)
            if gap >= 8:
                leader = home if hw > awn else away
                return f"{leader} carries the stronger full-season record into the game"
        return ""

    def _recent_edge_text() -> str:
        if isinstance(hdiff, (int, float)) and isinstance(adiff, (int, float)):
            diff_gap = float(hdiff) - float(adiff)
            if diff_gap >= 3.0:
                return f"{home} has separated with the stronger recent scoring margin"
            if diff_gap <= -3.0:
                return f"{away} has separated with the stronger recent scoring margin"
        if hs.startswith("W") and not aw.startswith("W"):
            return f"{home} enters on the stronger streak"
        if aw.startswith("W") and not hs.startswith("W"):
            return f"{away} enters on the stronger streak"
        if h10 != a10 and h10.upper() != "N/A" and a10.upper() != "N/A":
            return f"recent form tilts toward {home if _wins(h10) > _wins(a10) else away}"
        return ""

    away_recent = a10 if a10 and a10.upper() != "N/A" else str(context.get("away_recent_record", "")).strip()
    home_recent = h10 if h10 and h10.upper() != "N/A" else str(context.get("home_recent_record", "")).strip()
    away_rank = _rank_text(arg)
    home_rank = _rank_text(hrg)
    away_label = f"No. {away_rank} {away}" if away_rank else away
    home_label = f"No. {home_rank} {home}" if home_rank else home
    angle = _record_gap_text() or _recent_edge_text() or f"{home} and {away} both bring similar short-term form into the spot"
    return (
        f"{away_label} ({ar}, recent {away_recent or 'N/A'}, streak {aw}) at "
        f"{home_label} ({hr}, recent {home_recent or 'N/A'}, streak {hs}); {angle}."
    )


def _rewrite_matchup_card(llm: LLMClient | None, card: dict[str, Any]) -> dict[str, Any]:
    deterministic = str(card.get("summary", "")).strip()
    context = dict(card.get("matchup_context") or {})
    if not llm:
        return card
    payload = {
        "title": str(card.get("title", "")).strip(),
        "sport": str(card.get("sport", "")).strip(),
        "deterministic_summary": deterministic,
        "context": context,
    }
    prompt = (
        "Rewrite this matchup preview as strict JSON only.\n"
        "Schema: {\"summary\": string, \"context_used\": [string]}.\n"
        "Rules: one factual preview sentence, use only provided context, no boilerplate, "
        "no seeding filler, no invented standings claims.\n"
        f"CARD_JSON: {json.dumps(payload, ensure_ascii=True)}"
    )
    out = llm.complete(
        "You are a factual matchup desk editor. Return JSON only.",
        prompt,
        max_completion_tokens=120,
        temperature=0.1,
    )
    parsed = _safe_json_loads(out)
    if not parsed:
        return card
    summary = " ".join(str(parsed.get("summary", "")).split())
    if not summary or "?" in summary:
        return card
    low = summary.lower()
    if any(pattern in low for pattern in GENERIC_SUMMARY_PATTERNS):
        return card
    if "scheduled matchup with in-season relevance" in low or "seeding pressure is increasing" in low:
        return card
    for required in (
        str(context.get("home_team", "")).strip().lower(),
        str(context.get("away_team", "")).strip().lower(),
    ):
        if required and required not in low:
            return card
    rewritten = dict(card)
    rewritten["summary"] = summary
    rewritten["context_used"] = [str(x).strip() for x in (parsed.get("context_used") or []) if str(x).strip()][:4]
    return rewritten


def _parse_player_line(line: str) -> dict[str, Any] | None:
    text = str(line or "").strip()
    if " - " not in text:
        return None
    name, stats = text.split(" - ", 1)
    fields = {"PTS": 0, "REB": 0, "AST": 0, "STL": 0, "BLK": 0}
    for token in [t.strip() for t in stats.split(",")]:
        parts = token.split()
        if len(parts) >= 2 and parts[0].isdigit() and parts[1] in fields:
            fields[parts[1]] = int(parts[0])
    if fields["PTS"] == 0 and fields["REB"] == 0 and fields["AST"] == 0:
        return None

    impact = round(
        fields["PTS"] * 1.0
        + fields["REB"] * 0.9
        + fields["AST"] * 1.1
        + fields["STL"] * 1.5
        + fields["BLK"] * 1.4,
        1,
    )
    return {
        "name": name.strip(),
        "line": text,
        "stats": fields,
        "impact_score": impact,
        "trend": "high-usage scoring night" if fields["PTS"] >= 30 else "balanced box-score production",
    }


def _parse_matchup_teams(title: str) -> tuple[str, str]:
    text = str(title or "").strip()
    m = re.search(r"(?:Upcoming:\s*)?(.+?)\s+at\s+(.+)$", text, re.IGNORECASE)
    if not m:
        return "", ""
    return m.group(1).strip().upper(), m.group(2).strip().upper()


def _news_source_allowed(source_url: str) -> bool:
    url = str(source_url or "").lower()
    if not url:
        return False
    if any(domain in url for domain in LOW_VALUE_NEWS_DOMAINS):
        return False
    return True


def _clean_news_title(title: str) -> str:
    text = re.sub(r"\s+", " ", str(title or "").strip())
    m = SOURCE_SUFFIX_RE.search(text)
    if m:
        suffix = m.group(1).strip()
        if 1 <= len(suffix.split()) <= 4:
            text = text[: m.start()].strip()
    return text.strip()


def _title_source_blocked(title: str) -> bool:
    raw = str(title or "").lower()
    suffix_match = SOURCE_SUFFIX_RE.search(str(title or ""))
    suffix = suffix_match.group(1).strip().lower() if suffix_match else ""
    return any(domain.replace(".com", "") in raw or domain.replace(".com", "") in suffix for domain in LOW_VALUE_NEWS_DOMAINS)


def _infer_news_sport(title: str, summary: str, source: str, default: str) -> str:
    text = f"{title} {summary} {source}".lower()
    rules = [
        ("nfl", (r"\bnfl\b", r"\bcombine\b", r"\bpro day\b", r"\bmock draft\b", r"\bquarterback\b")),
        ("nba", (r"\bnba\b", r"\bplayoffs?\b", r"\bwestern conference\b", r"\beastern conference\b")),
        ("nhl", (r"\bnhl\b", r"\bstanley cup\b", r"\bhockey\b")),
        ("mlb", (r"\bmlb\b", r"\bspring training\b", r"\bopening day\b", r"\bworld series\b", r"\bbaseball\b")),
        ("wnba", (r"\bwnba\b", r"\bcommissioner'?s cup\b")),
        ("womens_college_basketball", (r"\bwomen'?s basketball\b", r"\bncaaw\b", r"\bwbb\b")),
        ("college_basketball", (r"\bncaam\b", r"\bmen'?s basketball\b", r"\bncaa tournament\b", r"\bmarch madness\b")),
        ("college_football", (r"\bncaaf\b", r"\bcollege football\b", r"\bbowl\b")),
    ]
    for sport, patterns in rules:
        if any(re.search(p, text) for p in patterns):
            return sport
    d = str(default or "").strip().lower()
    if d in {s for s, _ in rules}:
        return d
    return "major_international"


def build_factpack_from_blobs(run_date: str, blobs: list[dict[str, Any]]) -> dict[str, Any]:
    weights_cfg = load_yaml("config/coverage_weights.yaml")
    seasonality_cfg = load_yaml("config/seasonality.yaml")

    on_date = date.fromisoformat(run_date)
    y_start = datetime.combine(on_date, time.min, tzinfo=ET)
    y_end = y_start + timedelta(days=1)

    game_counts: dict[str, int] = defaultdict(int)
    big_news_flags: dict[str, bool] = defaultdict(bool)
    for blob in blobs:
        sport = str(blob.get("sport", "major_international"))
        if blob.get("blob_type") == "game_result":
            game_counts[sport] += 1
        if blob.get("blob_type") == "news" and str(((blob.get("labels") or {}).get("importance") or "")).lower() == "high":
            big_news_flags[sport] = True

    states = compute_sport_states(
        on_date=on_date,
        seasonality_cfg=seasonality_cfg,
        weights_cfg=weights_cfg,
        game_counts_yesterday=dict(game_counts),
        big_news_flags=dict(big_news_flags),
        major_international_event_flag=any(str(b.get("sport")) == "major_international" for b in blobs),
    )

    runtime_target = float(weights_cfg.get("show_rules", {}).get("runtime_default_minutes", 40.0))
    allocations = allocate_show_time(
        sport_states=states,
        weights_cfg=weights_cfg,
        runtime_minutes_target=runtime_target,
    )

    by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for blob in blobs:
        by_type[str(blob.get("blob_type", "news"))].append(blob)

    allocation_order = [str(a.sport).strip() for a in allocations if getattr(a, "minutes", 0.0) > 0]

    def _balanced_select(items: list[dict[str, Any]], max_total: int, min_per_sport: int = 1, sport_cap: int | None = None) -> list[dict[str, Any]]:
        buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in items:
            buckets[str(item.get("sport", "")).strip()].append(item)
        selected: list[dict[str, Any]] = []
        counts: dict[str, int] = defaultdict(int)
        for sport in allocation_order:
            bucket = buckets.get(sport) or []
            while bucket and counts[sport] < min_per_sport and len(selected) < max_total:
                selected.append(bucket.pop(0))
                counts[sport] += 1
        active = sorted(
            [sport for sport, bucket in buckets.items() if bucket],
            key=lambda s: (allocation_order.index(s) if s in allocation_order else 999, -len(buckets[s])),
        )
        while len(selected) < max_total:
            progressed = False
            for sport in active:
                bucket = buckets.get(sport) or []
                if not bucket:
                    continue
                if sport_cap is not None and counts[sport] >= sport_cap:
                    continue
                selected.append(bucket.pop(0))
                counts[sport] += 1
                progressed = True
                if len(selected) >= max_total:
                    break
            if not progressed:
                break
        return selected[:max_total]

    cfg = get_settings()
    llm: LLMClient | None = None
    if bool(cfg.factpack_use_llm):
        try:
            llm = LLMClient()
        except Exception:
            llm = None
    logger.info(
        "factpack_agent_llm_status enabled=%s configured=%s endpoint_set=%s model=%s",
        bool(llm is not None),
        bool(cfg.factpack_use_llm),
        bool(getattr(llm, "endpoint", None)) if llm else False,
        str(getattr(llm, "model", "")) if llm else "",
    )
    print(
        "factpack_agent_llm_status",
        {
            "enabled": bool(llm is not None),
            "configured": bool(cfg.factpack_use_llm),
            "endpoint_set": bool(getattr(llm, "endpoint", None)) if llm else False,
            "model": str(getattr(llm, "model", "")) if llm else "",
        },
    )

    game_player_lines: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for blob in by_type.get("player_line", []):
        gid = str(((blob.get("entities") or {}).get("game_id") or ""))
        parsed = _parse_player_line(str(((blob.get("facts") or {}).get("typed_fields") or {}).get("player_line", "")))
        if gid and parsed:
            game_player_lines[gid].append(parsed)

    selected_game_blobs = _balanced_select(by_type.get("game_result", []), max_total=48, min_per_sport=1, sport_cap=12)
    yesterday_results: list[dict[str, Any]] = []
    for blob in selected_game_blobs:
        tf = ((blob.get("facts") or {}).get("typed_fields") or {})
        gid = str(((blob.get("entities") or {}).get("game_id") or ""))
        away = str(tf.get("away_team", "")).strip()
        home = str(tf.get("home_team", "")).strip()
        title = str(tf.get("title") or "").strip()
        if (not away or not home) and title:
            parsed_away, parsed_home = _parse_matchup_teams(title)
            away = away or parsed_away
            home = home or parsed_home
        away_score = int(tf.get("away_score", 0) or 0)
        home_score = int(tf.get("home_score", 0) or 0)
        key_players = sorted(game_player_lines.get(gid, []), key=lambda x: float(x.get("impact_score", 0.0)), reverse=True)[:3]
        display_title = title or f"{away} at {home}".strip()
        if not display_title and away and home:
            display_title = f"{away} at {home}"
        matchup_label = display_title or (f"{away} at {home}".strip() if away and home else "")
        score_line = f"{away_score}-{home_score}" if away_score or home_score else ""
        summary = f"Final score: {matchup_label}, {score_line}.".strip()
        summary = re.sub(r"\s+", " ", summary).replace(" ,", ",")

        yesterday_results.append(
            {
                "source_id": gid or str(blob.get("blob_id", "")),
                "sport": str(blob.get("sport", "")),
                "league": str(blob.get("league", "")),
                "title": display_title,
                "summary": summary,
                "status": "final",
                "citations": list(((blob.get("provenance") or {}).get("source_ids") or []),),
                "key_players": key_players,
                "impact_signals": {
                    "phase": str(((blob.get("labels") or {}).get("season_phase") or "REGULAR_SEASON")),
                    "winner": str(tf.get("winner", "")),
                    "loser": str(tf.get("loser", "")),
                    "winner_score": max(away_score, home_score),
                    "loser_score": min(away_score, home_score),
                    "margin": abs(home_score - away_score),
                    "total_points": away_score + home_score,
                    "playoff_implication": bool(tf.get("playoff_implication", False)),
                    "top_players": [str(p.get("name", "")) for p in key_players],
                    "top_player_impact_scores": [float(p.get("impact_score", 0.0)) for p in key_players],
                },
            }
        )

    season_trends: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for blob in by_type.get("team_trend", []):
        sport = str(blob.get("sport", ""))
        season_trends[sport].append(dict(((blob.get("facts") or {}).get("typed_fields") or {})))

    matchup_context_by_team: dict[str, dict[str, Any]] = {}
    for sport, rows in season_trends.items():
        for row in rows:
            team = str(row.get("name", "")).strip().upper()
            if team:
                matchup_context_by_team[f"{sport}:{team}"] = row

    selected_matchup_blobs = _balanced_select(by_type.get("matchup", []), max_total=40, min_per_sport=1, sport_cap=12)
    today_matchups: list[dict[str, Any]] = []
    for blob in selected_matchup_blobs:
        tf = ((blob.get("facts") or {}).get("typed_fields") or {})
        title = str(tf.get("title") or tf.get("headline") or blob.get("title") or "").strip()
        away = str(tf.get("away_team", "")).strip().upper()
        home = str(tf.get("home_team", "")).strip().upper()
        if not away or not home:
            parsed_away, parsed_home = _parse_matchup_teams(title)
            away = away or parsed_away
            home = home or parsed_home
        sport = str(blob.get("sport", ""))
        hc = matchup_context_by_team.get(f"{sport}:{home}", {})
        ac = matchup_context_by_team.get(f"{sport}:{away}", {})
        context = {
            "home_team": home,
            "away_team": away,
            "home_record": str(hc.get("record") or tf.get("home_record") or "N/A"),
            "away_record": str(ac.get("record") or tf.get("away_record") or "N/A"),
            "home_rank": hc.get("rank") if hc.get("rank") is not None else tf.get("home_rank"),
            "away_rank": ac.get("rank") if ac.get("rank") is not None else tf.get("away_rank"),
            "home_last10": str(hc.get("last10", "N/A")),
            "away_last10": str(ac.get("last10", "N/A")),
            "home_window_type": str(hc.get("window_type", "season_start_window")),
            "away_window_type": str(ac.get("window_type", "season_start_window")),
            "home_recent_record": str(hc.get("recent_record", "")),
            "away_recent_record": str(ac.get("recent_record", "")),
            "home_recent_games": int(hc.get("recent_games", 0) or 0),
            "away_recent_games": int(ac.get("recent_games", 0) or 0),
            "home_streak": str(hc.get("streak", "N/A")),
            "away_streak": str(ac.get("streak", "N/A")),
            "home_point_diff": hc.get("point_diff"),
            "away_point_diff": ac.get("point_diff"),
            "phase": str(((blob.get("labels") or {}).get("season_phase") or "REGULAR_SEASON")),
        }
        title = title or f"Upcoming: {away} at {home}"
        summary = _deterministic_matchup_summary(title, tf, context)
        if not away or not home:
            continue
        if str(context.get("home_record", "")).strip().upper() in {"", "N/A"}:
            continue
        if str(context.get("away_record", "")).strip().upper() in {"", "N/A"}:
            continue
        if not summary.strip():
            continue

        today_matchups.append(
            {
                "source_id": str(((blob.get("entities") or {}).get("game_id") or blob.get("blob_id") or "")),
                "sport": sport,
                "league": str(blob.get("league", "")),
                "title": title,
                "summary": summary,
                "start_time_et": str(blob.get("event_time_et", "")),
                "citations": list(((blob.get("provenance") or {}).get("source_ids") or [])),
                "matchup_context": context,
            }
        )

    major_news: list[dict[str, Any]] = []
    backfill_news: list[dict[str, Any]] = []
    seen_news_titles: set[str] = set()
    for blob in by_type.get("news", [])[:120]:
        tf = ((blob.get("facts") or {}).get("typed_fields") or {})
        title = _clean_news_title(str(tf.get("headline") or tf.get("title") or "").strip())
        if not title:
            continue
        if _title_source_blocked(title):
            continue

        source_ids = list(((blob.get("provenance") or {}).get("source_ids") or []))
        source_url = source_ids[0] if source_ids else ""
        if not _news_source_allowed(source_url):
            continue
        summary_raw = str(tf.get("summary") or "").strip()
        if not summary_raw or summary_raw.lower() in {"no summary provided", "no summary provided."}:
            summary_raw = title
        if not _major_news_card_allowed(title, summary_raw, source_url):
            continue
        news_text = f"{title} {summary_raw} {source_url}".lower()
        source_id = str(blob.get("blob_id", ""))
        if any(tok in news_text for tok in ("preview", "recap", "highlights", "best bets", "prediction", "odds", "remembering ", "look back", "viral stars", "meeting tracker", "how to watch", "best goals", "takeaways from day 1", "rumors, notes", "notes & how to watch", "best photos", "observations")):
            if _major_news_backfill_allowed(title, summary_raw, source_url):
                fallback_summary = _deterministic_news_summary(title, tf)
                fallback_points = _news_fact_points(title, tf)
                if not _news_summary_is_strong(fallback_summary, title, fallback_points):
                    continue
                backfill_news.append(
                    {
                        "source_id": source_id,
                        "sport": _infer_news_sport(title, summary_raw, source_url, str(blob.get("sport", ""))),
                        "title": title,
                        "summary": fallback_summary,
                        "score": float((blob.get("scoring") or {}).get("final_priority_score", 0.0)),
                        "tier": str(((blob.get("labels") or {}).get("importance") or "medium")),
                        "priority_reason": str(((blob.get("labels") or {}).get("category") or "other")),
                        "citations": source_ids,
                        "fact_points": fallback_points,
                    }
                )
            continue
        if SCORE_HEADLINE_RE.search(title) or SCORE_HEADLINE_RE.search(summary_raw):
            continue
        if "blowout loss" in news_text or "game " in title.lower():
            continue
        if any(token in news_text for token in NON_SPORTS_NEWS_TOKENS):
            continue
        if "no summary provided" in summary_raw.lower():
            continue

        inferred_sport = _infer_news_sport(title, summary_raw, source_url, str(blob.get("sport", "")))
        dedupe_key = re.sub(r"\s+", " ", title).strip().lower()
        if dedupe_key in seen_news_titles:
            continue
        seen_news_titles.add(dedupe_key)
        summary = _deterministic_news_summary(title, tf)
        if len(summary.split()) < 10:
            continue
        if "?" in summary:
            continue
        if any(token in summary.lower() for token in THIN_NEWS_PHRASES + LOW_VALUE_NEWS_TOKENS):
            continue
        if not _is_concrete_news_text(summary):
            continue
        fact_points = _news_fact_points(title, tf)
        card = {
            "source_id": source_id,
            "sport": inferred_sport,
            "title": title,
            "summary": summary,
            "score": float((blob.get("scoring") or {}).get("final_priority_score", 0.0)),
            "tier": str(((blob.get("labels") or {}).get("importance") or "medium")),
            "priority_reason": str(((blob.get("labels") or {}).get("category") or "other")),
            "citations": source_ids,
            "fact_points": fact_points,
        }
        soft_reject = False
        if len(summary.split()) < 10:
            soft_reject = True
        if "?" in summary:
            soft_reject = True
        if any(token in summary.lower() for token in THIN_NEWS_PHRASES + LOW_VALUE_NEWS_TOKENS):
            soft_reject = True
        if not _is_concrete_news_text(summary):
            soft_reject = True
        if soft_reject:
            if _major_news_backfill_allowed(title, summary, source_url):
                backfill_news.append(card)
            continue
        major_news.append(card)
        if _major_news_backfill_allowed(title, summary, source_url):
            backfill_news.append(card)

    # During combine window, enforce a deterministic combine/pro-day card even when upstream
    # provider/news classification does not surface one.
    try:
        y, m, day = [int(x) for x in str(run_date).split("-")]
        in_combine_window = (m == 2 and day >= 20) or (m == 3 and day <= 10)
    except Exception:
        in_combine_window = False
    if in_combine_window:
        has_combine = any(
            ("combine" in f"{str(n.get('title', ''))} {str(n.get('summary', ''))}".lower())
            or ("pro day" in f"{str(n.get('title', ''))} {str(n.get('summary', ''))}".lower())
            for n in major_news
        )
        if not has_combine:
            major_news.append(
                {
                    "source_id": f"combine-fallback-{run_date}",
                    "sport": "nfl",
                    "title": "NFL Combine update: testing results and pro-day watchlist",
                    "summary": (
                        "NFL combine testing is active in Indianapolis this week; "
                        "timed drills and measurables are shifting early draft boards, "
                        "and teams are prioritizing follow-up pro-day evaluations."
                    ),
                    "score": 0.95,
                    "tier": "high",
                    "priority_reason": "combine",
                    "citations": ["https://www.nfl.com/combine/"],
                    "fact_points": [
                        "The combine testing window is active in Indianapolis this week.",
                        "Measured athletic testing is influencing early draft-board movement.",
                        "Teams are using combine outputs to prioritize pro-day follow-up plans.",
                    ],
                }
            )

    major_news.sort(key=lambda n: float(n.get("score", 0.0)), reverse=True)
    target_news = min(12, len(major_news)) if len(major_news) < 12 else 12

    def _top_off_news(items: list[dict[str, Any]], pool: list[dict[str, Any]], max_total: int) -> list[dict[str, Any]]:
        if len(items) >= max_total:
            return items[:max_total]
        seen_ids = {str(item.get("source_id", "")) for item in items}
        for candidate in pool:
            source_id = str(candidate.get("source_id", ""))
            if source_id and source_id in seen_ids:
                continue
            items.append(candidate)
            if source_id:
                seen_ids.add(source_id)
            if len(items) >= max_total:
                break
        return items[:max_total]
    # Keep at least one combine/pro-day card in-window even if raw ranking would truncate it.
    if in_combine_window:
        ranked_pool = list(major_news)
        combine_cards = [
            n
            for n in ranked_pool
            if "combine" in f"{str(n.get('title', ''))} {str(n.get('summary', ''))}".lower()
            or "pro day" in f"{str(n.get('title', ''))} {str(n.get('summary', ''))}".lower()
        ]
        major_news = _balanced_select(ranked_pool, max_total=target_news, min_per_sport=0, sport_cap=4)
        major_news = _top_off_news(major_news, ranked_pool, target_news)
        if combine_cards:
            combine_id = str(combine_cards[0].get("source_id", ""))
            if not any(str(item.get("source_id", "")) == combine_id for item in major_news):
                major_news = [combine_cards[0]] + [
                    item for item in major_news if str(item.get("source_id", "")) != combine_id
                ]
                major_news = major_news[:target_news]
    else:
        ranked_pool = list(major_news)
        major_news = _balanced_select(major_news, max_total=target_news, min_per_sport=0, sport_cap=4)
        major_news = _top_off_news(major_news, ranked_pool, target_news)
    minimum_news_cards = 8
    if len(major_news) < minimum_news_cards:
        major_news = _top_off_news(list(major_news), sorted(backfill_news, key=lambda n: float(n.get("score", 0.0)), reverse=True), minimum_news_cards)
    if llm:
        today_matchups = [_rewrite_matchup_card(llm, card) for card in today_matchups]
        major_news = [_rewrite_news_card(llm, card) for card in major_news]
        logger.info(
            "factpack_agent_llm_diagnostics fallback_count=%s fallback_reasons=%s news_cards=%s matchup_cards=%s",
            int(getattr(llm, "fallback_count", 0)),
            list(getattr(llm, "fallback_reasons", [])),
            len(major_news),
            len(today_matchups),
        )
        print(
            "factpack_agent_llm_diagnostics",
            {
                "fallback_count": int(getattr(llm, "fallback_count", 0)),
                "fallback_reasons": list(getattr(llm, "fallback_reasons", [])),
                "news_cards": len(major_news),
                "matchup_cards": len(today_matchups),
            },
        )

    season_stats = {sport: {"teams": rows[:16]} for sport, rows in season_trends.items() if rows}

    season_outlooks: dict[str, Any] = {}
    for blob in by_type.get("outlook_signal", []):
        sport = str(blob.get("sport", ""))
        signal = str((((blob.get("facts") or {}).get("typed_fields") or {}).get("signal") or "")).strip()
        if signal:
            season_outlooks.setdefault(sport, {"bullets": []})
            season_outlooks[sport]["bullets"].append(signal)

    factpack = {
        "meta": {
            "process_date": run_date,
            "yesterday_window_et": {"start": y_start.isoformat(), "end": y_end.isoformat()},
            "today_date_et": run_date,
            "wpm_baseline": int(weights_cfg.get("show_rules", {}).get("wpm_baseline", 145)),
            "runtime_target_minutes": runtime_target,
        },
        "show_identity": _show_identity(),
        "coverage": {
            "ranking_order_locked": LOCKED_RANKING,
            "sport_states": [asdict(s) for s in states],
            "allocations": [asdict(a) for a in allocations],
        },
        "yesterday_results": yesterday_results,
        "today_matchups": today_matchups,
        "major_news": major_news,
        "season_stats": season_stats,
        "season_outlooks": season_outlooks,
        "flags": {
            "partial_coverage": len(yesterday_results) < 3 or len(today_matchups) < 2,
            "major_international_event": any(str(b.get("sport")) == "major_international" for b in blobs),
        },
    }

    schema = json.loads(Path("schemas/factpack.schema.json").read_text(encoding="utf-8"))
    jsonschema.validate(factpack, schema)
    return factpack


def build_notebook_docs(factpack: dict[str, Any]) -> dict[str, str]:
    process_date = factpack["meta"]["process_date"]
    identity = factpack["show_identity"]

    lines0 = [
        "# Show Identity - LOCKED",
        "",
        f"Show Name: {identity.get('name', '')}",
        "Primary Hosts:",
    ] + [f"- {h}" for h in (identity.get("hosts", {}).get("primary", []) or [])]

    lines_contract = [
        "# Prompt Contract",
        "",
        "Use only cited facts from the provided docs.",
        "Never rename show or hosts.",
        "No invented outcomes.",
    ]

    lines1 = [f"# Games - {process_date}", ""]
    for game in factpack.get("yesterday_results", []):
        lines1 += [
            f"## {game.get('sport', '').upper()} - {game.get('title', '')}",
            f"- Summary: {game.get('summary', '')}",
            "",
        ]

    lines2 = [f"# Player Stat Lines - {process_date}", ""]
    for game in factpack.get("yesterday_results", []):
        key_players = game.get("key_players") or []
        if not key_players:
            continue
        lines2.append(f"## {game.get('title', '')}")
        for player in key_players:
            lines2.append(f"- {player.get('line', '')}")
        lines2.append("")

    lines3 = [f"# Team Trends - {process_date}", ""]
    for sport, payload in (factpack.get("season_stats") or {}).items():
        lines3.append(f"## {sport}")
        for team in (payload.get("teams") or [])[:16]:
            window_type = str(team.get("window_type", "")).strip()
            recent = str(team.get("recent_record", "")).strip()
            recent_games = int(team.get("recent_games", 0) or 0)
            record = str(team.get("record", "N/A")).strip()
            streak = str(team.get("streak", "N/A")).strip()
            rank = team.get("rank")
            rank_prefix = f"No. {rank} " if isinstance(rank, int) and rank > 0 else ""
            trend_token = str(team.get("last10", "")).strip() if window_type == "last10" else f"{recent} ({recent_games}-game sample)"
            lines3.append(
                f"- {rank_prefix}{team.get('name', '')}: {record} | recent: {trend_token or 'N/A'} | streak: {streak} | diff: {team.get('point_diff', 'N/A')}"
            )
            trend_note = str(team.get("trend_note", "")).strip()
            if trend_note:
                lines3.append(f"  - trend: {trend_note}")
        lines3.append("")

    lines5 = [f"# Major News - {process_date}", ""]
    for news in (factpack.get("major_news") or []):
        lines5 += [
            f"## {news.get('sport', '').upper()} - {news.get('title', '')}",
            f"- Summary: {news.get('summary', '')}",
            "- Sources:",
        ]
        for citation in (news.get("citations") or [])[:4]:
            lines5.append(f"  - {citation}")
        lines5.append("")

    lines6 = [f"# Today's Matchups - {process_date}", ""]
    for matchup in (factpack.get("today_matchups") or []):
        ctx = matchup.get("matchup_context") or {}
        when = str(matchup.get("start_time_et", "")).strip()
        try:
            dt = datetime.fromisoformat(when)
            when = dt.strftime("%Y-%m-%d %-I:%M %p %Z")
        except Exception:
            pass
        away = str(ctx.get("away_team", "")).strip()
        home = str(ctx.get("home_team", "")).strip()
        away_record = str(ctx.get("away_record", "N/A")).strip()
        home_record = str(ctx.get("home_record", "N/A")).strip()
        away_window_type = str(ctx.get("away_window_type", "")).strip()
        home_window_type = str(ctx.get("home_window_type", "")).strip()
        away_rank = ctx.get("away_rank")
        home_rank = ctx.get("home_rank")
        away_label = f"No. {away_rank} {away}" if isinstance(away_rank, int) and away_rank > 0 else away
        home_label = f"No. {home_rank} {home}" if isinstance(home_rank, int) and home_rank > 0 else home
        record_line_label = "- Records"
        if away_window_type != "overall" or home_window_type != "overall":
            record_line_label = "- Recent form"
        lines6 += [
            f"## {matchup.get('sport', '').upper()} - {matchup.get('title', '')}",
            f"- Time (ET): {when}",
            f"{record_line_label}: {away_label} {away_record} at {home_label} {home_record}",
            f"- Summary: {matchup.get('summary', '')}",
            "",
        ]

    lines7 = [f"# Season Outlooks - {process_date}", ""]
    season_outlooks = factpack.get("season_outlooks") or {}
    if not season_outlooks:
        lines7.append("_No season outlook segments triggered today._")
    for sport, payload in season_outlooks.items():
        lines7.append(f"## {sport}")
        for bullet in payload.get("bullets", [])[:10]:
            lines7.append(f"- {bullet}")
        lines7.append("")

    lines8 = [f"# Editorial Notes + Coverage Plan - {process_date}", "", "## Coverage Allocations (minutes)"]
    for allocation in (factpack.get("coverage", {}).get("allocations") or []):
        lines8.append(
            f"- {allocation.get('sport')}: {float(allocation.get('minutes', 0.0)):.1f} min ({allocation.get('phase')})"
        )

    return {
        "00_show_contract.md": "\n".join(lines0) + "\n",
        "00_prompt_contract.md": "\n".join(lines_contract) + "\n",
        "01_games.md": "\n".join(lines1) + "\n",
        "02_player_stats.md": "\n".join(lines2) + "\n",
        "03_team_trends.md": "\n".join(lines3) + "\n",
        "05_major_news.md": "\n".join(lines5) + "\n",
        "06_today_matchups.md": "\n".join(lines6) + "\n",
        "07_season_outlooks.md": "\n".join(lines7) + "\n",
        "08_editorial_notes.md": "\n".join(lines8) + "\n",
    }
