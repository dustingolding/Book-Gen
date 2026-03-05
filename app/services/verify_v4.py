from __future__ import annotations

import json
import re
from collections import Counter

import jsonschema

GENERIC_PATTERNS = (
    "coverage threshold",
    "confidence threshold",
    "runbook",
    "verified slate",
    "highest-leverage matchups",
    "pivots to watch",
    "underlying trend sample",
    "editorial filter",
    "scheduled matchup with in-season relevance",
    "seeding pressure is increasing",
    "what matters next",
    "the opener starts with",
    "recent form is",
    "sits in the regular season portion of the calendar",
    "sits in the postseason portion of the calendar",
    "the swing performers were",
)

SECTION_SOURCE_REQUIRED = {"cold_open", "scoreboard_roundup", "major_storylines", "today_matchups"}


def _runtime_minutes(script: dict) -> float:
    words = []
    for sec in script.get("sections", []):
        for turn in sec.get("turns", []):
            words.extend(re.findall(r"\b[\w']+\b", str(turn.get("text", ""))))
    wpm = int(script.get("meta", {}).get("wpm_baseline", 145))
    return round((len(words) / max(wpm, 1)), 2)


def _identity_ok(script: dict, factpack: dict) -> bool:
    script_name = str(script.get("meta", {}).get("show_name", ""))
    factpack_name = str(factpack.get("show_identity", {}).get("name", ""))
    script_hosts = list(script.get("meta", {}).get("hosts", []) or [])
    factpack_hosts = list((factpack.get("show_identity", {}).get("hosts", {}) or {}).get("primary", []) or [])
    return script_name == factpack_name and script_hosts[:2] == factpack_hosts[:2]


def _normalize(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(text or "").lower())


def _title_pairs(items: list[dict], strip_upcoming: bool = False) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for item in items:
        raw = str(item.get("title", "")).strip()
        if strip_upcoming:
            raw = re.sub(r"^Upcoming:\s*", "", raw, flags=re.IGNORECASE)
        m = re.match(r"^(.*?)\s+at\s+(.*)$", raw, flags=re.IGNORECASE)
        if not m:
            continue
        away = _normalize(m.group(1))
        home = _normalize(m.group(2))
        if away and home:
            pairs.append((away, home))
    return pairs


def _boilerplate_hits(script: dict) -> int:
    hits = 0
    for sec in script.get("sections", []):
        for turn in sec.get("turns", []):
            low = str(turn.get("text", "")).strip().lower()
            if any(pattern in low for pattern in GENERIC_PATTERNS):
                hits += 1
    return hits


def _turn_metrics(script: dict) -> tuple[int, int, int, int]:
    total = 0
    unsourced = 0
    duplicate = 0
    low_fact_density = 0
    seen = Counter()
    for sec in script.get("sections", []):
        sec_name = str(sec.get("name", "")).strip()
        for turn in sec.get("turns", []):
            total += 1
            text = str(turn.get("text", "")).strip()
            norm = _normalize(text)
            if norm:
                seen[norm] += 1
            sources = [str(x).strip() for x in (turn.get("source_ids") or []) if str(x).strip()]
            if sec_name in SECTION_SOURCE_REQUIRED and not sources:
                unsourced += 1
            numerics = len(re.findall(r"\b\d+(?:\.\d+)?\b", text))
            capitals = len(re.findall(r"\b(?:[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?|[A-Z]{2,4})\b", text))
            if sec_name in {"scoreboard_roundup", "major_storylines", "today_matchups"} and numerics + capitals < 3:
                low_fact_density += 1
    duplicate = sum(1 for _, count in seen.items() if count > 1)
    return total, unsourced, duplicate, low_fact_density


def _supported_reference_failures(script: dict, factpack: dict) -> int:
    score_pairs = _title_pairs(list(factpack.get("yesterday_results") or []))
    matchup_pairs = _title_pairs(list(factpack.get("today_matchups") or []), strip_upcoming=True)
    failures = 0
    for sec in script.get("sections", []):
        sec_name = str(sec.get("name", "")).strip()
        turns = [str(turn.get("text", "")) for turn in sec.get("turns", [])]
        joined = _normalize(" ".join(turns))
        if sec_name == "scoreboard_roundup" and score_pairs:
            if not any(away in joined and home in joined for away, home in score_pairs):
                failures += 1
        if sec_name == "today_matchups" and matchup_pairs:
            if not any(away in joined and home in joined for away, home in matchup_pairs):
                failures += 1
    return failures


def verify_script(process_date: str, script: dict, factpack: dict) -> dict:
    runtime = _runtime_minutes(script)
    issues: list[str] = []
    target_runtime = float(script.get("meta", {}).get("target_runtime_minutes", 40.0) or 40.0)
    runtime_floor = max(22.0, round(target_runtime * 0.70, 2))

    if not _identity_ok(script, factpack):
        issues.append("identity_integrity_failed")

    total_turns, unsourced_turns, duplicate_turns, low_fact_density = _turn_metrics(script)
    boilerplate_hits = _boilerplate_hits(script)
    unsupported_refs = _supported_reference_failures(script, factpack)

    if total_turns < 12:
        issues.append("turn_count_below_minimum")
    if unsourced_turns > 0:
        issues.append("unsourced_turns_present")
    if duplicate_turns > 0:
        issues.append("duplicate_turn_text_present")
    if boilerplate_hits > 0:
        issues.append("repetitive_boilerplate_detected")
    if low_fact_density > max(1, int(total_turns * 0.10)):
        issues.append("fact_density_too_low")
    if unsupported_refs > 0:
        issues.append("section_reference_not_grounded")
    if runtime < runtime_floor:
        issues.append("runtime_below_minimum")

    status = "pass" if not issues else "fail"
    report = {
        "status": status,
        "metrics": {
            "runtime_minutes": runtime,
            "section_count": len(script.get("sections", [])),
            "turn_count": total_turns,
            "boilerplate_hits": boilerplate_hits,
            "unsourced_turns": unsourced_turns,
            "duplicate_turns": duplicate_turns,
            "low_fact_density_turns": low_fact_density,
            "unsupported_reference_turns": unsupported_refs,
            "runtime_floor_minutes": runtime_floor,
        },
        "issues": [{"code": i} for i in issues],
    }

    schema = json.loads(open("schemas/qa_report.schema.json", "r", encoding="utf-8").read())
    jsonschema.validate(report, schema)
    return report
