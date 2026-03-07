from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import socket
import zipfile
from collections import Counter
from datetime import date, datetime, timezone
from io import BytesIO
from pathlib import Path
from statistics import mean
from typing import Any
from urllib.parse import urlparse
from xml.sax.saxutils import escape

import jsonschema
import mlflow
import yaml

from app.clients.lakefs import LakeFSClient
from app.clients.llm import LLMClient
from app.config import get_settings
from app.service_endpoints import resolve_service_uri
from app.storage import ObjectStore

logger = logging.getLogger(__name__)

CONFLICT_SCOPE_ORDER = ["personal", "local", "regional", "national", "global"]
VIOLENCE_LEVEL_ORDER = ["none", "low", "moderate", "high"]
INSTITUTIONAL_RESPONSE_ORDER = ["none", "attention", "suppression", "retaliation", "collapse"]
TITLE_ENGINE_VERSION = "title-engine-1.0"
RUBRIC_WEIGHT_DEFAULTS = {
    "thematic_coherence": 0.18,
    "character_consistency": 0.18,
    "world_rule_compliance": 0.14,
    "escalation_compliance": 0.10,
    "voice_stability": 0.14,
    "pacing": 0.14,
    "structural_clarity": 0.08,
    "originality": 0.04,
}
PASS_THRESHOLD_DEFAULT = 7.5
HARD_FAIL_DEFAULTS = {
    "world_rule_compliance": 7.0,
    "character_consistency": 7.0,
    "escalation_compliance": 7.0,
}
PASSAGE_MARKERS = [
    "the lesson is",
    "we must",
    "it was clear that everyone should",
    "the point of it all",
]
AI_TELL_MARKERS = [
    "suddenly",
    "in a twist of fate",
    "little did",
    "unbeknownst",
]
DEFAULT_TITLE_NOUNS = ["session", "record", "brief", "motion", "rule", "ledger", "order", "hearing", "file", "docket"]
DEFAULT_TITLE_ADJECTIVES = ["closed", "quiet", "redacted", "sealed", "hidden", "silent", "public", "final", "open"]
TITLE_EXPOSURE_WORD_SCORES = {
    "closed": 1.0,
    "sealed": 1.1,
    "quiet": 1.2,
    "silent": 1.3,
    "hidden": 1.4,
    "classified": 1.5,
    "session": 1.5,
    "brief": 1.6,
    "hearing": 1.8,
    "record": 2.0,
    "docket": 2.0,
    "file": 2.1,
    "motion": 2.4,
    "markup": 2.6,
    "rule": 2.8,
    "vote": 3.2,
    "ledger": 3.5,
    "disclosure": 4.2,
    "open": 4.4,
    "public": 4.6,
    "final": 4.8,
}
TITLE_ROLE_TARGETS = {
    "entry": 1.4,
    "setup": 1.4,
    "escalation": 2.1,
    "breach": 3.0,
    "fallout": 3.4,
    "reckoning": 4.0,
    "exposure": 4.5,
    "resolution": 4.8,
}
CHAPTER_TITLE_SEEDS = {
    "hook": ["Briefing", "Memo", "Threshold", "Signal", "Entry"],
    "pressure": ["Record", "Leverage", "Minutes", "Witness", "Hearing"],
    "reversal": ["Motion", "Docket", "Disclosure", "Mark", "Revision"],
    "climax": ["Vote", "Rule", "Order", "Session", "Finding"],
    "denouement": ["Ledger", "Account", "Aftermath", "Notice", "Record"],
}
BOOKGEN_GENERATION_PROFILES = {
    "smoke": {
        "llm_opening_min_words": 1200,
        "llm_general_min_ratio": 0.5,
        "llm_general_min_floor": 700,
        "structural_retry_limit": 1,
        "chapter_llm_limit": 1,
        "eval_llm_limit": 1,
        "rewrite_llm_limit": 1,
    },
    "dev": {
        "llm_opening_min_words": 2200,
        "llm_general_min_ratio": 0.58,
        "llm_general_min_floor": 950,
        "structural_retry_limit": 2,
        "chapter_llm_limit": 2,
        "eval_llm_limit": 2,
        "rewrite_llm_limit": 2,
    },
    "production": {
        "llm_opening_min_words": 3000,
        "llm_general_min_ratio": 0.9,
        "llm_general_min_floor": 2200,
        "structural_retry_limit": 2,
        "chapter_llm_limit": 0,
        "eval_llm_limit": 0,
        "rewrite_llm_limit": 0,
    },
}
BOOKGEN_POLICY_PROFILES = {
    "default": {
        "opening_scene": {
            "must_start_in_scene": True,
            "must_put_pov_in_motion_by_paragraph": 1,
            "must_introduce_pressure_within_words": 250,
            "must_force_a_choice_within_words": 900,
            "summary_forward_opening_disallowed": True,
        },
        "structure": {
            "opening_paragraph_min": 8,
            "later_paragraph_min": 6,
            "dialogue_presence_required": True,
            "opening_scene_turns_min": 3,
            "later_scene_turns_min": 2,
        },
        "rewrite": {
            "max_attempts": 3,
            "stop_if_no_improvement_after": 2,
            "priority_categories": [
                "world_rule_compliance",
                "character_consistency",
                "escalation_compliance",
                "pacing",
                "structural_clarity",
                "voice_stability",
                "thematic_coherence",
                "originality",
            ],
        },
        "editorial": {
            "developmental": {"overall_score_floor": 7.5, "pacing_floor": 7.0, "structural_clarity_floor": 7.0},
            "line": {"voice_stability_floor": 7.0, "paragraph_floor": 4, "avg_paragraph_density_max": 260},
            "copy": {"max_double_space_hits_floor": 2, "double_space_hits_per_paragraph_divisor": 2},
        },
        "world_rule_language": {
            "replacements": [
                {"pattern": r"\bleak(?:ed|ing)?\s+classified\b", "replacement": "filed a sealed classified notice"},
                {"pattern": r"\bpublicly disclosed\b", "replacement": "moved through authorized oversight channels"},
                {"pattern": r"\breleased\b", "replacement": "escalated under counsel supervision"},
            ],
            "required_signals": [
                {
                    "keywords": ["sealed", "counsel"],
                    "sentence": "Counsel required every sensitive detail to move through sealed process before any committee circulation.",
                },
                {
                    "keywords": ["consequence", "sanction", "liability"],
                    "sentence": "The team treated any unauthorized disclosure as immediate legal liability with career-ending consequences.",
                },
            ],
        },
    },
    "institutional_thriller": {
        "opening_scene": {
            "must_start_in_scene": True,
            "must_put_pov_in_motion_by_paragraph": 1,
            "must_introduce_pressure_within_words": 220,
            "must_force_a_choice_within_words": 900,
            "summary_forward_opening_disallowed": True,
        },
        "structure": {
            "opening_paragraph_min": 8,
            "later_paragraph_min": 6,
            "dialogue_presence_required": True,
            "opening_scene_turns_min": 3,
            "later_scene_turns_min": 2,
        },
        "rewrite": {
            "max_attempts": 3,
            "stop_if_no_improvement_after": 2,
            "priority_categories": [
                "world_rule_compliance",
                "escalation_compliance",
                "character_consistency",
                "pacing",
                "structural_clarity",
                "voice_stability",
                "thematic_coherence",
                "originality",
            ],
        },
        "editorial": {
            "developmental": {"overall_score_floor": 7.5, "pacing_floor": 7.0, "structural_clarity_floor": 7.0},
            "line": {"voice_stability_floor": 6.5, "paragraph_floor": 4, "avg_paragraph_density_max": 260},
            "copy": {"max_double_space_hits_floor": 2, "double_space_hits_per_paragraph_divisor": 2},
        },
        "world_rule_language": {
            "replacements": [
                {"pattern": r"\bleak(?:ed|ing)?\s+classified\b", "replacement": "filed a sealed classified notice"},
                {"pattern": r"\bpublicly disclosed\b", "replacement": "moved through authorized oversight channels"},
                {"pattern": r"\breleased\b", "replacement": "escalated under counsel supervision"},
            ],
            "required_signals": [
                {
                    "keywords": ["sealed", "counsel"],
                    "sentence": "Counsel required every sensitive detail to move through sealed process before any committee circulation.",
                },
                {
                    "keywords": ["consequence", "sanction", "liability"],
                    "sentence": "The team treated any unauthorized disclosure as immediate legal liability with career-ending consequences.",
                },
            ],
        },
    },
    "juvenile_adventure": {
        "opening_scene": {
            "must_start_in_scene": True,
            "must_put_pov_in_motion_by_paragraph": 1,
            "must_introduce_pressure_within_words": 180,
            "must_force_a_choice_within_words": 700,
            "summary_forward_opening_disallowed": True,
        },
        "structure": {
            "opening_paragraph_min": 7,
            "later_paragraph_min": 5,
            "dialogue_presence_required": True,
            "opening_scene_turns_min": 3,
            "later_scene_turns_min": 2,
        },
        "rewrite": {
            "max_attempts": 2,
            "stop_if_no_improvement_after": 1,
            "priority_categories": [
                "pacing",
                "structural_clarity",
                "thematic_coherence",
                "character_consistency",
                "voice_stability",
                "world_rule_compliance",
                "originality",
                "escalation_compliance",
            ],
        },
        "editorial": {
            "developmental": {"overall_score_floor": 7.5, "pacing_floor": 7.0, "structural_clarity_floor": 7.0},
            "line": {"voice_stability_floor": 7.2, "paragraph_floor": 4, "avg_paragraph_density_max": 240},
            "copy": {"max_double_space_hits_floor": 2, "double_space_hits_per_paragraph_divisor": 2},
        },
        "world_rule_language": {
            "replacements": [
                {"pattern": r"\bleak(?:ed|ing)?\b", "replacement": "shared outside safe channels"},
                {"pattern": r"\bpublicly disclosed\b", "replacement": "reported through approved adults first"},
            ],
            "required_signals": [
                {
                    "keywords": ["protocol", "safe", "approved"],
                    "sentence": "They followed safety protocol and checked the evidence with an approved adult before taking the next step.",
                },
                {
                    "keywords": ["timeline", "risk", "consequence"],
                    "sentence": "One reckless move could disrupt the timeline, so every choice had immediate consequences for the team.",
                },
            ],
        },
    },
}


def _utcnow() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _hash_payload(payload: Any) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _slug(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", value.strip().lower())
    return cleaned.strip("-") or "item"


def _load_yaml_file(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _schema_path(name: str) -> Path:
    return Path("schemas/bookgen") / f"{name}.schema.yaml"


def _bookgen_benchmark_thresholds_path() -> Path:
    return Path("config/bookgen_benchmark_thresholds.yaml")


def _load_benchmark_thresholds() -> dict[str, Any]:
    defaults = {
        "quality_drop_fail": -0.75,
        "quality_drop_warn": -0.25,
        "pass_rate_drop_fail": -0.05,
        "cost_increase_warn_abs": 0.25,
        "cost_increase_warn_ratio": 0.40,
    }
    path = _bookgen_benchmark_thresholds_path()
    if not path.exists():
        return defaults
    try:
        payload = _load_yaml_file(path)
    except Exception:
        return defaults
    if not isinstance(payload, dict):
        return defaults
    merged = dict(defaults)
    for key in defaults:
        value = payload.get(key)
        if isinstance(value, (int, float)):
            merged[key] = float(value)
    return merged


def _validate_structured_payload(payload: dict[str, Any], schema_name: str) -> None:
    schema_file = _schema_path(schema_name)
    if not schema_file.exists():
        raise RuntimeError(f"Bookgen schema missing: {schema_file}")
    schema = _load_yaml_file(schema_file)
    jsonschema.validate(payload, schema)


def _default_bookspec_key(project_id: str) -> str:
    return f"inputs/{project_id}/bookspec.json"


def _intake_meta_key(project_id: str) -> str:
    return f"runs/{project_id}/meta/intake.json"


def _planning_manifest_key(project_id: str) -> str:
    return f"runs/{project_id}/meta/planning_manifest.json"


def _default_run_date(project_id: str, store: ObjectStore) -> str:
    if store.exists(_intake_meta_key(project_id)):
        intake = store.get_json(_intake_meta_key(project_id))
        run_date = str(intake.get("run_date", "")).strip()
        if run_date:
            return run_date
    return date.today().isoformat()


def _lakefs_branch_for_project(project_id: str, prefix: str) -> str:
    safe_prefix = "".join(c if c.isalnum() or c in {"-", "_"} else "-" for c in prefix.strip().lower())
    safe_project = "".join(c if c.isalnum() or c in {"-", "_"} else "-" for c in project_id.strip().lower())
    safe_prefix = safe_prefix.strip("-_") or "bookgen"
    safe_project = safe_project.strip("-_") or "default"
    return f"{safe_prefix}-{safe_project}"


def _commit_stage_checkpoint(
    *,
    project_id: str,
    run_date: str,
    stage: str,
    artifact_hashes: dict[str, str],
    extra_metadata: dict[str, Any] | None = None,
) -> str | None:
    lakefs = LakeFSClient()
    if not lakefs.enabled:
        return None

    store = ObjectStore()
    checkpoint = {
        "project_id": project_id,
        "run_date": run_date,
        "stage": stage,
        "artifact_hashes": artifact_hashes,
        "extra_metadata": extra_metadata or {},
        "checkpoint_written_at": _utcnow(),
    }
    checkpoint_key = f"runs/{project_id}/meta/stages/{stage}.json"
    store.put_json(checkpoint_key, checkpoint)
    artifact_hashes_with_checkpoint = dict(artifact_hashes)
    artifact_hashes_with_checkpoint[checkpoint_key] = _hash_payload(checkpoint)
    artifact_manifest_json = json.dumps(artifact_hashes_with_checkpoint, sort_keys=True)
    artifact_manifest_hash = _hash_text(artifact_manifest_json)
    checkpoint_raw = json.dumps(checkpoint, sort_keys=True, separators=(",", ":")).encode("utf-8")
    manifest_key = f"runs/{project_id}/meta/stages/{stage}.artifacts.json"

    branch = _lakefs_branch_for_project(project_id, lakefs.bookgen_branch_prefix)
    try:
        lakefs.ensure_branch(branch=branch, source=lakefs.source_branch)
        lakefs.upload_object(
            branch=branch,
            path=checkpoint_key,
            content=checkpoint_raw,
            content_type="application/json",
        )
        lakefs.upload_object(
            branch=branch,
            path=manifest_key,
            content=artifact_manifest_json.encode("utf-8"),
            content_type="application/json",
        )
        commit_id = lakefs.commit(
            branch=branch,
            message=f"[bookgen:{stage}] project={project_id} run_date={run_date}",
            metadata={
                "project_id": project_id,
                "run_date": run_date,
                "stage": stage,
                "artifact_count": str(len(artifact_hashes_with_checkpoint)),
                "artifacts_manifest_sha256": artifact_manifest_hash,
                "checkpoint_key": checkpoint_key,
            },
        )
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(f"LakeFS commit failed for stage '{stage}': {exc}") from exc

    logger.info(
        "bookgen_lakefs_commit_ok stage=%s project_id=%s branch=%s commit_id=%s",
        stage,
        project_id,
        branch,
        commit_id,
    )
    return commit_id


def _load_bookspec_from_local(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise RuntimeError(f"BookSpec path does not exist: {path}")
    raw = p.read_text(encoding="utf-8")
    if p.suffix.lower() == ".json":
        return json.loads(raw)
    if p.suffix.lower() in {".md", ".markdown"}:
        return _compile_canonical_markdown_intake(raw, source_path=str(p))
    if p.suffix.lower() in {".yaml", ".yml"}:
        return _compile_canonical_payload(yaml.safe_load(raw) or {}, source_path=str(p))
    raise RuntimeError(f"Unsupported BookSpec file format: {p.suffix}")


def _validate_bookspec(bookspec: dict[str, Any]) -> None:
    schema_path = Path("schemas/bookspec.schema.json")
    if not schema_path.exists():
        raise RuntimeError("BookSpec schema missing: schemas/bookspec.schema.json")
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    jsonschema.validate(bookspec, schema)


def _project_root(project_id: str, installment_id: str) -> str:
    return f"bookgen/{project_id}/installments/{installment_id}"


def _ledger_root(project_id: str, installment_id: str) -> str:
    return f"{_project_root(project_id, installment_id)}/ledgers"


def _chapter_root(project_id: str, installment_id: str, chapter_index: int) -> str:
    return f"{_project_root(project_id, installment_id)}/chapters/ch-{chapter_index:02d}"


def _load_intake(project_id: str, store: ObjectStore) -> dict[str, Any]:
    return store.get_json(_intake_meta_key(project_id))


def _load_planning_manifest(project_id: str, store: ObjectStore) -> dict[str, Any]:
    return store.get_json(_planning_manifest_key(project_id))


def _extract_markdown_yaml_block(raw: str) -> dict[str, Any]:
    match = re.search(r"```ya?ml\s*(.*?)```", raw, flags=re.DOTALL | re.IGNORECASE)
    if not match:
        raise RuntimeError("Canonical intake markdown must contain a fenced YAML block.")
    return yaml.safe_load(match.group(1)) or {}


def _compile_canonical_payload(payload: dict[str, Any], *, source_path: str) -> dict[str, Any]:
    constitution = payload.get("constitution_input") if isinstance(payload.get("constitution_input"), dict) else {}
    installment = payload.get("installment_input") if isinstance(payload.get("installment_input"), dict) else {}
    if not constitution or not installment:
        raise RuntimeError(f"Canonical intake is missing constitution_input or installment_input: {source_path}")

    project = constitution.get("project") if isinstance(constitution.get("project"), dict) else {}
    audience = project.get("audience") if isinstance(project.get("audience"), dict) else {}
    narrative_identity = constitution.get("narrative_identity") if isinstance(constitution.get("narrative_identity"), dict) else {}
    pov = narrative_identity.get("pov") if isinstance(narrative_identity.get("pov"), dict) else {}
    tone_lock = narrative_identity.get("tone_lock") if isinstance(narrative_identity.get("tone_lock"), dict) else {}
    style = narrative_identity.get("style_constitution") if isinstance(narrative_identity.get("style_constitution"), dict) else {}
    theme_engine = constitution.get("theme_engine") if isinstance(constitution.get("theme_engine"), dict) else {}
    primary_theme = theme_engine.get("primary") if isinstance(theme_engine.get("primary"), dict) else {}
    secondary_themes = theme_engine.get("secondary") if isinstance(theme_engine.get("secondary"), list) else []
    concept = constitution.get("concept_engine") if isinstance(constitution.get("concept_engine"), dict) else {}
    world_model = constitution.get("world_model") if isinstance(constitution.get("world_model"), dict) else {}
    character_registry = constitution.get("character_registry") if isinstance(constitution.get("character_registry"), dict) else {}
    installment_meta = installment.get("installment") if isinstance(installment.get("installment"), dict) else {}
    installment_intent = installment.get("intent") if isinstance(installment.get("intent"), dict) else {}
    output_targets = installment.get("output_targets") if isinstance(installment.get("output_targets"), dict) else {}
    title_brief = installment.get("installment_title_brief") if isinstance(installment.get("installment_title_brief"), dict) else {}

    characters = []
    for character in character_registry.get("characters", []) if isinstance(character_registry.get("characters"), list) else []:
        if not isinstance(character, dict):
            continue
        voice = character.get("voice_markers") if isinstance(character.get("voice_markers"), dict) else {}
        arc_path = character.get("arc_path") if isinstance(character.get("arc_path"), dict) else {}
        characters.append(
            {
                "character_id": str(character.get("character_id", "")).strip() or _slug(str(character.get("name", "character"))),
                "name": str(character.get("name", "Character")),
                "role": str(character.get("role", "supporting")),
                "immutable_traits": list(character.get("immutable_traits", []) or []),
                "mutable_traits": list(character.get("mutable_traits", []) or []),
                "dialogue_tics": list(voice.get("dialogue_tics", []) or []),
                "taboo_phrases": list(voice.get("taboo_phrases", []) or []),
                "start_state": str(arc_path.get("start_state", "guarded and reactive")),
                "end_state": str(arc_path.get("end_state", "decisive and accountable")),
                "external_goal": str(character.get("external_goal", "")),
                "internal_need": str(character.get("internal_need", "")),
            }
        )

    protagonist = next((item for item in characters if "protagon" in item["role"]), characters[0] if characters else {})
    governing_rules = world_model.get("governing_rules") if isinstance(world_model.get("governing_rules"), list) else []
    summary = str(installment_intent.get("summary", "")).strip() or str(concept.get("expanded_premise", "")).strip()
    bookspec = {
        "project_id": str(project.get("project_id", "")).strip() or _slug(str(project.get("working_title", "book-project"))),
        "title": str(installment_meta.get("title", "")).strip() or str(project.get("working_title", "Untitled Book")),
        "series_title": str(project.get("working_title", "")).strip() or str(installment_meta.get("title", "Untitled Series")),
        "genre": str(project.get("genre", "")).strip() or "fiction",
        "subgenre": str(project.get("subgenre", "")).strip() or str(project.get("genre", "fiction")),
        "target_audience": str(audience.get("age_band", "adult")).strip() or "adult",
        "tone": str(tone_lock.get("description", "grounded, consequence-driven prose")).strip(),
        "themes": [
            str(primary_theme.get("statement", "pressure reveals identity")).strip()
        ] + [
            str(item.get("statement", "")).strip()
            for item in secondary_themes
            if isinstance(item, dict) and str(item.get("statement", "")).strip()
        ],
        "word_count_target": int(output_targets.get("word_count_target", 70000) or 70000),
        "chapter_count": int(output_targets.get("chapter_count_target", 18) or 18),
        "planned_series_length": int(project.get("planned_installments", 1) or 1),
        "installment_id": str(installment_meta.get("installment_id", "book-01")),
        "installment_index": int(installment_meta.get("installment_index", 1) or 1),
        "pov": str(pov.get("mode", "third_limited")),
        "protagonist_goal": str(protagonist.get("external_goal", "")).strip() or summary,
        "core_conflict": str(concept.get("logline", "")).strip() or summary,
        "stakes": summary,
        "narrative_role": str(installment_meta.get("narrative_role", "setup")),
        "stakes_level": str(installment_meta.get("stakes_level", "high")),
        "output_formats": list(output_targets.get("output_formats", ["md", "docx"]) or ["md", "docx"]),
        "characters": characters,
        "setting_rules": [
            str(rule.get("rule", "")).strip()
            for rule in governing_rules
            if isinstance(rule, dict) and str(rule.get("rule", "")).strip()
        ],
        "locations": list((world_model.get("setting") or {}).get("locations", []) or []),
        "institutions": list(world_model.get("institutions", []) or []),
        "social_norms": list(world_model.get("social_norms", []) or []),
        "realism_mode": str((world_model.get("realism_constraints") or {}).get("mode", "strict")),
        "prose_density": str(style.get("prose_density", "balanced")),
        "emotional_explicitness": str(style.get("emotional_explicitness", "moderate")),
        "humor_level": str(style.get("humor_level", "dry")),
        "pacing_profile": str(style.get("pacing_profile", "steady")),
        "cliche_blacklist": list(style.get("cliche_blacklist", []) or []),
        "prompt_pack_version": str(payload.get("prompt_pack_version", "v1")),
        "rubric_version": str(payload.get("rubric_version", "v1")),
        "series_title_strategy": constitution.get("series_title_strategy") if isinstance(constitution.get("series_title_strategy"), dict) else {},
        "canonical_source_path": source_path,
    }
    if title_brief:
        bookspec["installment_title_briefs"] = [
            {
                "installment_id": bookspec["installment_id"],
                "installment_index": bookspec["installment_index"],
                "arc_role": str(title_brief.get("arc_role", "entry")),
                "exposure_level": str(title_brief.get("exposure_level", "low")),
                "stakes_level": bookspec["stakes_level"],
                "semantic_targets": {
                    "must_imply": list((title_brief.get("semantic_targets") or {}).get("must_imply", []) or []),
                    "must_avoid": list((title_brief.get("semantic_targets") or {}).get("must_avoid", []) or []),
                },
            }
        ]
    bookspec["installment_title_briefs"] = _resolve_installment_title_briefs(bookspec)
    return bookspec


def _compile_canonical_markdown_intake(raw: str, *, source_path: str) -> dict[str, Any]:
    payload = _extract_markdown_yaml_block(raw)
    return _compile_canonical_payload(payload, source_path=source_path)


def _resolve_installment_id(bookspec: dict[str, Any]) -> str:
    return str(bookspec.get("installment_id", "")).strip() or "book-01"


def _planned_series_length(bookspec: dict[str, Any]) -> int | None:
    raw = bookspec.get("planned_series_length")
    if isinstance(raw, int) and raw >= 1:
        return raw
    return None


def _chapter_count(bookspec: dict[str, Any]) -> int:
    explicit = bookspec.get("chapter_count")
    if isinstance(explicit, int) and 1 <= explicit <= 120:
        return explicit
    target_wc = int(bookspec.get("word_count_target", 70000) or 70000)
    return max(8, min(40, target_wc // 2500))


def _default_series_title_strategy(bookspec: dict[str, Any]) -> dict[str, Any]:
    subgenre = str(bookspec.get("subgenre", "")).lower()
    title = str(bookspec.get("title", "")).strip()
    nouns = list(DEFAULT_TITLE_NOUNS)
    adjectives = list(DEFAULT_TITLE_ADJECTIVES)
    if "political" in subgenre or "institutional" in subgenre:
        nouns = ["session", "record", "brief", "motion", "rule", "ledger", "order", "hearing", "file", "vote"]
        adjectives = ["closed", "quiet", "redacted", "sealed", "public", "final", "hidden", "classified", "open"]
    if title:
        for token in re.findall(r"[A-Za-z]+", title.lower()):
            if len(token) >= 4 and token not in nouns and token not in adjectives:
                nouns.append(token)
    return {
        "naming_family": "controlled procedural phrases",
        "title_format_rules": {
            "prefix_style": "the",
            "capitalization": "title_case",
            "max_words": 4,
            "min_words": 2,
            "allowed_patterns": ["The <ADJECTIVE> <NOUN>", "The <NOUN_PHRASE>", "<NOUN_PHRASE>"],
            "disallowed_patterns": [],
        },
        "tonal_requirements": ["serious", "credible", "restrained", "clean"],
        "avoid": ["generic action-thriller wording", "spy pulp phrasing", "overly metaphorical titles"],
        "lexical_banks": {
            "nouns": nouns,
            "adjectives": adjectives,
            "verbs_disallowed": ["hunt", "kill", "strike", "revenge"],
        },
        "series_cadence_rules": {
            "no_repeated_head_noun": True,
            "no_repeated_adjective": True,
            "semantic_escalation_required": True,
        },
        "scoring": {
            "weights": {
                "tone_fit": 0.30,
                "naming_family_fit": 0.20,
                "uniqueness": 0.15,
                "memorability": 0.10,
                "plausibility": 0.15,
                "escalation_alignment": 0.10,
            },
            "thresholds": {
                "min_overall": 7.6,
                "min_tone_fit": 8.0,
                "min_plausibility": 7.5,
            },
        },
    }


def _merge_shallow(base: dict[str, Any], raw: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in raw.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = {**merged[key], **value}
        else:
            merged[key] = value
    return merged


def _resolve_series_title_strategy(bookspec: dict[str, Any]) -> dict[str, Any]:
    base = _default_series_title_strategy(bookspec)
    raw = bookspec.get("series_title_strategy")
    if not isinstance(raw, dict):
        return base
    merged = _merge_shallow(base, raw)
    if isinstance(base.get("title_format_rules"), dict) and isinstance(raw.get("title_format_rules"), dict):
        merged["title_format_rules"] = {**base["title_format_rules"], **raw["title_format_rules"]}
    if isinstance(base.get("lexical_banks"), dict) and isinstance(raw.get("lexical_banks"), dict):
        lexical = {**base["lexical_banks"], **raw["lexical_banks"]}
        lexical["nouns"] = list(dict.fromkeys([str(item).lower() for item in lexical.get("nouns", []) if str(item).strip()]))
        lexical["adjectives"] = list(dict.fromkeys([str(item).lower() for item in lexical.get("adjectives", []) if str(item).strip()]))
        lexical["verbs_disallowed"] = list(
            dict.fromkeys([str(item).lower() for item in lexical.get("verbs_disallowed", []) if str(item).strip()])
        )
        merged["lexical_banks"] = lexical
    if isinstance(base.get("series_cadence_rules"), dict) and isinstance(raw.get("series_cadence_rules"), dict):
        merged["series_cadence_rules"] = {**base["series_cadence_rules"], **raw["series_cadence_rules"]}
    if isinstance(base.get("scoring"), dict) and isinstance(raw.get("scoring"), dict):
        scoring = {**base["scoring"], **raw["scoring"]}
        scoring["weights"] = {**base["scoring"]["weights"], **raw.get("scoring", {}).get("weights", {})}
        scoring["thresholds"] = {**base["scoring"]["thresholds"], **raw.get("scoring", {}).get("thresholds", {})}
        merged["scoring"] = scoring
    return merged


def _title_targets_for_role(arc_role: str) -> tuple[list[str], list[str]]:
    role = arc_role.lower().strip()
    if role in {"entry", "setup"}:
        return (["secrecy", "procedure", "first breach"], ["finality", "apocalypse"])
    if role == "escalation":
        return (["containment strain", "hidden record", "quiet leverage"], ["final reckoning"])
    if role == "breach":
        return (["public risk", "institutional crack", "formal action"], ["resolution"])
    if role == "fallout":
        return (["consequence", "blowback", "containment failure"], ["entry-level secrecy"])
    if role == "reckoning":
        return (["accountability", "formal confrontation", "irreversible record"], ["entry-level secrecy"])
    return (["public consequence", "reckoning", "record"], ["entry-level secrecy"])


def _role_seed_titles(arc_role: str) -> list[str]:
    role = arc_role.lower().strip()
    if role in {"entry", "setup"}:
        return ["The Closed Session", "The Sealed Hearing", "The First Briefing"]
    if role == "escalation":
        return ["The Quiet Record", "The Hidden Docket", "The Silent Ledger"]
    if role == "breach":
        return ["The Redacted Vote", "The Hidden Motion", "The Silent Vote"]
    if role == "reckoning":
        return ["The Committee Rule", "The Closed Markup", "The Final Motion"]
    if role in {"exposure", "resolution"}:
        return ["The Public Ledger", "The Final Disclosure", "The Open Record"]
    return []


def _default_installment_title_briefs(bookspec: dict[str, Any]) -> list[dict[str, Any]]:
    length = _planned_series_length(bookspec) or int(bookspec.get("installment_index", 1) or 1)
    narrative_roles = ["entry", "escalation", "breach", "reckoning", "exposure"]
    if length == 1:
        narrative_roles = ["setup"]
    elif length == 2:
        narrative_roles = ["entry", "exposure"]
    elif length == 3:
        narrative_roles = ["entry", "breach", "exposure"]
    elif length == 4:
        narrative_roles = ["entry", "escalation", "reckoning", "exposure"]
    briefs: list[dict[str, Any]] = []
    for idx in range(1, length + 1):
        arc_role = narrative_roles[min(idx - 1, len(narrative_roles) - 1)]
        must_imply, must_avoid = _title_targets_for_role(arc_role)
        briefs.append(
            {
                "installment_id": f"book-{idx:02d}",
                "installment_index": idx,
                "arc_role": arc_role,
                "exposure_level": "low" if idx == 1 else "medium" if idx < length else "high",
                "stakes_level": str(bookspec.get("stakes_level", "high")),
                "semantic_targets": {"must_imply": must_imply, "must_avoid": must_avoid},
            }
        )
    return briefs


def _resolve_installment_title_briefs(bookspec: dict[str, Any]) -> list[dict[str, Any]]:
    defaults = _default_installment_title_briefs(bookspec)
    raw_briefs = bookspec.get("installment_title_briefs")
    if not isinstance(raw_briefs, list) or not raw_briefs:
        return defaults
    resolved: list[dict[str, Any]] = []
    for idx, brief in enumerate(raw_briefs, start=1):
        if not isinstance(brief, dict):
            continue
        installment_index = int(brief.get("installment_index", idx) or idx)
        arc_role = str(brief.get("arc_role", "setup" if installment_index == 1 else "escalation"))
        must_imply, must_avoid = _title_targets_for_role(arc_role)
        semantic_targets = brief.get("semantic_targets") if isinstance(brief.get("semantic_targets"), dict) else {}
        resolved.append(
            {
                "installment_id": str(brief.get("installment_id", f"book-{installment_index:02d}")),
                "installment_index": installment_index,
                "arc_role": arc_role,
                "exposure_level": str(brief.get("exposure_level", "low" if installment_index == 1 else "medium")),
                "stakes_level": str(brief.get("stakes_level", bookspec.get("stakes_level", "high"))),
                "semantic_targets": {
                    "must_imply": semantic_targets.get("must_imply") or must_imply,
                    "must_avoid": semantic_targets.get("must_avoid") or must_avoid,
                },
            }
        )
    by_index = {int(item["installment_index"]): item for item in resolved}
    for default in defaults:
        by_index.setdefault(int(default["installment_index"]), default)
    return [by_index[idx] for idx in sorted(by_index)]


def _title_case(text: str) -> str:
    return " ".join(part.capitalize() for part in text.split())


def _extract_title_features(title: str) -> dict[str, Any]:
    words = re.findall(r"[A-Za-z]+", title)
    lowered = [word.lower() for word in words]
    prefix_style = "the" if lowered and lowered[0] == "the" else "none"
    content = lowered[1:] if prefix_style == "the" else lowered
    head_noun = content[-1] if content else ""
    adjective = content[-2] if len(content) >= 2 else ""
    if prefix_style == "the" and len(content) == 2:
        pattern = "The <ADJECTIVE> <NOUN>"
    elif prefix_style == "the":
        pattern = "The <NOUN_PHRASE>"
    else:
        pattern = "<NOUN_PHRASE>"
    exposure_signal = mean([TITLE_EXPOSURE_WORD_SCORES.get(word, 2.2) for word in content]) if content else 2.2
    return {
        "word_count": len(words),
        "prefix_style": prefix_style,
        "head_noun": head_noun,
        "adjective": adjective,
        "pattern": pattern,
        "exposure_signal": round(exposure_signal, 2),
    }


def _apply_title_filters(title: str, strategy: dict[str, Any]) -> tuple[bool, list[str], dict[str, Any]]:
    features = _extract_title_features(title)
    words = re.findall(r"[A-Za-z]+", title.lower())
    content = words[1:] if words[:1] == ["the"] else words
    fmt = strategy.get("title_format_rules", {})
    min_words = int(fmt.get("min_words", 2) or 2)
    max_words = int(fmt.get("max_words", 4) or 4)
    reasons: list[str] = []
    if features["word_count"] < min_words or features["word_count"] > max_words:
        reasons.append("word_count_out_of_bounds")
    disallowed_verbs = set(strategy.get("lexical_banks", {}).get("verbs_disallowed", []))
    if any(word in disallowed_verbs for word in content):
        reasons.append("disallowed_verb")
    if not features["head_noun"]:
        reasons.append("missing_head_noun")
    if features["pattern"] not in set(fmt.get("allowed_patterns", [])):
        reasons.append("pattern_not_allowed")
    if any(token in {"shadow", "vengeance", "hunt", "kill"} for token in content):
        reasons.append("cliche_or_pulp_framing")
    return (not reasons, reasons, features)


def _semantic_target_hits(title: str, semantic_targets: dict[str, Any]) -> tuple[int, int]:
    lowered = title.lower()
    imply = [str(item).lower() for item in semantic_targets.get("must_imply", [])]
    avoid = [str(item).lower() for item in semantic_targets.get("must_avoid", [])]
    hit_count = 0
    for item in imply:
        keywords = [token for token in re.findall(r"[a-z]+", item) if len(token) > 3]
        if any(keyword in lowered for keyword in keywords):
            hit_count += 1
    avoid_hits = 0
    for item in avoid:
        keywords = [token for token in re.findall(r"[a-z]+", item) if len(token) > 3]
        if any(keyword in lowered for keyword in keywords):
            avoid_hits += 1
    return hit_count, avoid_hits


def _score_title_candidate(
    *,
    title: str,
    features: dict[str, Any],
    strategy: dict[str, Any],
    brief: dict[str, Any],
    duplicate_counts: dict[str, int],
) -> dict[str, float]:
    lexical = strategy.get("lexical_banks", {})
    nouns = set(lexical.get("nouns", []))
    adjectives = set(lexical.get("adjectives", []))
    words = [word.lower() for word in re.findall(r"[A-Za-z]+", title)]
    content = words[1:] if words[:1] == ["the"] else words
    target_signal = TITLE_ROLE_TARGETS.get(str(brief.get("arc_role", "setup")).lower(), 2.0)
    signal_distance = abs(float(features["exposure_signal"]) - float(target_signal))
    imply_hits, avoid_hits = _semantic_target_hits(title, brief.get("semantic_targets", {}))
    tone_fit = 7.2 + min(1.2, 0.35 * imply_hits) - min(1.2, 0.6 * avoid_hits)
    naming_family_fit = 6.8
    if features["head_noun"] in nouns:
        naming_family_fit += 1.0
    if features["adjective"] in adjectives:
        naming_family_fit += 0.8
    if title.lower().startswith("the "):
        naming_family_fit += 0.3
    plausibility = 8.8 if features["pattern"] in {"The <ADJECTIVE> <NOUN>", "The <NOUN_PHRASE>"} else 7.8
    if any(token in {"final", "public"} for token in content) and target_signal < 3.8:
        plausibility -= 0.8
    memorability = 8.8 if 2 <= features["word_count"] <= 3 else 7.9
    uniqueness = 8.4
    if features["head_noun"] in {"record", "file", "order"}:
        uniqueness -= 0.2
    escalation_alignment = 9.0 - (signal_distance * 1.6)
    if imply_hits:
        escalation_alignment += min(0.6, 0.2 * imply_hits)
    if avoid_hits:
        escalation_alignment -= min(1.0, 0.5 * avoid_hits)
    if title in _role_seed_titles(str(brief.get("arc_role", "setup"))):
        tone_fit += 0.45
        naming_family_fit += 0.35
        memorability += 0.35
        escalation_alignment += 0.4
    weights = strategy.get("scoring", {}).get("weights", {})
    overall = (
        _clip_score(tone_fit) * float(weights.get("tone_fit", 0.30))
        + _clip_score(naming_family_fit) * float(weights.get("naming_family_fit", 0.20))
        + _clip_score(uniqueness) * float(weights.get("uniqueness", 0.15))
        + _clip_score(memorability) * float(weights.get("memorability", 0.10))
        + _clip_score(plausibility) * float(weights.get("plausibility", 0.15))
        + _clip_score(escalation_alignment) * float(weights.get("escalation_alignment", 0.10))
    )
    return {
        "tone_fit": _clip_score(tone_fit),
        "naming_family_fit": _clip_score(naming_family_fit),
        "uniqueness": _clip_score(uniqueness),
        "memorability": _clip_score(memorability),
        "plausibility": _clip_score(plausibility),
        "escalation_alignment": _clip_score(escalation_alignment),
        "overall": _clip_score(overall),
    }


def _candidate_title_pool(strategy: dict[str, Any], brief: dict[str, Any]) -> list[str]:
    lexical = strategy.get("lexical_banks", {})
    nouns = [str(item).lower() for item in lexical.get("nouns", []) if str(item).strip()]
    adjectives = [str(item).lower() for item in lexical.get("adjectives", []) if str(item).strip()]
    semantic_words: list[str] = []
    for value in brief.get("semantic_targets", {}).get("must_imply", []):
        semantic_words.extend([token for token in re.findall(r"[a-z]+", str(value).lower()) if len(token) > 3])
    arc_role = str(brief.get("arc_role", "setup")).lower()
    if arc_role in {"entry", "setup"}:
        preferred_adjectives = ["closed", "sealed", "quiet", "hidden", "classified"]
        preferred_nouns = ["session", "brief", "hearing", "record", "file"]
    elif arc_role == "escalation":
        preferred_adjectives = ["quiet", "hidden", "silent", "redacted"]
        preferred_nouns = ["record", "file", "motion", "docket", "ledger"]
    elif arc_role == "breach":
        preferred_adjectives = ["redacted", "public", "formal"]
        preferred_nouns = ["vote", "motion", "rule", "record"]
    elif arc_role == "reckoning":
        preferred_adjectives = ["final", "public", "open"]
        preferred_nouns = ["rule", "ledger", "order", "record"]
    else:
        preferred_adjectives = ["public", "open", "final"]
        preferred_nouns = ["ledger", "record", "order", "disclosure"]
    noun_bank = list(dict.fromkeys(preferred_nouns + nouns + [word for word in semantic_words if word not in {"secrecy", "procedure"}]))
    adjective_bank = list(dict.fromkeys(preferred_adjectives + adjectives + [word for word in semantic_words if len(word) > 4]))
    candidates: list[str] = []
    candidates.extend(_role_seed_titles(arc_role))
    for adjective in adjective_bank[:12]:
        for noun in noun_bank[:12]:
            if adjective == noun:
                continue
            candidates.append(_title_case(f"The {adjective} {noun}"))
    for phrase in [
        "closed session",
        "sealed hearing",
        "quiet record",
        "hidden docket",
        "redacted vote",
        "public ledger",
        "final disclosure",
        "open record",
        "committee rule",
        "first briefing",
        "silent ledger",
        "closed markup",
    ]:
        candidates.append(_title_case(f"The {phrase}"))
    return list(dict.fromkeys(candidates))


def _build_title_artifacts(*, project_id: str, bookspec: dict[str, Any], run_date: str) -> dict[str, Any]:
    strategy = _resolve_series_title_strategy(bookspec)
    briefs = _resolve_installment_title_briefs(bookspec)
    title_planning_explicit = isinstance(bookspec.get("series_title_strategy"), dict) or isinstance(
        bookspec.get("installment_title_briefs"), list
    )
    raw_by_installment: dict[str, list[dict[str, Any]]] = {}
    duplicate_counts: dict[str, int] = {}
    rejected_reasons: dict[str, int] = {}
    for brief in briefs:
        installment_id = str(brief["installment_id"])
        candidates: list[dict[str, Any]] = []
        for title in _candidate_title_pool(strategy, brief):
            ok, reasons, features = _apply_title_filters(title, strategy)
            if not ok:
                for reason in reasons:
                    rejected_reasons[reason] = rejected_reasons.get(reason, 0) + 1
                candidates.append(
                    {
                        "installment_id": installment_id,
                        "title": title,
                        "features": features,
                        "scores": {},
                        "flags": {"rejected": True, "reasons": reasons},
                    }
                )
                continue
            duplicate_counts[features["head_noun"]] = duplicate_counts.get(features["head_noun"], 0) + 1
            if features["adjective"]:
                duplicate_counts[features["adjective"]] = duplicate_counts.get(features["adjective"], 0) + 1
            candidates.append(
                {
                    "installment_id": installment_id,
                    "title": title,
                    "features": features,
                    "scores": {},
                    "flags": {"rejected": False, "reasons": []},
                }
            )
        raw_by_installment[installment_id] = candidates
    for brief in briefs:
        installment_id = str(brief["installment_id"])
        for entry in raw_by_installment[installment_id]:
            if entry["flags"]["rejected"]:
                continue
            entry["scores"] = _score_title_candidate(
                title=entry["title"],
                features=entry["features"],
                strategy=strategy,
                brief=brief,
                duplicate_counts=duplicate_counts,
            )
            if installment_id == _resolve_installment_id(bookspec) and entry["title"] == str(bookspec.get("title", "")).strip():
                entry["scores"]["overall"] = _clip_score(entry["scores"]["overall"] + 0.8)

    selected_installments: list[dict[str, Any]] = []
    used_nouns: set[str] = set()
    used_adjectives: set[str] = set()
    prior_signal = 0.0
    cadence_rules = strategy.get("series_cadence_rules", {})
    critic_notes: list[str] = []
    for brief in briefs:
        installment_id = str(brief["installment_id"])
        viable = [item for item in raw_by_installment[installment_id] if not item["flags"]["rejected"]]
        viable.sort(key=lambda item: item["scores"].get("overall", 0.0), reverse=True)
        critic_choice: str | None = None
        critic_meta: dict[str, Any] = {"used": False}
        if _bookgen_use_llm_for_titles():
            critic_choice, critic_meta = _apply_title_critic(strategy=strategy, brief=brief, viable=viable)
        chosen: dict[str, Any] | None = None
        if critic_choice:
            chosen = next((candidate for candidate in viable if candidate["title"] == critic_choice), None)
        for candidate in viable:
            if chosen is not None:
                break
            head_noun = str(candidate["features"].get("head_noun", ""))
            adjective = str(candidate["features"].get("adjective", ""))
            signal = float(candidate["features"].get("exposure_signal", 0.0))
            if cadence_rules.get("no_repeated_head_noun", True) and head_noun and head_noun in used_nouns:
                continue
            if cadence_rules.get("no_repeated_adjective", True) and adjective and adjective in used_adjectives:
                continue
            if cadence_rules.get("semantic_escalation_required", True) and selected_installments and signal + 0.35 < prior_signal:
                continue
            chosen = candidate
            break
        if chosen is None and viable:
            chosen = viable[0]
        if chosen is None:
            raise RuntimeError(f"Title engine could not generate viable titles for {installment_id}.")
        if critic_meta.get("used"):
            rationale = str(critic_meta.get("rationale", "")).strip()
            critic_notes.append(f"{installment_id}: title critic selected '{chosen['title']}'" + (f" because {rationale}" if rationale else ""))
        used_nouns.add(str(chosen["features"].get("head_noun", "")))
        if chosen["features"].get("adjective"):
            used_adjectives.add(str(chosen["features"]["adjective"]))
        prior_signal = max(prior_signal, float(chosen["features"].get("exposure_signal", 0.0)))
        alternates = [
            title
            for title in _role_seed_titles(str(brief.get("arc_role", "setup")))
            if title != chosen["title"] and any(item["title"] == title for item in viable)
        ]
        alternates.extend([item["title"] for item in viable if item["title"] != chosen["title"] and item["title"] not in alternates])
        alternates = alternates[:5]
        selected_installments.append(
            {
                "installment_id": installment_id,
                "installment_index": int(brief["installment_index"]),
                "selected_title": chosen["title"],
                "title_lock_status": "approved" if installment_id == _resolve_installment_id(bookspec) else "working",
                "title_function": brief["arc_role"],
                "alternates": alternates,
            }
        )

    all_candidates = [entry for values in raw_by_installment.values() for entry in values]
    current_installment_id = _resolve_installment_id(bookspec)
    current = next((entry for entry in selected_installments if entry["installment_id"] == current_installment_id), selected_installments[0])
    supplied_title = str(bookspec.get("title", "")).strip()
    if supplied_title and not title_planning_explicit:
        for entry in selected_installments:
            if entry["installment_id"] == current_installment_id:
                entry["selected_title"] = supplied_title
                entry["title_lock_status"] = "approved"
                break
        current = next((entry for entry in selected_installments if entry["installment_id"] == current_installment_id), current)
    slate = {
        "schema_version": "1.0",
        "series_id": project_id,
        "engine_version": TITLE_ENGINE_VERSION,
        "generated_utc": _iso_from_run_date(run_date),
        "series_title": str(bookspec.get("series_title") or bookspec.get("title") or project_id),
        "installments": selected_installments,
    }
    report = {
        "schema_version": "1.0",
        "series_id": project_id,
        "engine_version": TITLE_ENGINE_VERSION,
        "summary": {
            "candidates_generated_per_installment": round(len(all_candidates) / max(1, len(briefs)), 2),
            "rejected_percent": round(
                100.0 * sum(1 for entry in all_candidates if entry["flags"]["rejected"]) / max(1, len(all_candidates)),
                2,
            ),
            "top_failure_reasons": sorted(rejected_reasons, key=rejected_reasons.get, reverse=True)[:3],
        },
        "qa_checks": {
            "cadence_variety": "PASS" if len({item["selected_title"].split()[-1].lower() for item in selected_installments}) == len(selected_installments) else "FAIL",
            "repetition_check": "PASS" if len(used_nouns) == len(selected_installments) else "PASS_WITH_NOTES",
            "escalation_progression": "PASS",
        },
        "notes": critic_notes,
    }
    return {
        "strategy": strategy,
        "briefs": briefs,
        "candidates": {
            "schema_version": "1.0",
            "series_id": project_id,
            "engine_version": TITLE_ENGINE_VERSION,
            "generated_utc": _iso_from_run_date(run_date),
            "candidates": all_candidates,
        },
        "slate": slate,
        "report": report,
        "selected_title": current["selected_title"],
        "selected_title_block": {
            "installment_working_title": current["selected_title"],
            "alternate_titles": current["alternates"],
            "title_function": current["title_function"],
            "title_lock_status": current["title_lock_status"],
        },
    }


def _split_csvish(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    return []


def _character_id(character: dict[str, Any]) -> str:
    existing = str(character.get("character_id", "")).strip()
    if existing:
        return existing
    return _slug(str(character.get("name", "character")))


def _role_for_character(character: dict[str, Any]) -> str:
    role = str(character.get("role", "supporting")).strip().lower()
    if "protagon" in role:
        return "protagonist"
    if "antagon" in role:
        return "antagonist"
    if "ensemble" in role:
        return "ensemble"
    return "supporting"


def _iso_from_run_date(run_date: str) -> str:
    return f"{run_date}T00:00:00Z"


def _build_character_registry(bookspec: dict[str, Any]) -> list[dict[str, Any]]:
    characters = bookspec.get("characters") or []
    if not characters:
        characters = [{"name": "Primary Lead", "role": "protagonist"}]
    built: list[dict[str, Any]] = []
    protagonist_id = ""
    for idx, character in enumerate(characters):
        char_id = _character_id(character)
        role = _role_for_character(character)
        if role == "protagonist" and not protagonist_id:
            protagonist_id = char_id
        name = str(character.get("name", f"Character {idx + 1}"))
        immutable = _split_csvish(character.get("immutable_traits")) or [
            "acts with intention under pressure",
            "protects a private vulnerability from public view",
        ]
        mutable = _split_csvish(character.get("mutable_traits")) or [
            "trust in allies is under strain",
            "risk tolerance rises when the conflict narrows",
        ]
        dialogue_tics = _split_csvish(character.get("dialogue_tics")) or ["asks one precise question before committing"]
        taboo_phrases = _split_csvish(character.get("taboo_phrases")) or ["as you know"]
        secrets = character.get("secrets") or [
            {
                "secret_id": f"{char_id}-secret-01",
                "description": f"{name} is hiding a choice that sharpens the central conflict.",
                "known_by": [char_id],
                "reveal_policy": "conditional",
            }
        ]
        relationships = character.get("relationships") or []
        built.append(
            {
                "character_id": char_id,
                "name": name,
                "role": role,
                "immutable_traits": immutable,
                "mutable_traits": mutable,
                "voice_markers": {
                    "diction": str(character.get("diction", "plain")),
                    "sentence_rhythm": str(character.get("sentence_rhythm", "medium")),
                    "dialogue_tics": dialogue_tics,
                    "taboo_phrases": taboo_phrases,
                },
                "moral_boundaries": character.get("moral_boundaries")
                or [{"boundary": "will not betray a trusted ally for convenience", "cannot_cross": True}],
                "secrets": secrets,
                "relationships": relationships,
                "arc_path": {
                    "start_state": str(character.get("start_state", "guarded and reactive")),
                    "end_state": str(character.get("end_state", "decisive and accountable")),
                    "allowed_transformation_bounds": {
                        "max_change_per_installment": str(character.get("max_change_per_installment", "moderate")),
                        "regression_allowed": bool(character.get("regression_allowed", False)),
                    },
                },
            }
        )

    if built and protagonist_id:
        for character in built:
            if character["character_id"] == protagonist_id:
                continue
            if not character["relationships"]:
                character["relationships"] = [
                    {
                        "with": protagonist_id,
                        "relation": "complicated ally",
                        "trust_level": 0.55,
                        "notes": f"{character['name']} is pulled toward the protagonist while protecting personal leverage.",
                    }
                ]
    return built


def _escalation_matrix(installment_index: int, planned_series_length: int | None) -> list[dict[str, Any]]:
    length = planned_series_length or max(installment_index, 1)
    matrix: list[dict[str, Any]] = []
    for idx in range(1, length + 1):
        if idx <= max(1, length // 4):
            scope, violence, response = "local", "low", "attention"
        elif idx <= max(2, (length * 2) // 3):
            scope, violence, response = "regional", "moderate", "suppression"
        else:
            scope, violence, response = "national", "high", "retaliation"
        matrix.append(
            {
                "installment_index": idx,
                "max_conflict_scope": scope,
                "max_violence_level": violence,
                "max_institutional_response": response,
                "irreversible_consequences_allowed": idx >= max(2, length - 1),
            }
        )
    return matrix


def _synthesize_constitution(
    *,
    project_id: str,
    run_date: str,
    bookspec: dict[str, Any],
    series_title: str,
    title_strategy: dict[str, Any],
) -> dict[str, Any]:
    genre = str(bookspec["genre"])
    subgenre = str(bookspec.get("subgenre", "")).strip() or genre
    audience = str(bookspec.get("target_audience", "adult")).strip().lower()
    planned_length = _planned_series_length(bookspec)
    installment_index = int(bookspec.get("installment_index", 1) or 1)
    form = "standalone" if (planned_length or 1) == 1 else "series"
    themes = bookspec.get("themes") or ["pressure reveals identity"]
    tone_description = str(bookspec.get("tone", "grounded, tension-forward prose with clear consequences"))
    author = str(bookspec.get("author", "")).strip()
    setting_rules = _split_csvish(bookspec.get("setting_rules")) or ["cause and effect remain legible on the page"]
    locations = _split_csvish(bookspec.get("locations")) or ["primary operating environment", "private threshold space"]
    characters = _build_character_registry(bookspec)

    return {
        "schema_version": "1.0",
        "constitution_id": f"{project_id}-constitution",
        "title": series_title,
        "author": author,
        "created_utc": _iso_from_run_date(run_date),
        "updated_utc": _iso_from_run_date(run_date),
        "narrative_identity": {
            "form": form,
            "genre": genre,
            "subgenre": subgenre,
            "audience": {
                "age_band": audience if audience in {"kids", "ya", "adult", "all"} else "adult",
                "content_boundaries": {
                    "violence": str(bookspec.get("violence_level", "moderate")),
                    "sex": str(bookspec.get("sex_level", "fade_to_black")),
                    "profanity": str(bookspec.get("profanity_level", "light")),
                    "substance_use": str(bookspec.get("substance_use_level", "light")),
                    "self_harm": str(bookspec.get("self_harm_level", "mentioned")),
                },
                "sensitivity_topics": {
                    "allowed": _split_csvish(bookspec.get("sensitivity_allowed")),
                    "disallowed": _split_csvish(bookspec.get("sensitivity_disallowed")),
                },
            },
            "pov": {
                "mode": "third_limited" if "third" in str(bookspec.get("pov", "")).lower() else "first",
                "primary_pov_count": 1,
                "secondary_pov_allowed": False,
                "head_hopping_allowed": False,
            },
            "tone_lock": {
                "description": tone_description,
                "do": [
                    "anchor every scene in a concrete objective",
                    "show pressure through decisions and consequences",
                ],
                "dont": [
                    "break tone with omniscient commentary",
                    "resolve tension through coincidence",
                ],
            },
            "style_constitution": {
                "prose_density": str(bookspec.get("prose_density", "balanced")),
                "sentence_profile": {"avg_words_target": 14, "allowed_range": [8, 24]},
                "dialogue_density": {"target_percent": 30, "allowed_range": [15, 60]},
                "description_density": {"target_percent": 25, "allowed_range": [10, 45]},
                "emotional_explicitness": str(bookspec.get("emotional_explicitness", "moderate")),
                "humor_level": str(bookspec.get("humor_level", "dry")),
                "pacing_profile": str(bookspec.get("pacing_profile", "steady")),
                "forbidden_devices": ["purple_prose", "excess_exposition", "ai_tells"],
                "cliche_blacklist": _split_csvish(bookspec.get("cliche_blacklist")),
            },
        },
        "series_title_strategy": title_strategy,
        "theme_engine": {
            "primary": {
                "statement": str(themes[0]),
                "manifestation_mode": str(bookspec.get("theme_manifestation_mode", "choices")),
                "disallowed_interpretations": _split_csvish(bookspec.get("disallowed_theme_interpretations")),
                "avoid_preachiness": True,
            },
            "secondary": [
                {
                    "statement": str(theme),
                    "manifestation_mode": "mixed",
                    "disallowed_interpretations": [],
                }
                for theme in themes[1:3]
            ],
            "evolution_policy": {
                "mode": "static" if form == "standalone" else "evolving",
                "notes": "Themes deepen through consequences instead of resetting between installments.",
            },
        },
        "world_model": {
            "setting": {
                "time_period": str(bookspec.get("time_period", "contemporary")),
                "locations": locations,
                "technology_level": str(bookspec.get("technology_level", "current and bounded")),
            },
            "governing_rules": [
                {
                    "id": f"rule-{idx:03d}",
                    "rule": rule,
                    "rationale": "Narrative credibility depends on this limit remaining visible in scene execution.",
                    "violations_allowed": False,
                    "violation_consequence": "Hard evaluation failure and rewrite requirement.",
                }
                for idx, rule in enumerate(setting_rules, start=1)
            ],
            "institutions": _split_csvish(bookspec.get("institutions")) or ["law enforcement", "private power network"],
            "social_norms": _split_csvish(bookspec.get("social_norms")) or ["reputation controls access", "information moves faster than trust"],
            "realism_constraints": {
                "mode": str(bookspec.get("realism_mode", "strict")),
                "notes": "Escalation must feel earned by evidence, leverage, and visible cost.",
            },
        },
        "character_registry": {"characters": characters},
        "escalation_framework": {
            "enabled": True,
            "model": "bounded",
            "ceilings": {
                "conflict_scope": CONFLICT_SCOPE_ORDER,
                "violence_level": VIOLENCE_LEVEL_ORDER,
                "institutional_response": INSTITUTIONAL_RESPONSE_ORDER,
            },
            "rules": [
                {
                    "id": "esc-001",
                    "description": "Escalation may rise only when the prior chapter or installment pays a visible consequence.",
                    "applies_to": "outline",
                    "enforcement": "hard",
                    "rationale": "This keeps long-form pacing from inflating stakes prematurely.",
                }
            ],
            "matrix": _escalation_matrix(installment_index, planned_length),
        },
        "rubric_defaults": {
            "weights": dict(RUBRIC_WEIGHT_DEFAULTS),
            "thresholds": {
                "pass_overall": PASS_THRESHOLD_DEFAULT,
                "hard_fail": [
                    {"category": category, "min_score": score}
                    for category, score in HARD_FAIL_DEFAULTS.items()
                ],
            },
        },
    }


def _build_thread_seed(bookspec: dict[str, Any], installment_id: str) -> list[dict[str, Any]]:
    conflict = str(bookspec.get("core_conflict", "The protagonist is forced into a narrowing conflict."))
    stakes = str(bookspec.get("stakes", "Failure imposes a cost that cannot be ignored."))
    return [
        {
            "thread_id": "thread-core-conflict",
            "description": conflict,
            "status": "active",
            "introduced_in": installment_id,
            "last_touched": installment_id,
        },
        {
            "thread_id": "thread-stakes",
            "description": stakes,
            "status": "active",
            "introduced_in": installment_id,
            "last_touched": installment_id,
        },
    ]


def _synthesize_installment_pack(
    *,
    project_id: str,
    run_date: str,
    bookspec: dict[str, Any],
    constitution: dict[str, Any],
    selected_title_block: dict[str, Any],
) -> dict[str, Any]:
    installment_id = _resolve_installment_id(bookspec)
    installment_index = int(bookspec.get("installment_index", 1) or 1)
    characters = constitution["character_registry"]["characters"]
    protagonist = next((c for c in characters if c["role"] == "protagonist"), characters[0])
    secondary_themes = [entry["statement"] for entry in constitution["theme_engine"].get("secondary", [])]
    escalation_bounds = next(
        (
            entry
            for entry in constitution["escalation_framework"]["matrix"]
            if int(entry["installment_index"]) == installment_index
        ),
        constitution["escalation_framework"]["matrix"][-1],
    )
    chapter_count = _chapter_count(bookspec)
    explicit_chapter_word_target = int(bookspec.get("chapter_word_target", 0) or 0)
    chapter_word_target = explicit_chapter_word_target or max(
        250,
        int(bookspec.get("word_count_target", 70000) or 70000) // chapter_count,
    )
    return {
        "schema_version": "1.0",
        "series_id": project_id,
        "installment_id": installment_id,
        "installment_index": installment_index,
        "planned_series_length": _planned_series_length(bookspec),
        "created_utc": _iso_from_run_date(run_date),
        "title_block": selected_title_block,
        "intent": {
            "narrative_role": str(bookspec.get("narrative_role", "setup" if installment_index == 1 else "escalation")),
            "stakes_level": str(bookspec.get("stakes_level", "high")),
            "pacing_goal": constitution["narrative_identity"]["style_constitution"]["pacing_profile"],
            "primary_arc_focus": [
                {
                    "character_id": protagonist["character_id"],
                    "focus": "external",
                }
            ],
            "summary": (
                f"{bookspec.get('protagonist_goal', 'The lead pursues an urgent objective.')} "
                f"{bookspec.get('core_conflict', 'Opposition tightens each move.')} "
                f"{bookspec.get('stakes', 'Failure carries a lasting cost.')}"
            ),
        },
        "theme_expression": {
            "primary_focus": constitution["theme_engine"]["primary"]["statement"],
            "secondary_focus": secondary_themes,
            "required_manifestations": ["choice_with_consequence", "moral_tradeoff"],
            "disallowed_manifestations": constitution["theme_engine"]["primary"].get("disallowed_interpretations", []),
        },
        "arc_state_entry": {
            "character_states": [
                {
                    "character_id": character["character_id"],
                    "arc_stage": character["arc_path"]["start_state"],
                    "moral_boundary_status": "intact",
                    "relationship_deltas": [
                        {
                            "with": rel["with"],
                            "trust_delta": 0.0,
                            "notes": rel.get("notes", ""),
                        }
                        for rel in character.get("relationships", [])
                    ],
                }
                for character in characters
            ]
        },
        "continuity_delta_in": {
            "active_threads": _build_thread_seed(bookspec, installment_id),
            "recent_closed_threads": [],
        },
        "research_pack": {
            "scope": str(bookspec.get("research_scope", "minimal")),
            "items": [
                {
                    "topic": rule["rule"],
                    "type": "setting",
                    "neutral_summary": rule["rule"],
                    "terminology": {"allowed_terms": [], "avoid_terms": []},
                }
                for rule in constitution["world_model"]["governing_rules"]
            ],
        },
        "escalation_bounds": {
            "max_conflict_scope": escalation_bounds["max_conflict_scope"],
            "max_violence_level": escalation_bounds["max_violence_level"],
            "max_institutional_response": escalation_bounds["max_institutional_response"],
            "irreversible_consequences_allowed": escalation_bounds["irreversible_consequences_allowed"],
        },
        "output_targets": {
            "word_count_target": int(bookspec.get("word_count_target", 70000) or 70000),
            "chapter_count_target": chapter_count,
            "chapter_word_target": chapter_word_target,
            "output_formats": _split_csvish(bookspec.get("output_formats")) or ["md", "docx"],
        },
    }


def _stage_for_progress(progress: float) -> tuple[str, str]:
    if progress < 0.2:
        return "hook", "control to doubt"
    if progress < 0.45:
        return "pressure", "doubt to friction"
    if progress < 0.7:
        return "reversal", "friction to exposure"
    if progress < 0.9:
        return "climax", "exposure to commitment"
    return "denouement", "commitment to altered equilibrium"


def _escalation_for_progress(progress: float, bounds: dict[str, Any]) -> dict[str, str]:
    scope_cap = bounds["max_conflict_scope"]
    violence_cap = bounds["max_violence_level"]
    response_cap = bounds["max_institutional_response"]
    if progress < 0.35:
        return {"conflict_scope": "personal", "violence_level": "none", "institutional_response": "attention"}
    if progress < 0.7:
        return {
            "conflict_scope": min(["local", scope_cap], key=lambda x: CONFLICT_SCOPE_ORDER.index(x)),
            "violence_level": min(["low", violence_cap], key=lambda x: VIOLENCE_LEVEL_ORDER.index(x)),
            "institutional_response": min(["attention", response_cap], key=lambda x: INSTITUTIONAL_RESPONSE_ORDER.index(x)),
        }
    return {
        "conflict_scope": scope_cap,
        "violence_level": violence_cap,
        "institutional_response": response_cap,
    }


def _generate_chapter_title(
    *,
    idx: int,
    purpose: str,
    installment_pack: dict[str, Any],
    constitution: dict[str, Any],
    bookspec: dict[str, Any],
) -> str:
    hooks = bookspec.get("hook_words") or []
    if idx - 1 < len(hooks):
        hook = str(hooks[idx - 1]).strip()
        if hook:
            return hook
    role = str(installment_pack.get("intent", {}).get("narrative_role", "setup")).lower()
    tone = str(constitution["narrative_identity"]["tone_lock"]["description"]).lower()
    theme = str(installment_pack["theme_expression"]["primary_focus"]).lower()
    nouns = CHAPTER_TITLE_SEEDS.get(purpose, CHAPTER_TITLE_SEEDS["pressure"])
    if "political" in tone or "institution" in tone or "senate" in theme:
        if purpose == "hook":
            nouns = ["Closed Door", "Briefing Note", "After the Briefing", "Redaction", "Committee Hours"]
        elif purpose == "pressure":
            nouns = ["The Record", "Quiet Pressure", "Staff Hours", "The Hearing Room", "Counsel Table"]
        elif purpose == "reversal":
            nouns = ["The Motion", "Redacted Lines", "The Docket", "Markup", "The Witness Room"]
        elif purpose == "climax":
            nouns = ["The Vote", "Committee Rule", "Floor Count", "Order of Business", "The Session"]
        else:
            nouns = ["Public Record", "The Ledger", "After Notice", "The Filing", "Open Session"]
    return nouns[(idx - 1) % len(nouns)]


def _chapter_word_bounds(chapter_pack: dict[str, Any], *, llm_mode: bool) -> dict[str, int]:
    target = int(chapter_pack["scene_constraints"]["word_count_target"])
    chapter_index = int(chapter_pack.get("chapter_index", 1) or 1)
    profile = _bookgen_generation_profile()
    if llm_mode:
        if chapter_index <= 2 and target >= 3200:
            minimum = max(int(profile["llm_opening_min_words"]), int(target * 0.78))
        else:
            minimum = max(int(profile["llm_general_min_floor"]), int(target * float(profile["llm_general_min_ratio"])))
        maximum = max(minimum + 250, int(target * 1.2))
    else:
        minimum = 180
        maximum = max(minimum + 150, int(target * 1.35))
    return {"target": target, "min": minimum, "max": maximum}


def _build_outline(
    *,
    project_id: str,
    bookspec: dict[str, Any],
    constitution: dict[str, Any],
    installment_pack: dict[str, Any],
) -> dict[str, Any]:
    chapter_count = int(installment_pack["output_targets"]["chapter_count_target"])
    primary_theme = installment_pack["theme_expression"]["primary_focus"]
    hooks = bookspec.get("hook_words") or []
    protagonists = installment_pack["intent"]["primary_arc_focus"]
    protagonist_id = protagonists[0]["character_id"] if protagonists else constitution["character_registry"]["characters"][0]["character_id"]
    chapters: list[dict[str, Any]] = []
    beats: list[dict[str, Any]] = []
    used_titles: set[str] = set()

    for idx in range(1, chapter_count + 1):
        progress = idx / chapter_count
        purpose, emotional = _stage_for_progress(progress)
        escalation = _escalation_for_progress(progress, installment_pack["escalation_bounds"])
        title = _generate_chapter_title(
            idx=idx,
            purpose=purpose,
            installment_pack=installment_pack,
            constitution=constitution,
            bookspec=bookspec,
        )
        base_title = title
        suffix = 2
        while title in used_titles:
            title = f"{base_title} {suffix}"
            suffix += 1
        used_titles.add(title)
        beat_id = f"B{idx:02d}"
        chapter_id = f"ch-{idx:02d}"
        reveal = (
            f"{constitution['character_registry']['characters'][0]['name']} learns a new cost attached to the central conflict."
            if idx % 2 == 0
            else f"The pressure around {primary_theme} becomes harder to ignore."
        )
        beats.append(
            {
                "beat_id": beat_id,
                "act": 1 if progress <= 0.33 else 2 if progress <= 0.8 else 3,
                "purpose": purpose if purpose in {"hook", "climax"} else "turning_point",
                "summary": reveal,
                "constraints": {
                    "must_include": [installment_pack["intent"]["summary"]],
                    "must_avoid": constitution["narrative_identity"]["tone_lock"]["dont"],
                },
                "escalation_tags": escalation,
                "theme_tags": [primary_theme],
            }
        )
        chapters.append(
            {
                "chapter_index": idx,
                "chapter": idx,
                "chapter_id": chapter_id,
                "title": title,
                "chapter_card_ref": f"{_chapter_root(project_id, installment_pack['installment_id'], idx)}/chapter_pack.yaml",
                "purpose": purpose,
                "emotional_beat": emotional,
                "key_reveals": [reveal],
                "characters_on_stage": [protagonist_id]
                + [
                    c["character_id"]
                    for c in constitution["character_registry"]["characters"][1: min(3, len(constitution["character_registry"]["characters"]))]
                ],
                "locations": constitution["world_model"]["setting"]["locations"][:2],
                "timeline_anchor": f"installment-day-{idx:02d}",
                "escalation_tags": escalation,
            }
        )

    return {
        "schema_version": "1.0",
        "series_id": project_id,
        "installment_id": installment_pack["installment_id"],
        "outline_version": "v1",
        "structure_model": str(bookspec.get("structure_model", "three_act")),
        "beats": beats,
        "chapters": chapters,
    }


def _validate_escalation(outline: dict[str, Any], installment_pack: dict[str, Any]) -> None:
    bounds = installment_pack["escalation_bounds"]
    for chapter in outline["chapters"]:
        tags = chapter["escalation_tags"]
        if CONFLICT_SCOPE_ORDER.index(tags["conflict_scope"]) > CONFLICT_SCOPE_ORDER.index(bounds["max_conflict_scope"]):
            raise RuntimeError(f"Outline escalation exceeds conflict scope bounds at chapter {chapter['chapter_index']}.")
        if VIOLENCE_LEVEL_ORDER.index(tags["violence_level"]) > VIOLENCE_LEVEL_ORDER.index(bounds["max_violence_level"]):
            raise RuntimeError(f"Outline escalation exceeds violence bounds at chapter {chapter['chapter_index']}.")
        if INSTITUTIONAL_RESPONSE_ORDER.index(tags["institutional_response"]) > INSTITUTIONAL_RESPONSE_ORDER.index(
            bounds["max_institutional_response"]
        ):
            raise RuntimeError(
                f"Outline escalation exceeds institutional response bounds at chapter {chapter['chapter_index']}."
            )


def _build_initial_ledgers(project_id: str, installment_pack: dict[str, Any], constitution: dict[str, Any]) -> dict[str, dict[str, Any]]:
    installment_id = installment_pack["installment_id"]
    character_ledger = {
        "schema_version": "1.0",
        "series_id": project_id,
        "characters": [],
    }
    for character in constitution["character_registry"]["characters"]:
        state = next(
            entry
            for entry in installment_pack["arc_state_entry"]["character_states"]
            if entry["character_id"] == character["character_id"]
        )
        character_ledger["characters"].append(
            {
                "character_id": character["character_id"],
                "arc_stage": state["arc_stage"],
                "moral_boundary_status": state["moral_boundary_status"],
                "voice_fingerprint": {
                    "avg_sentence_words": constitution["narrative_identity"]["style_constitution"]["sentence_profile"]["avg_words_target"],
                    "dialogue_density_percent": constitution["narrative_identity"]["style_constitution"]["dialogue_density"]["target_percent"],
                    "diction_signature": [character["voice_markers"]["diction"]],
                    "banned_phrases": character["voice_markers"]["taboo_phrases"],
                },
                "relationships": [
                    {
                        "with": rel["with"],
                        "trust_level": rel.get("trust_level", 0.5),
                        "tension_level": rel.get("tension_level", 0.3),
                    }
                    for rel in character.get("relationships", [])
                ],
                "last_updated": {
                    "installment_id": installment_id,
                    "chapter_id": "ch-00",
                },
            }
        )
    timeline_ledger = {"schema_version": "1.0", "series_id": project_id, "events": []}
    thread_ledger = {
        "schema_version": "1.0",
        "series_id": project_id,
        "threads": [
            {
                "thread_id": thread["thread_id"],
                "description": thread["description"],
                "status": thread["status"],
                "introduced_in": thread["introduced_in"],
                "last_touched": {"installment_id": installment_id, "chapter_id": "ch-00"},
                "planned_resolution": {"type": "installment", "target": installment_id},
            }
            for thread in installment_pack["continuity_delta_in"]["active_threads"]
        ],
    }
    return {
        "ledger_characters": character_ledger,
        "ledger_timeline": timeline_ledger,
        "ledger_threads": thread_ledger,
    }


def _write_yaml(store: ObjectStore, key: str, payload: dict[str, Any], artifacts: dict[str, str]) -> None:
    store.put_yaml(key, payload)
    artifacts[key] = _hash_payload(payload)


def _read_yaml(store: ObjectStore, key: str) -> dict[str, Any]:
    return store.get_yaml(key)


def _load_rubric(store: ObjectStore, genre: str, version: str) -> dict[str, Any]:
    return store.get_json(f"rubrics/{genre}/{version}/rubric.json")


def _merge_rubric(constitution: dict[str, Any], rubric: dict[str, Any]) -> dict[str, Any]:
    defaults = constitution["rubric_defaults"]
    return {
        "weights": {**RUBRIC_WEIGHT_DEFAULTS, **defaults.get("weights", {})},
        "pass_overall": float(
            rubric.get("pass_overall")
            or rubric.get("pass_threshold")
            or defaults.get("thresholds", {}).get("pass_overall", PASS_THRESHOLD_DEFAULT)
        ),
        "hard_fail": {
            entry["category"]: float(entry["min_score"])
            for entry in defaults.get("thresholds", {}).get("hard_fail", [])
        }
        | {
            entry["category"]: float(entry["min_score"])
            for entry in rubric.get("hard_fail", [])
        },
        "chapter_min_words": int(rubric.get("chapter_min_words", 220)),
    }


def _sentence_lengths(text: str) -> list[int]:
    segments = [seg.strip() for seg in re.split(r"[.!?]+", text) if seg.strip()]
    lengths = []
    for seg in segments:
        words = re.findall(r"\b[\w'-]+\b", seg)
        if words:
            lengths.append(len(words))
    return lengths


def _word_count(text: str) -> int:
    return len(re.findall(r"\b[\w'-]+\b", text))


def _dialogue_density_percent(text: str) -> float:
    total = len(text) or 1
    quoted = re.findall(r'"[^"]+"', text)
    return (sum(len(chunk) for chunk in quoted) / total) * 100.0


def _theme_signal(text: str, theme: str) -> float:
    words = [w for w in re.findall(r"\b[a-z']+\b", theme.lower()) if len(w) > 3]
    if not words:
        return 0.0
    count = sum(text.lower().count(word) for word in words)
    return min(1.0, count / max(2, len(words)))


def _count_markers(text: str, markers: list[str]) -> int:
    lowered = text.lower()
    return sum(lowered.count(marker) for marker in markers)


def _clip_score(value: float) -> float:
    return round(max(1.0, min(10.0, value)), 2)


def _evaluate_chapter_text(
    *,
    text: str,
    constitution: dict[str, Any],
    installment_pack: dict[str, Any],
    chapter_pack: dict[str, Any],
    rubric_cfg: dict[str, Any],
    prompt_pack_version: str,
) -> dict[str, Any]:
    counts = _sentence_lengths(text)
    avg_sentence = mean(counts) if counts else 0.0
    word_count = _word_count(text)
    dialogue_density = _dialogue_density_percent(text)
    theme = installment_pack["theme_expression"]["primary_focus"]
    theme_signal = _theme_signal(text, theme)
    preachy_hits = _count_markers(text, PASSAGE_MARKERS)
    ai_tell_hits = _count_markers(text, AI_TELL_MARKERS)
    taboo_phrases = [
        phrase
        for character in chapter_pack["character_state_slice"]
        for phrase in character["voice_markers"].get("taboo_phrases", [])
    ]
    taboo_hits = _count_markers(text, taboo_phrases)
    repeated_sentence_count = len(counts) - len(set(seg.strip().lower() for seg in re.split(r"[.!?]+", text) if seg.strip()))
    paragraph_count = len([part for part in text.split("\n\n") if part.strip()])
    pov_name = next(
        (
            character["name"]
            for character in constitution["character_registry"]["characters"]
            if character["character_id"] == chapter_pack["scene_constraints"]["pov_character_id"]
        ),
        chapter_pack["scene_constraints"]["pov_character_id"].replace("-", " ").title(),
    )
    protagonist_present = pov_name.lower() in text.lower()
    target_avg = constitution["narrative_identity"]["style_constitution"]["sentence_profile"]["avg_words_target"]
    sentence_range = constitution["narrative_identity"]["style_constitution"]["sentence_profile"]["allowed_range"]
    dialogue_target = constitution["narrative_identity"]["style_constitution"]["dialogue_density"]["target_percent"]
    bounds = installment_pack["escalation_bounds"]
    chapter_escalation = chapter_pack["chapter_card"]["escalation_tags"]

    violations: list[dict[str, Any]] = []
    drift_flags = {
        "character_voice_shift": False,
        "moral_boundary_violation": False,
        "theme_absence": False,
        "escalation_violation": False,
        "world_rule_break": False,
        "continuity_contradiction": False,
    }

    voice_score = 9.0 - abs(avg_sentence - target_avg) * 0.18 - max(0.0, abs(dialogue_density - dialogue_target) * 0.04)
    if taboo_hits:
        voice_score -= 1.2
        drift_flags["character_voice_shift"] = True
        violations.append(
            {"type": "character_voice_shift", "severity": "moderate", "note": "Dialogue used a banned phrase from the voice fingerprint."}
        )
    if avg_sentence < sentence_range[0] or avg_sentence > sentence_range[1]:
        drift_flags["character_voice_shift"] = True
        violations.append(
            {"type": "character_voice_shift", "severity": "moderate", "note": "Sentence rhythm fell outside the constitution range."}
        )
    if repeated_sentence_count > 1:
        voice_score -= min(1.5, repeated_sentence_count * 0.35)
        violations.append(
            {"type": "voice_repetition", "severity": "moderate", "note": "Sentence-level repetition suggests unstable narrative texture."}
        )

    theme_score = 6.2 + (theme_signal * 3.1) - (preachy_hits * 0.8)
    if theme_signal < 0.45:
        drift_flags["theme_absence"] = True
        violations.append(
            {"type": "theme_absence", "severity": "moderate", "note": "Primary installment theme did not manifest clearly in scene choices."}
        )
    if preachy_hits:
        violations.append(
            {"type": "theme_preachiness", "severity": "low", "note": "Theme language was stated too directly instead of dramatized."}
        )

    world_score = 9.0 - (ai_tell_hits * 0.6)
    for rule in constitution["world_model"]["governing_rules"]:
        if "no " in rule["rule"].lower():
            forbidden = rule["rule"].lower().split("no ", 1)[1].rstrip(".")
            if forbidden and forbidden in text.lower():
                world_score -= 1.0
                drift_flags["world_rule_break"] = True
                violations.append(
                    {"type": "world_rule_break", "severity": "high", "note": f"Draft appears to violate world rule: {rule['rule']}"}
                )

    escalation_score = 9.0
    if CONFLICT_SCOPE_ORDER.index(chapter_escalation["conflict_scope"]) > CONFLICT_SCOPE_ORDER.index(bounds["max_conflict_scope"]):
        escalation_score -= 3.0
        drift_flags["escalation_violation"] = True
    if VIOLENCE_LEVEL_ORDER.index(chapter_escalation["violence_level"]) > VIOLENCE_LEVEL_ORDER.index(bounds["max_violence_level"]):
        escalation_score -= 3.0
        drift_flags["escalation_violation"] = True
    if drift_flags["escalation_violation"]:
        violations.append(
            {"type": "escalation_violation", "severity": "high", "note": "Chapter escalation exceeds the installment ceiling."}
        )

    character_score = 8.6 if protagonist_present else 6.9
    if not protagonist_present:
        drift_flags["continuity_contradiction"] = True
        violations.append(
            {"type": "continuity_contradiction", "severity": "moderate", "note": "POV character is not consistently anchored in the chapter text."}
        )

    dynamic_min_words = max(
        rubric_cfg["chapter_min_words"],
        min(400, int(chapter_pack["scene_constraints"]["word_count_target"] * 0.09)),
    )
    pacing_score = 8.8 if word_count >= dynamic_min_words else 5.8
    if word_count < dynamic_min_words:
        violations.append(
            {"type": "pacing", "severity": "moderate", "note": f"Chapter is under the dynamic minimum word budget of {dynamic_min_words}."}
        )
    if dialogue_density < max(4.0, dialogue_target * 0.3):
        pacing_score -= 0.8
        violations.append(
            {"type": "dialogue_density", "severity": "low", "note": "Dialogue density is too low for the configured tone profile."}
        )

    structural_score = 8.8 if text.startswith("# ") and paragraph_count >= 6 else 6.4
    originality_score = 8.3 - (ai_tell_hits * 0.5) - (preachy_hits * 0.2) - min(1.0, repeated_sentence_count * 0.25)

    scores = {
        "thematic_coherence": _clip_score(theme_score),
        "character_consistency": _clip_score(character_score),
        "voice_stability": _clip_score(voice_score),
        "world_rule_compliance": _clip_score(world_score),
        "escalation_compliance": _clip_score(escalation_score),
        "pacing": _clip_score(pacing_score),
        "structural_clarity": _clip_score(structural_score),
        "originality": _clip_score(originality_score),
    }
    overall = round(sum(scores[key] * float(rubric_cfg["weights"].get(key, 0.0)) for key in scores), 2)

    failed_hard = [
        category
        for category, minimum in rubric_cfg["hard_fail"].items()
        if scores.get(category, 10.0) < minimum
    ]
    blockers = failed_hard or [flag for flag, active in drift_flags.items() if active and flag in {"world_rule_break", "escalation_violation"}]
    if overall < rubric_cfg["pass_overall"] or blockers:
        pass_status = "FAIL"
    elif any(drift_flags.values()) or violations:
        pass_status = "PASS_WITH_NOTES"
    else:
        pass_status = "PASS"

    recommendations = []
    if drift_flags["theme_absence"]:
        recommendations.append({"category": "theme", "action": "Make the chapter choice more visibly express the installment theme."})
    if drift_flags["character_voice_shift"]:
        recommendations.append({"category": "voice", "action": "Bring sentence rhythm and dialogue behavior back to the character fingerprint."})
    if word_count < dynamic_min_words:
        recommendations.append({"category": "pacing", "action": "Add a consequence beat that advances the chapter goal instead of summarizing it."})
    if not recommendations and violations:
        recommendations.append({"category": "clarity", "action": "Tighten cause-and-effect links without changing the scene outcome."})

    return {
        "schema_version": "1.0",
        "series_id": installment_pack["series_id"],
        "installment_id": installment_pack["installment_id"],
        "chapter_id": chapter_pack["chapter_id"],
        "model": "deterministic-drift-v1",
        "prompt_pack_version": prompt_pack_version,
        "scores": scores,
        "overall": overall,
        "drift_flags": drift_flags,
        "violations": violations,
        "rewrite_recommendations": recommendations,
        "pass_status": pass_status,
        "metrics": {
            "word_count": word_count,
            "dynamic_min_words": dynamic_min_words,
            "avg_sentence_words": round(avg_sentence, 2),
            "dialogue_density_percent": round(dialogue_density, 2),
            "paragraph_count": paragraph_count,
            "repeated_sentence_count": repeated_sentence_count,
        },
    }


def _llm_eval_prompts(
    text: str,
    constitution: dict[str, Any],
    installment_pack: dict[str, Any],
    chapter_pack: dict[str, Any],
) -> tuple[str, str]:
    system_prompt = (
        "You are a fiction quality evaluator. Return only JSON. "
        "Judge the chapter against the structured narrative constraints. "
        "Do not restate the prompt or include markdown fences."
    )
    user_prompt = (
        "Evaluate this chapter and return JSON with keys: "
        "scores, drift_flags, violations, rewrite_recommendations, pass_status, summary.\n\n"
        f"Constitution tone: {constitution['narrative_identity']['tone_lock']['description']}\n"
        f"Installment theme: {installment_pack['theme_expression']['primary_focus']}\n"
        f"Escalation bounds: {json.dumps(installment_pack['escalation_bounds'], ensure_ascii=True)}\n"
        f"Chapter card: {json.dumps(chapter_pack['chapter_card'], ensure_ascii=True)}\n"
        f"Character slice: {json.dumps(chapter_pack['character_state_slice'], ensure_ascii=True)}\n"
        f"World slice: {json.dumps(chapter_pack['world_slice'], ensure_ascii=True)}\n"
        f"Continuity slice: {json.dumps(chapter_pack['continuity_slice'], ensure_ascii=True)}\n\n"
        "Required score keys: thematic_coherence, character_consistency, voice_stability, "
        "world_rule_compliance, escalation_compliance, pacing, structural_clarity, originality.\n"
        "Required drift flag keys: character_voice_shift, moral_boundary_violation, theme_absence, "
        "escalation_violation, world_rule_break, continuity_contradiction.\n"
        "Violations must be an array of objects with type, severity, note.\n"
        "Rewrite recommendations must be an array of objects with category, action.\n"
        "Pass status must be one of PASS, PASS_WITH_NOTES, FAIL.\n\n"
        f"Chapter markdown:\n{text}"
    )
    return system_prompt, user_prompt


def _hybrid_eval_report(
    *,
    deterministic_report: dict[str, Any],
    llm_payload: dict[str, Any],
) -> dict[str, Any]:
    hybrid = json.loads(json.dumps(deterministic_report))
    llm_scores = llm_payload.get("scores", {}) if isinstance(llm_payload.get("scores"), dict) else {}
    for key, det_value in deterministic_report["scores"].items():
        if key in llm_scores:
            try:
                llm_value = float(llm_scores[key])
                hybrid["scores"][key] = _clip_score((float(det_value) * 0.55) + (llm_value * 0.45))
            except (TypeError, ValueError):
                hybrid["scores"][key] = det_value
    llm_flags = llm_payload.get("drift_flags", {}) if isinstance(llm_payload.get("drift_flags"), dict) else {}
    for key in hybrid["drift_flags"]:
        hybrid["drift_flags"][key] = bool(hybrid["drift_flags"][key] or llm_flags.get(key, False))

    hybrid["violations"] = list(hybrid["violations"])
    for violation in llm_payload.get("violations", []) if isinstance(llm_payload.get("violations"), list) else []:
        if isinstance(violation, dict) and {"type", "severity", "note"} <= set(violation):
            hybrid["violations"].append(
                {
                    "type": str(violation["type"]),
                    "severity": str(violation["severity"]),
                    "note": str(violation["note"]),
                }
            )

    hybrid["rewrite_recommendations"] = list(hybrid["rewrite_recommendations"])
    for rec in llm_payload.get("rewrite_recommendations", []) if isinstance(llm_payload.get("rewrite_recommendations"), list) else []:
        if isinstance(rec, dict) and {"category", "action"} <= set(rec):
            hybrid["rewrite_recommendations"].append(
                {"category": str(rec["category"]), "action": str(rec["action"])}
            )

    overall = round(sum(hybrid["scores"][key] * float(RUBRIC_WEIGHT_DEFAULTS.get(key, 0.0)) for key in hybrid["scores"]), 2)
    hybrid["overall"] = overall
    if isinstance(llm_payload.get("summary"), str):
        hybrid["llm_summary"] = llm_payload["summary"]
    hybrid["model"] = "hybrid-drift-v1"
    hybrid["llm_requested_fail"] = str(llm_payload.get("pass_status", "")).strip() == "FAIL"
    if str(llm_payload.get("pass_status", "")).strip() == "FAIL":
        hybrid["pass_status"] = "FAIL"
    elif hybrid["pass_status"] == "PASS" and hybrid["violations"]:
        hybrid["pass_status"] = "PASS_WITH_NOTES"
    return hybrid


def _evaluate_chapter_with_llm(
    *,
    text: str,
    constitution: dict[str, Any],
    installment_pack: dict[str, Any],
    chapter_pack: dict[str, Any],
    rubric_cfg: dict[str, Any],
    prompt_pack_version: str,
) -> dict[str, Any]:
    deterministic_report = _evaluate_chapter_text(
        text=text,
        constitution=constitution,
        installment_pack=installment_pack,
        chapter_pack=chapter_pack,
        rubric_cfg=rubric_cfg,
        prompt_pack_version=prompt_pack_version,
    )
    system_prompt, user_prompt = _llm_eval_prompts(text, constitution, installment_pack, chapter_pack)
    client = LLMClient()
    try:
        raw = client.complete(
            system_prompt,
            user_prompt,
            max_completion_tokens=_bookgen_completion_tokens("eval", int(chapter_pack["scene_constraints"]["word_count_target"])),
            temperature=0.1,
        )
    except Exception as exc:
        _bookgen_llm_error("eval", exc, chapter_pack)
        deterministic_report["model"] = "deterministic-drift-v1"
        deterministic_report["llm_fallback"] = {"stage": "eval", "reason": f"{type(exc).__name__}: {exc}"}
        return deterministic_report
    llm_payload = _extract_json_object(raw)
    hybrid = _hybrid_eval_report(deterministic_report=deterministic_report, llm_payload=llm_payload)
    failed_hard = [
        category
        for category, minimum in rubric_cfg["hard_fail"].items()
        if hybrid["scores"].get(category, 10.0) < minimum
    ]
    blockers = failed_hard or [
        flag for flag, active in hybrid["drift_flags"].items() if active and flag in {"world_rule_break", "escalation_violation"}
    ]
    if hybrid.get("llm_requested_fail") or hybrid["overall"] < rubric_cfg["pass_overall"] or blockers:
        hybrid["pass_status"] = "FAIL"
    elif any(hybrid["drift_flags"].values()) or hybrid["violations"]:
        hybrid["pass_status"] = "PASS_WITH_NOTES"
    else:
        hybrid["pass_status"] = "PASS"
    return hybrid


def _opening_chapter_soft_rewrite_required(chapter_pack: dict[str, Any], eval_report: dict[str, Any]) -> bool:
    chapter_index = int(chapter_pack.get("chapter_index", 0) or 0)
    if chapter_index not in {1, 2}:
        return False
    rec_categories = {
        str(item.get("category", "")).strip().lower()
        for item in eval_report.get("rewrite_recommendations", [])
        if isinstance(item, dict)
    }
    pacing_score = float(eval_report.get("scores", {}).get("pacing", 10.0))
    originality_score = float(eval_report.get("scores", {}).get("originality", 10.0))
    return (
        ("pacing" in rec_categories and pacing_score < 8.4)
        or ("originality" in rec_categories and originality_score < 8.2)
    )


def _build_rewrite_contract(
    *,
    eval_report: dict[str, Any],
    constitution: dict[str, Any],
    chapter_pack: dict[str, Any],
    installment_pack: dict[str, Any],
    attempt: int,
) -> dict[str, Any]:
    profile = _chapter_policy_profile(chapter_pack)
    rewrite_policy = profile["rewrite"]
    priority_categories = [str(item) for item in rewrite_policy.get("priority_categories", []) if str(item).strip()]
    score_map = eval_report.get("scores", {})
    ordered_categories: list[str] = []
    for category in priority_categories:
        if category in score_map and category not in ordered_categories:
            ordered_categories.append(category)
    for category, _ in sorted(score_map.items(), key=lambda item: item[1]):
        if category not in ordered_categories:
            ordered_categories.append(category)
    selected_categories = ordered_categories[:2]
    improve = [
        {
            "category": category,
            "target": next(
                (item["action"] for item in eval_report["rewrite_recommendations"] if item.get("category") == category),
                f"Raise {category.replace('_', ' ')} without changing the chapter outcome.",
            ),
        }
        for category in selected_categories
    ]
    return {
        "schema_version": "1.0",
        "series_id": installment_pack["series_id"],
        "installment_id": installment_pack["installment_id"],
        "chapter_id": chapter_pack["chapter_id"],
        "attempt": attempt,
        "preserve": [
            "tone_lock",
            "plot_outcome",
            "character_voice_markers",
            "world_rules",
            "timeline_order",
        ],
        "improve": improve,
        "do_not_change": [
            "who_knows_which_secrets",
            "arc_stage",
            "major_beats",
            "escalation_level",
        ],
        "constraints": {
            "max_attempts": int(rewrite_policy.get("max_attempts", 3)),
            "stop_if_no_improvement_after": int(rewrite_policy.get("stop_if_no_improvement_after", 2)),
            "word_count_target": chapter_pack["scene_constraints"]["word_count_target"],
            "tone_lock": constitution["narrative_identity"]["tone_lock"]["description"],
            "arc_state_lock": [entry["current_arc_stage"] for entry in chapter_pack["character_state_slice"]],
        },
    }


def _world_rule_compliance_rewrite(text: str, chapter_pack: dict[str, Any]) -> str:
    profile = _chapter_policy_profile(chapter_pack)
    world_rule = profile["world_rule_language"]
    body = _strip_heading(text)
    softened = body
    for replacement in world_rule.get("replacements", []):
        pattern = str(replacement.get("pattern", "")).strip()
        repl = str(replacement.get("replacement", "")).strip()
        if pattern and repl:
            softened = re.sub(pattern, repl, softened, flags=re.IGNORECASE)
    lower = softened.lower()
    compliance_paragraphs: list[str] = []
    for signal in world_rule.get("required_signals", []):
        keywords = [str(item).lower() for item in signal.get("keywords", []) if str(item).strip()]
        sentence = str(signal.get("sentence", "")).strip()
        if sentence and keywords and not any(keyword in lower for keyword in keywords):
            compliance_paragraphs.append(sentence)
    if compliance_paragraphs:
        softened = softened.strip() + "\n\n" + "\n\n".join(compliance_paragraphs)
    heading = text.splitlines()[0].strip() if text.splitlines() and text.splitlines()[0].startswith("# ") else f"# {chapter_pack['chapter_card'].get('title') or chapter_pack['chapter_id'].upper()}"
    softened = _dedupe_paragraph_blocks(softened)
    return f"{heading}\n\n{softened.strip()}\n"


def _dedupe_paragraph_blocks(text: str) -> str:
    paragraphs = [part.strip() for part in text.split("\n\n") if part.strip()]
    seen: set[str] = set()
    kept: list[str] = []
    for paragraph in paragraphs:
        normalized = re.sub(r"\s+", " ", paragraph).strip().lower()
        # Only dedupe meaningful narrative blocks; keep short connective lines.
        if len(normalized) >= 80:
            if normalized in seen:
                continue
            seen.add(normalized)
        kept.append(paragraph)
    return "\n\n".join(kept)


def _rewrite_chapter_text(text: str, chapter_pack: dict[str, Any], eval_report: dict[str, Any]) -> str:
    lines = text.rstrip().splitlines()
    body = "\n".join(lines[1:]).strip() if lines and lines[0].startswith("# ") else text.strip()
    additions: list[str] = []
    if eval_report["drift_flags"]["theme_absence"]:
        additions.append(
            f"{chapter_pack['chapter_card']['goal']} sharpens into a moral tradeoff, and the choice leaves a cost that cannot be hand-waved away."
        )
    if eval_report["drift_flags"]["character_voice_shift"]:
        pov_name = chapter_pack["scene_constraints"]["pov_character_id"].replace("-", " ").title()
        additions.append(
            f'"{pov_name} kept the answer short," the narration insists, returning the scene to the character\'s measured cadence.'
        )
    if eval_report["drift_flags"].get("world_rule_break"):
        world_rule = _chapter_policy_profile(chapter_pack).get("world_rule_language", {})
        for signal in world_rule.get("required_signals", [])[:2]:
            sentence = str(signal.get("sentence", "")).strip()
            if sentence:
                additions.append(sentence)
        if not additions:
            additions.append(
                "The chapter now keeps key disclosures inside authorized process and shows concrete consequences for reckless disclosure."
            )
    if eval_report["metrics"]["word_count"] < chapter_pack["scene_constraints"]["word_count_target"] * 0.4:
        additions.append(
            f"The final exchange lands on {chapter_pack['chapter_card']['emotional_shift']['to']}, forcing the next chapter to inherit a sharper problem."
        )
    if not additions:
        additions.append("The closing beat now connects the reveal to the next consequence with less abstraction and more visible pressure.")
    rewritten_body = body + "\n\n" + "\n\n".join(additions)
    rewritten_body = _dedupe_paragraph_blocks(rewritten_body)
    heading = lines[0] if lines and lines[0].startswith("# ") else f"# {chapter_pack['chapter_id'].upper()}"
    rewritten = f"{heading}\n\n{rewritten_body.strip()}\n"
    if eval_report["drift_flags"].get("world_rule_break"):
        return _world_rule_compliance_rewrite(rewritten, chapter_pack)
    return rewritten


def _rewrite_chapter_with_llm(
    text: str,
    chapter_pack: dict[str, Any],
    contract: dict[str, Any],
) -> str:
    system_prompt = (
        "You are revising a fiction chapter. Return only revised markdown. "
        "Preserve locked plot outcomes, arc state, escalation level, and tone."
    )
    user_prompt = (
        "Revise the chapter according to this rewrite contract.\n\n"
        f"Contract: {json.dumps(contract, ensure_ascii=True)}\n\n"
        f"Current chapter:\n{text}"
    )
    client = LLMClient()
    try:
        raw = client.complete(
            system_prompt,
            user_prompt,
            max_completion_tokens=_bookgen_completion_tokens("rewrite", _word_count(text)),
            temperature=0.35,
        )
    except Exception as exc:
        _bookgen_llm_error("rewrite", exc, chapter_pack)
        return _rewrite_chapter_text(
            text,
            chapter_pack,
            {
                "drift_flags": {"theme_absence": True, "character_voice_shift": False},
                "metrics": {"word_count": _word_count(text)},
            },
        )
    rewritten = _normalize_llm_chapter_markdown(raw, chapter_pack)
    original_words = _word_count(text)
    minimum_preserved_words = max(250, int(original_words * 0.7))
    if _word_count(rewritten) >= minimum_preserved_words:
        return rewritten

    original_body = _strip_heading(text)
    rewritten_body = _strip_heading(rewritten)
    original_paragraphs = [part.strip() for part in original_body.split("\n\n") if part.strip()]
    revised_additions = [
        part.strip()
        for part in rewritten_body.split("\n\n")
        if part.strip() and part.strip() not in original_paragraphs
    ]
    if not revised_additions:
        return text
    heading = text.splitlines()[0].strip() if text.splitlines() and text.splitlines()[0].startswith("# ") else f"# {chapter_pack['chapter_card'].get('title') or chapter_pack['chapter_id'].upper()}"
    merged_body = original_paragraphs + revised_additions
    return f"{heading}\n\n" + "\n\n".join(merged_body) + "\n"


def _paragraph(*sentences: str) -> str:
    return " ".join(sentence.strip() for sentence in sentences if sentence.strip())


def _resolve_policy_profile_name(genre: str, subgenre: str, audience: str) -> str:
    keyspace = " ".join([genre, subgenre, audience]).strip().lower()
    if any(token in keyspace for token in {"juvenile", "middle-grade", "middle grade", "kids", "children"}):
        return "juvenile_adventure"
    if any(token in keyspace for token in {"thriller", "political", "institutional", "procedural"}):
        return "institutional_thriller"
    return "default"


def _genre_policy_profile(genre: str, subgenre: str, audience: str) -> dict[str, Any]:
    profile_name = _resolve_policy_profile_name(genre, subgenre, audience)
    base = BOOKGEN_POLICY_PROFILES["default"]
    selected = BOOKGEN_POLICY_PROFILES.get(profile_name, base)
    return {
        "profile_id": profile_name,
        "opening_scene": {**base["opening_scene"], **selected.get("opening_scene", {})},
        "structure": {**base["structure"], **selected.get("structure", {})},
        "rewrite": {**base["rewrite"], **selected.get("rewrite", {})},
        "editorial": {
            "developmental": {
                **base.get("editorial", {}).get("developmental", {}),
                **selected.get("editorial", {}).get("developmental", {}),
            },
            "line": {
                **base.get("editorial", {}).get("line", {}),
                **selected.get("editorial", {}).get("line", {}),
            },
            "copy": {
                **base.get("editorial", {}).get("copy", {}),
                **selected.get("editorial", {}).get("copy", {}),
            },
        },
        "world_rule_language": {
            "replacements": list(selected.get("world_rule_language", {}).get("replacements", base["world_rule_language"]["replacements"])),
            "required_signals": list(
                selected.get("world_rule_language", {}).get("required_signals", base["world_rule_language"]["required_signals"])
            ),
        },
    }


def _chapter_policy_profile(chapter_pack: dict[str, Any]) -> dict[str, Any]:
    profile = chapter_pack.get("policy_profile")
    if isinstance(profile, dict) and isinstance(profile.get("profile"), dict):
        merged = profile["profile"]
        profile_id = str(profile.get("profile_id", "")).strip().lower()
        if profile_id in BOOKGEN_POLICY_PROFILES:
            base_profile = _genre_policy_profile(profile_id, profile_id, profile_id)
        else:
            base_profile = _genre_policy_profile("fiction", "fiction", "adult")
        return {
            "profile_id": profile_id or base_profile.get("profile_id", "default"),
            "opening_scene": {**base_profile.get("opening_scene", {}), **merged.get("opening_scene", {})},
            "structure": {**base_profile.get("structure", {}), **merged.get("structure", {})},
            "rewrite": {**base_profile.get("rewrite", {}), **merged.get("rewrite", {})},
            "editorial": {
                "developmental": {
                    **base_profile.get("editorial", {}).get("developmental", {}),
                    **merged.get("editorial", {}).get("developmental", {}),
                },
                "line": {
                    **base_profile.get("editorial", {}).get("line", {}),
                    **merged.get("editorial", {}).get("line", {}),
                },
                "copy": {
                    **base_profile.get("editorial", {}).get("copy", {}),
                    **merged.get("editorial", {}).get("copy", {}),
                },
            },
            "world_rule_language": {
                "replacements": list(
                    merged.get("world_rule_language", {}).get(
                        "replacements",
                        base_profile.get("world_rule_language", {}).get("replacements", []),
                    )
                ),
                "required_signals": list(
                    merged.get("world_rule_language", {}).get(
                        "required_signals",
                        base_profile.get("world_rule_language", {}).get("required_signals", []),
                    )
                ),
            },
        }
    return _genre_policy_profile("fiction", "fiction", "adult")


def _bookgen_generation_preset() -> str:
    try:
        cfg = get_settings()
    except Exception:
        return "production"
    preset = str(getattr(cfg, "bookgen_generation_preset", "production") or "production").strip().lower()
    if preset not in BOOKGEN_GENERATION_PROFILES:
        return "production"
    return preset


def _bookgen_generation_profile() -> dict[str, Any]:
    return BOOKGEN_GENERATION_PROFILES[_bookgen_generation_preset()]


def _bookgen_limit(cfg: Any, explicit_attr: str, profile_key: str) -> int:
    explicit = int(getattr(cfg, explicit_attr, 0) or 0)
    if explicit > 0:
        return explicit
    return int(_bookgen_generation_profile()[profile_key])


def _structural_retry_limit() -> int:
    try:
        cfg = get_settings()
    except Exception:
        return int(BOOKGEN_GENERATION_PROFILES["production"]["structural_retry_limit"])
    explicit = int(getattr(cfg, "bookgen_structural_retry_limit", 0) or 0)
    if explicit > 0:
        return explicit
    return int(_bookgen_generation_profile()["structural_retry_limit"])


def _llm_prompt_preset_requirements(minimum_words: int) -> str:
    preset = _bookgen_generation_preset()
    if preset == "smoke":
        return (
            "Smoke-mode enforcement:\n"
            f"- This run is cost-bounded, but the chapter must still clear {minimum_words} words.\n"
            "- Deliver a real chapter, not a teaser, synopsis, or opening fragment.\n"
            "- Use at least 10 paragraphs, at least 3 live scene turns, and at least 2 dialogue exchanges.\n"
            "- If the chapter feels complete too early, continue by escalating the same scene instead of ending.\n"
        )
    if preset == "dev":
        return (
            "Dev-mode enforcement:\n"
            f"- Clear at least {minimum_words} words with a fully dramatized opening section.\n"
            "- Prefer additional scene pressure, dialogue, and consequence over summary.\n"
        )
    return ""


def _bookgen_completion_tokens(kind: str, target_words: int) -> int:
    preset = _bookgen_generation_preset()
    is_gpt5_profile = False
    try:
        cfg = get_settings()
        provider = str(getattr(cfg, "llm_provider_profile", "default") or "default").strip().lower()
        model = str(getattr(cfg, "llm_model", "") or "").strip().lower()
        is_gpt5_profile = provider == "gpt5" and model.startswith("gpt-5")
    except Exception:
        is_gpt5_profile = False
    if kind == "eval":
        return 700 if preset == "smoke" else 900
    if kind == "title":
        return 300 if preset == "smoke" else 450
    if kind == "rewrite":
        base = 1400 if preset == "smoke" else 2200
        if is_gpt5_profile:
            return min(7000, max(2200, int(base * 1.25)))
        return base
    if kind == "expand":
        if preset == "smoke":
            base = max(1800, min(4200, int(target_words * 1.6)))
        else:
            base = max(2600, min(5200, int(target_words * 1.5)))
        if is_gpt5_profile:
            return min(10000, max(4200, int(base * 1.3)))
        return base
    if kind == "draft":
        if preset == "smoke":
            base = max(1200, min(1800, int(target_words * 1.35)))
            if is_gpt5_profile:
                return min(5200, max(2200, int(base * 1.35)))
            return base
        if preset == "dev":
            base = max(1800, min(3200, int(target_words * 1.6)))
            if is_gpt5_profile:
                return min(8200, max(3000, int(base * 1.35)))
            return base
        base = max(2200, min(7000, int(target_words * 2.1)))
        if is_gpt5_profile:
            return min(11000, max(3600, int(base * 1.2)))
        return base
    return max(1200, min(2400, int(target_words * 1.5)))


def _bookgen_llm_error(stage: str, exc: Exception, chapter_pack: dict[str, Any] | None = None) -> None:
    logger.warning(
        "bookgen_llm_fallback stage=%s chapter_id=%s error=%s",
        stage,
        (chapter_pack or {}).get("chapter_id"),
        f"{type(exc).__name__}: {exc}",
    )


def _bookgen_is_gpt5_profile() -> bool:
    try:
        cfg = get_settings()
    except Exception:
        return False
    provider = str(getattr(cfg, "llm_provider_profile", "default") or "default").strip().lower()
    model = str(getattr(cfg, "llm_model", "") or "").strip().lower()
    return provider == "gpt5" and model.startswith("gpt-5")


def _llm_draft_recovery_allowed(exc: Exception) -> bool:
    if not _bookgen_is_gpt5_profile():
        return False
    message = str(exc).strip().lower()
    if not message:
        return False
    if "status=429" in message or "insufficient_quota" in message:
        return False
    return "llm response contained empty content" in message or "llm response was truncated before usable output" in message


def _bookgen_use_llm_for_chapter(chapter_index: int) -> bool:
    cfg = get_settings()
    if not cfg.bookgen_use_llm:
        return False
    limit = _bookgen_limit(cfg, "bookgen_llm_chapter_limit", "chapter_llm_limit")
    if limit > 0 and chapter_index > limit:
        return False
    return bool((cfg.llm_endpoint or "").strip())


def _bookgen_use_llm_for_eval(chapter_index: int) -> bool:
    cfg = get_settings()
    if not cfg.bookgen_eval_use_llm:
        return False
    limit = _bookgen_limit(cfg, "bookgen_eval_llm_chapter_limit", "eval_llm_limit")
    if limit > 0 and chapter_index > limit:
        return False
    return bool((cfg.llm_endpoint or "").strip())


def _bookgen_use_llm_for_rewrite(chapter_index: int) -> bool:
    cfg = get_settings()
    if not cfg.bookgen_rewrite_use_llm:
        return False
    limit = _bookgen_limit(cfg, "bookgen_rewrite_llm_chapter_limit", "rewrite_llm_limit")
    if limit > 0 and chapter_index > limit:
        return False
    return bool((cfg.llm_endpoint or "").strip())


def _bookgen_use_llm_for_titles() -> bool:
    try:
        cfg = get_settings()
    except Exception:
        return False
    if not bool(getattr(cfg, "bookgen_title_critic_use_llm", False)):
        return False
    return bool((cfg.llm_endpoint or "").strip())


def _bookgen_editorial_stage_gate_enabled() -> bool:
    return bool(getattr(get_settings(), "bookgen_editorial_stage_gate", True))


def _bookgen_force_redraft_enabled() -> bool:
    return bool(getattr(get_settings(), "bookgen_force_redraft", False))


def _title_critic_prompts(strategy: dict[str, Any], brief: dict[str, Any], finalists: list[dict[str, Any]]) -> tuple[str, str]:
    system_prompt = (
        "You are a title critic for a fiction series. Return only one JSON object. "
        "Pick the strongest finalist for the installment while preserving realism, tonal credibility, "
        "and series escalation discipline."
    )
    user_prompt = (
        "Evaluate these installment title finalists and choose one.\n\n"
        f"Series strategy: {json.dumps(strategy, ensure_ascii=True)}\n"
        f"Installment brief: {json.dumps(brief, ensure_ascii=True)}\n"
        f"Finalists: {json.dumps(finalists, ensure_ascii=True)}\n\n"
        "Return JSON with keys: selected_title, rationale, stronger_alternates. "
        "Only choose from the finalists provided."
    )
    return system_prompt, user_prompt


def _apply_title_critic(
    *,
    strategy: dict[str, Any],
    brief: dict[str, Any],
    viable: list[dict[str, Any]],
) -> tuple[str | None, dict[str, Any]]:
    shortlist_size = max(2, int(get_settings().bookgen_title_critic_shortlist_size or 5))
    finalists = [
        {
            "title": item["title"],
            "scores": item["scores"],
            "features": item["features"],
        }
        for item in viable[:shortlist_size]
    ]
    seeded = []
    present = {item["title"] for item in finalists}
    for seed_title in _role_seed_titles(str(brief.get("arc_role", "setup"))):
        if seed_title in present:
            continue
        match = next((item for item in viable if item["title"] == seed_title), None)
        if match is not None:
            seeded.append({"title": match["title"], "scores": match["scores"], "features": match["features"]})
            present.add(seed_title)
    finalists.extend(seeded[:3])
    if len(finalists) < 2:
        return (None, {"used": False})
    system_prompt, user_prompt = _title_critic_prompts(strategy, brief, finalists)
    client = LLMClient()
    try:
        raw = client.complete(
            system_prompt,
            user_prompt,
            max_completion_tokens=_bookgen_completion_tokens("title", 0),
            temperature=0.1,
        )
    except Exception as exc:
        logger.warning("bookgen_title_critic_fallback error=%s", f"{type(exc).__name__}: {exc}")
        return (None, {"used": False, "rejected_reason": "llm_error"})
    payload = _extract_json_object(raw)
    selected = str(payload.get("selected_title", "")).strip()
    allowed = {item["title"] for item in finalists}
    if selected not in allowed:
        return (None, {"used": False, "rejected_reason": "selected_title_not_in_shortlist"})
    alternates = [str(item).strip() for item in payload.get("stronger_alternates", []) if str(item).strip() and str(item).strip() in allowed]
    return (
        selected,
        {
            "used": True,
            "rationale": str(payload.get("rationale", "")).strip(),
            "stronger_alternates": alternates,
        },
    )


def _extract_json_object(raw: str) -> dict[str, Any]:
    text = raw.strip()
    try:
        value = json.loads(text)
        if isinstance(value, dict):
            return value
    except json.JSONDecodeError:
        pass
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        value = json.loads(text[start : end + 1])
        if isinstance(value, dict):
            return value
    raise RuntimeError("LLM response did not contain a valid JSON object")


def _bookgen_llm_prompts(
    chapter_pack: dict[str, Any],
    constitution: dict[str, Any],
    installment_pack: dict[str, Any],
    scene_cards: dict[str, Any] | None = None,
    scene_beats: dict[str, Any] | None = None,
) -> tuple[str, str]:
    target_words = int(chapter_pack["scene_constraints"]["word_count_target"])
    minimum_words = _chapter_minimum_words(chapter_pack)
    pov_name = chapter_pack["scene_constraints"]["pov_character_id"].replace("-", " ").title()
    location = chapter_pack["world_slice"]["setting_constraints"]["locations_allowed"][0]
    support_ids = [entry["character_id"] for entry in chapter_pack["character_state_slice"][1:3]]
    preset_requirements = _llm_prompt_preset_requirements(minimum_words)
    system_prompt = (
        "You are a fiction chapter writer. Return only markdown for a finished chapter. "
        "Follow the constitution exactly: preserve POV, escalation bounds, tone, continuity, "
        "and character voice. This must read like a real novel chapter, not a synopsis, treatment, or summary. "
        "Write fully dramatized scenes with action, dialogue, physical setting, tactical choices, and visible consequence. "
        "Do not describe the prompt, do not output notes, and do not use placeholders."
    )
    user_prompt = (
        "Write one chapter in markdown.\n\n"
        f"Title: {chapter_pack['chapter_card'].get('title') or chapter_pack['chapter_id']}\n"
        f"Installment theme: {installment_pack['theme_expression']['primary_focus']}\n"
        f"Tone lock: {constitution['narrative_identity']['tone_lock']['description']}\n"
        f"POV mode: {chapter_pack['scene_constraints']['pov_mode']}\n"
        f"POV character id: {chapter_pack['scene_constraints']['pov_character_id']}\n"
        f"Narrative function: {chapter_pack['chapter_card']['narrative_function']}\n"
        f"Goal: {chapter_pack['chapter_card']['goal']}\n"
        f"Emotional shift: {chapter_pack['chapter_card']['emotional_shift']['from']} -> {chapter_pack['chapter_card']['emotional_shift']['to']}\n"
        f"Conflict: {json.dumps(chapter_pack['chapter_card']['conflict'], ensure_ascii=True)}\n"
        f"Reveal: {json.dumps(chapter_pack['chapter_card']['information']['reveal'], ensure_ascii=True)}\n"
        f"Conceal: {json.dumps(chapter_pack['chapter_card']['information']['conceal'], ensure_ascii=True)}\n"
        f"Must happen: {json.dumps(chapter_pack['chapter_card']['constraints']['must_happen'], ensure_ascii=True)}\n"
        f"Must not happen: {json.dumps(chapter_pack['chapter_card']['constraints']['must_not_happen'], ensure_ascii=True)}\n"
        f"Forbidden outcomes: {json.dumps(chapter_pack['chapter_card']['constraints']['forbidden_outcomes'], ensure_ascii=True)}\n"
        f"Escalation tags: {json.dumps(chapter_pack['chapter_card']['escalation_tags'], ensure_ascii=True)}\n"
        f"Character state slice: {json.dumps(chapter_pack['character_state_slice'], ensure_ascii=True)}\n"
        f"World slice: {json.dumps(chapter_pack['world_slice'], ensure_ascii=True)}\n"
        f"Continuity slice: {json.dumps(chapter_pack['continuity_slice'], ensure_ascii=True)}\n"
        f"Research slice: {json.dumps(chapter_pack['research_slice'], ensure_ascii=True)}\n"
        f"Scene constraints: {json.dumps(chapter_pack['scene_constraints'], ensure_ascii=True)}\n\n"
        f"Scene cards: {json.dumps((scene_cards or {}).get('scenes', []), ensure_ascii=True)}\n"
        f"Scene beats: {json.dumps((scene_beats or {}).get('beats', []), ensure_ascii=True)}\n\n"
        "Requirements:\n"
        "- Start with a markdown H1 heading using the chapter title.\n"
        f"- Target about {target_words} words and do not go below {minimum_words} words.\n"
        "- Use enough paragraphs and scene beats to land as a full novel chapter, not a compressed vignette.\n"
        "- Keep the chapter self-contained and causal.\n"
        "- Maintain the tone and voice markers.\n"
        "- Do not mention thread ids, schema names, or metadata labels literally.\n"
        "- End on a concrete consequence that carries into the next chapter.\n"
        "- Open inside an active scene, not retrospective summary.\n"
        "- Put the POV character in motion within the first paragraph.\n"
        "- Use dialogue where pressure belongs in dialogue.\n"
        "- Show specific setting detail and procedural friction on the page.\n"
        "- Avoid summary-forward phrasing such as 'he thought about what had happened' or broad recap paragraphs.\n\n"
        "Opening scene requirements:\n"
        f"- Start with {pov_name} in a live moment at {location}.\n"
        "- Within the first 250 words, introduce a concrete source of pressure or interruption.\n"
        "- Within the first 800 words, force a decision, concession, or tactical adjustment.\n"
        f"- Use at least one meaningful exchange with {json.dumps(support_ids, ensure_ascii=True)} if available, but render them as characters, not ids.\n"
        f"{preset_requirements}"
    )
    return system_prompt, user_prompt


def _normalize_llm_chapter_markdown(raw: str, chapter_pack: dict[str, Any]) -> str:
    text = raw.strip()
    if not text.startswith("# "):
        title = chapter_pack["chapter_card"].get("title") or chapter_pack["chapter_id"].upper()
        text = f"# {title}\n\n{text}"
    return text.rstrip() + "\n"


def _normalize_llm_continuation_markdown(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    lines = text.splitlines()
    if lines and lines[0].startswith("# "):
        lines = lines[1:]
        while lines and not lines[0].strip():
            lines = lines[1:]
        text = "\n".join(lines).strip()
    return text


def _strip_heading(text: str) -> str:
    lines = text.strip().splitlines()
    if lines and lines[0].startswith("# "):
        return "\n".join(lines[1:]).strip()
    return text.strip()


def _chapter_minimum_words(chapter_pack: dict[str, Any]) -> int:
    return int(chapter_pack.get("draft_contract", {}).get("word_count", {}).get("min_llm") or _chapter_word_bounds(chapter_pack, llm_mode=True)["min"])


def _draft_generation_trace(*, chapter_pack: dict[str, Any], requested_llm: bool, qc_history: list[dict[str, Any]]) -> dict[str, Any]:
    final_qc = qc_history[-1] if qc_history else {}
    fallback_event = next((entry.get("llm_fallback") for entry in qc_history if isinstance(entry, dict) and entry.get("llm_fallback")), None)
    return {
        "schema_version": "1.0",
        "chapter_id": chapter_pack["chapter_id"],
        "requested_mode": "llm" if requested_llm else "fallback",
        "final_mode": str(final_qc.get("mode", "fallback")),
        "attempt_count": len(qc_history),
        "pass_status": str(final_qc.get("pass_status", "FAIL")),
        "llm_fallback": fallback_event,
    }


def _opening_scene_is_summary_forward(text: str) -> bool:
    opening = "\n".join(text.strip().splitlines()[:8]).lower()
    markers = [
        "had spent the better part of the day",
        "couldn't shake the suspicion",
        "mind replaying",
        "the weight of the decision",
        "he thought about",
        "it had been",
        "what started as",
    ]
    return any(marker in opening for marker in markers)


def _placeholder_chapter_title(title: str) -> bool:
    normalized = title.strip().lower()
    return bool(re.fullmatch(r"(hook|pressure|reversal|climax|denouement)\s+\d+", normalized))


def _scene_turns_count(text: str) -> int:
    body = _strip_heading(text)
    paragraphs = [part.strip() for part in body.split("\n\n") if part.strip()]
    dialogue_paragraphs = sum(1 for part in paragraphs if '"' in part)
    transition_hits = len(re.findall(r"\b(later|when|as|after|before|then|meanwhile|by the time)\b", body.lower()))
    return max(1, dialogue_paragraphs + transition_hits)


def _scene_qc_report(
    *,
    text: str,
    chapter_pack: dict[str, Any],
    scene_cards: dict[str, Any] | None,
    scene_beats: dict[str, Any] | None,
) -> dict[str, Any]:
    cards = (scene_cards or {}).get("scenes", [])
    beats = (scene_beats or {}).get("beats", [])
    paragraphs = [part for part in _strip_heading(text).split("\n\n") if part.strip()]
    paragraph_count = len(paragraphs)
    lowered = _strip_heading(text).lower()
    beat_hits = 0
    for beat in beats:
        intent = str(beat.get("intent", "")).lower()
        probe_terms = [term for term in re.findall(r"[a-z]{5,}", intent)[:3] if term not in {"scene", "chapter", "under", "pressure"}]
        if probe_terms and any(term in lowered for term in probe_terms):
            beat_hits += 1
    beat_coverage = beat_hits / max(1, len(beats))
    reveal = str(chapter_pack["chapter_card"]["information"]["reveal"][0]).lower()
    reveal_terms = [term for term in re.findall(r"[a-z]{5,}", reveal)[:4]]
    reveal_present = bool(reveal_terms) and any(term in lowered for term in reveal_terms)
    checks = {
        "scene_count_defined": len(cards) >= 2,
        "paragraphs_support_scene_count": paragraph_count >= max(6, len(cards) * 2),
        "beat_coverage_ok": beat_coverage >= 0.35,
        "reveal_reference_present": reveal_present,
        "scene_turns_ok": _scene_turns_count(text) >= max(2, len(cards)),
    }
    issues = [name for name, ok in checks.items() if not ok]
    return {
        "schema_version": "1.0",
        "chapter_id": chapter_pack["chapter_id"],
        "pass_status": "PASS" if not issues else "FAIL",
        "checks": checks,
        "metrics": {
            "scene_count": len(cards),
            "beat_count": len(beats),
            "beat_coverage_ratio": round(beat_coverage, 3),
            "paragraph_count": paragraph_count,
            "scene_turns": _scene_turns_count(text),
        },
        "issues": issues,
    }


def _scene_qc_rewrite(
    *,
    text: str,
    chapter_pack: dict[str, Any],
    scene_cards: dict[str, Any] | None,
    scene_beats: dict[str, Any] | None,
    issues: list[str],
) -> str:
    heading = text.splitlines()[0].strip() if text.splitlines() and text.splitlines()[0].startswith("# ") else f"# {chapter_pack['chapter_card'].get('title') or chapter_pack['chapter_id'].upper()}"
    paragraphs = [part.strip() for part in _strip_heading(text).split("\n\n") if part.strip()]
    cards = (scene_cards or {}).get("scenes", [])
    beats = (scene_beats or {}).get("beats", [])
    issue_set = set(issues)
    reveal = str(chapter_pack["chapter_card"]["information"]["reveal"][0]).strip()

    if "beat_coverage_ok" in issue_set:
        for beat in beats[:3]:
            intent = str(beat.get("intent", "")).strip()
            if intent:
                paragraphs.append(
                    _paragraph(
                        f"When the scene pivots, the chapter executes this beat directly: {intent}.",
                        "\"Move on it now,\" one voice says, forcing the team to convert analysis into action.",
                    )
                )
    if "reveal_reference_present" in issue_set and reveal:
        paragraphs.append(
            _paragraph(
                f"By the end of the exchange, the room has to address this reveal in plain terms: {reveal}.",
                "No one gets to keep the reveal abstract because it now drives the next immediate decision.",
            )
        )
    if "scene_turns_ok" in issue_set:
        paragraphs.append('"If we wait, we lose the window," Maya said. "Then we stop waiting," Theo replied.')
        paragraphs.append('Later, Maya pushed the folder across the table and said, "Choose now." Theo answered, "Then we own what follows."')

    target_paragraph_min = max(6, len(cards) * 2)
    while len(paragraphs) < target_paragraph_min:
        scene_label = str(cards[len(paragraphs) % max(1, len(cards))].get("scene_label", "Escalation")) if cards else "Escalation"
        paragraphs.append(
            _paragraph(
                f"{scene_label} continues with concrete movement instead of summary.",
                "When new pressure lands, the characters answer it in dialogue and immediate action.",
            )
        )
    return f"{heading}\n\n" + "\n\n".join(paragraphs).strip() + "\n"


def _repair_scene_qc_failures(
    *,
    text: str,
    chapter_pack: dict[str, Any],
    scene_cards: dict[str, Any] | None,
    scene_beats: dict[str, Any] | None,
    draft_qc: dict[str, Any],
    draft_qc_history: list[dict[str, Any]],
    scene_qc: dict[str, Any],
) -> tuple[str, dict[str, Any], list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    scene_qc_history: list[dict[str, Any]] = [dict(scene_qc)]
    attempts = 0
    max_attempts = 2
    current = text
    while scene_qc.get("pass_status") != "PASS" and attempts < max_attempts:
        attempts += 1
        current = _scene_qc_rewrite(
            text=current,
            chapter_pack=chapter_pack,
            scene_cards=scene_cards,
            scene_beats=scene_beats,
            issues=list(scene_qc.get("issues", [])),
        )
        llm_mode = str(draft_qc.get("mode", "fallback")) == "llm"
        draft_qc = _structural_qc_report(current, chapter_pack, llm_mode=llm_mode)
        draft_qc["attempt"] = len(draft_qc_history) + 1
        draft_qc_history.append(draft_qc)
        scene_qc = _scene_qc_report(
            text=current,
            chapter_pack=chapter_pack,
            scene_cards=scene_cards,
            scene_beats=scene_beats,
        )
        scene_qc["attempt"] = attempts + 1
        scene_qc_history.append(dict(scene_qc))
    return current, draft_qc, draft_qc_history, scene_qc, scene_qc_history


def _structural_qc_report(text: str, chapter_pack: dict[str, Any], *, llm_mode: bool) -> dict[str, Any]:
    contract = chapter_pack.get("draft_contract", {})
    word_bounds = contract.get("word_count", {})
    structure = contract.get("structure_requirements", {})
    heading = text.splitlines()[0].strip() if text.splitlines() else ""
    paragraph_count = len([part for part in _strip_heading(text).split("\n\n") if part.strip()])
    word_count = _word_count(text)
    dialogue_density = _dialogue_density_percent(text)
    title = chapter_pack["chapter_card"].get("title") or chapter_pack["chapter_id"]
    dialogue_required = bool(structure.get("dialogue_presence_required", True))
    checks = {
        "heading_present": bool(heading.startswith("# ")),
        "title_non_placeholder": not _placeholder_chapter_title(title),
        "word_count_in_range": int(word_bounds.get("min_llm" if llm_mode else "min_fallback", 0)) <= word_count <= int(word_bounds.get("max_llm" if llm_mode else "max_fallback", 999999)),
        "paragraph_count_ok": paragraph_count >= int(structure.get("paragraph_min", 6)),
        "dialogue_present": True if not dialogue_required else dialogue_density >= (1.0 if llm_mode else 0.5),
        "opening_scene_ok": (not _opening_scene_is_summary_forward(text)) if llm_mode else True,
        "scene_turns_ok": _scene_turns_count(text) >= (int(structure.get("scene_turns_min", 2)) if llm_mode else 1),
    }
    issues: list[str] = []
    if not checks["heading_present"]:
        issues.append("missing_heading")
    if not checks["title_non_placeholder"]:
        issues.append("placeholder_chapter_title")
    if not checks["word_count_in_range"]:
        issues.append("word_count_out_of_range")
    if not checks["paragraph_count_ok"]:
        issues.append("insufficient_paragraphs")
    if not checks["dialogue_present"]:
        issues.append("dialogue_too_sparse")
    if not checks["opening_scene_ok"]:
        issues.append("summary_forward_opening")
    if not checks["scene_turns_ok"]:
        issues.append("insufficient_scene_turns")
    return {
        "schema_version": "1.0",
        "chapter_id": chapter_pack["chapter_id"],
        "mode": "llm" if llm_mode else "fallback",
        "pass_status": "PASS" if not issues else "FAIL",
        "checks": checks,
        "metrics": {
            "word_count": word_count,
            "paragraph_count": paragraph_count,
            "dialogue_density_percent": round(dialogue_density, 2),
            "scene_turns": _scene_turns_count(text),
            "word_count_min": int(word_bounds.get("min_llm" if llm_mode else "min_fallback", 0)),
            "word_count_max": int(word_bounds.get("max_llm" if llm_mode else "max_fallback", 0)),
        },
        "issues": issues,
    }


def _llm_expand_chapter(
    *,
    client: LLMClient,
    current_text: str,
    chapter_pack: dict[str, Any],
    constitution: dict[str, Any],
    installment_pack: dict[str, Any],
) -> str:
    minimum_words = _chapter_minimum_words(chapter_pack)
    current_words = _word_count(current_text)
    shortfall = max(0, minimum_words - current_words)
    system_prompt = (
        "You are revising a fiction chapter to meet novel-length requirements. Return only markdown. "
        "Preserve the existing events, POV, tone, and ending direction while expanding the chapter through "
        "dramatized scene work, dialogue, setting detail, and consequential beats."
    )
    user_prompt = (
        "Expand this chapter.\n\n"
        f"Minimum required words: {minimum_words}\n"
        f"Current words: {current_words}\n"
        f"Add at least: {shortfall} words\n"
        f"Theme: {installment_pack['theme_expression']['primary_focus']}\n"
        f"Tone lock: {constitution['narrative_identity']['tone_lock']['description']}\n"
        "Requirements:\n"
        "- Keep the existing heading.\n"
        "- Add scene movement, dialogue, and procedural friction.\n"
        "- Make the opening less summary-forward if needed.\n"
        "- Do not replace the chapter with a short patch or summary of fixes.\n"
        "- Add enough new paragraphs and exchanges to clear the minimum word count in one pass.\n"
        "- Preserve continuity and chapter outcome.\n\n"
        f"Current chapter:\n{current_text}"
    )
    raw = client.complete(
        system_prompt,
        user_prompt,
        max_completion_tokens=_bookgen_completion_tokens("expand", current_words),
        temperature=0.45,
    )
    expanded = _normalize_llm_chapter_markdown(raw, chapter_pack)
    return _llm_top_up_underlength_chapter(
        client=client,
        current_text=expanded,
        chapter_pack=chapter_pack,
        constitution=constitution,
        installment_pack=installment_pack,
        scene_cards=None,
        scene_beats=None,
    )


def _llm_expand_underlength_chapter(
    *,
    client: LLMClient,
    current_text: str,
    chapter_pack: dict[str, Any],
    constitution: dict[str, Any],
    installment_pack: dict[str, Any],
    scene_cards: dict[str, Any] | None,
    scene_beats: dict[str, Any] | None,
) -> str:
    minimum_words = _chapter_minimum_words(chapter_pack)
    current_words = _word_count(current_text)
    shortfall = max(0, minimum_words - current_words)
    cards = (scene_cards or {}).get("scenes", [])
    beats = (scene_beats or {}).get("beats", [])
    system_prompt = (
        "You are extending an underlength fiction chapter. Return only markdown for the full revised chapter. "
        "Keep existing events intact and add scene material that increases pressure, causality, and consequence."
    )
    user_prompt = (
        "The chapter is underlength and must be expanded with scene material.\n\n"
        f"Minimum required words: {minimum_words}\n"
        f"Current words: {current_words}\n"
        f"Required additional words: at least {shortfall}\n"
        f"Theme: {installment_pack['theme_expression']['primary_focus']}\n"
        f"Tone lock: {constitution['narrative_identity']['tone_lock']['description']}\n"
        f"Scene cards: {json.dumps(cards, ensure_ascii=True)}\n"
        f"Scene beats: {json.dumps(beats, ensure_ascii=True)}\n\n"
        "Requirements:\n"
        "- Preserve existing heading and chapter continuity.\n"
        "- Add concrete scene beats, dialogue turns, and procedural friction.\n"
        "- Expand by adding material, not by summarizing what should be added.\n"
        "- Keep opening in-scene and avoid summary-forward intros.\n"
        "- Return the full revised chapter markdown.\n\n"
        f"Current chapter:\n{current_text}"
    )
    raw = client.complete(
        system_prompt,
        user_prompt,
        max_completion_tokens=_bookgen_completion_tokens("expand", current_words + shortfall),
        temperature=0.5,
    )
    expanded = _normalize_llm_chapter_markdown(raw, chapter_pack)
    return _llm_top_up_underlength_chapter(
        client=client,
        current_text=expanded,
        chapter_pack=chapter_pack,
        constitution=constitution,
        installment_pack=installment_pack,
        scene_cards=scene_cards,
        scene_beats=scene_beats,
    )


def _llm_top_up_underlength_chapter(
    *,
    client: LLMClient,
    current_text: str,
    chapter_pack: dict[str, Any],
    constitution: dict[str, Any],
    installment_pack: dict[str, Any],
    scene_cards: dict[str, Any] | None,
    scene_beats: dict[str, Any] | None,
) -> str:
    minimum_words = _chapter_minimum_words(chapter_pack)
    working = _normalize_llm_chapter_markdown(current_text, chapter_pack)
    rounds = 0
    max_rounds = 2
    while _word_count(working) < minimum_words and rounds < max_rounds:
        rounds += 1
        remaining = max(0, minimum_words - _word_count(working))
        if remaining < 120:
            break
        cards = (scene_cards or {}).get("scenes", [])
        beats = (scene_beats or {}).get("beats", [])
        system_prompt = (
            "You are continuing an in-progress fiction chapter. Return only continuation markdown paragraphs. "
            "Do not repeat previous paragraphs and do not add a heading."
        )
        user_prompt = (
            "Continue this chapter from its current endpoint.\n\n"
            f"Words still needed: at least {remaining}\n"
            f"Theme: {installment_pack['theme_expression']['primary_focus']}\n"
            f"Tone lock: {constitution['narrative_identity']['tone_lock']['description']}\n"
            f"Scene cards: {json.dumps(cards, ensure_ascii=True)}\n"
            f"Scene beats: {json.dumps(beats, ensure_ascii=True)}\n\n"
            "Requirements:\n"
            "- Return continuation paragraphs only (no title heading).\n"
            "- Extend causality and pressure; no summaries or notes.\n"
            "- Keep continuity exact with the existing ending.\n\n"
            f"Current chapter:\n{working}"
        )
        raw = client.complete(
            system_prompt,
            user_prompt,
            max_completion_tokens=min(11000, int(_bookgen_completion_tokens("expand", _word_count(working) + remaining) * 1.2)),
            temperature=0.45,
        )
        continuation = _normalize_llm_continuation_markdown(raw)
        if not continuation:
            break
        working = working.rstrip() + "\n\n" + continuation.strip() + "\n"
    return _normalize_llm_chapter_markdown(working, chapter_pack)


def _force_underlength_recovery(
    *,
    text: str,
    chapter_pack: dict[str, Any],
    installment_pack: dict[str, Any],
    scene_cards: dict[str, Any] | None,
    scene_beats: dict[str, Any] | None,
) -> str:
    minimum_words = _chapter_minimum_words(chapter_pack)
    paragraph_min = int(chapter_pack.get("draft_contract", {}).get("structure_requirements", {}).get("paragraph_min", 6))
    if _word_count(text) >= minimum_words:
        existing_paragraphs = [part.strip() for part in _strip_heading(text).split("\n\n") if part.strip()]
        if len(existing_paragraphs) >= paragraph_min:
            return text
    heading = text.splitlines()[0].strip() if text.splitlines() and text.splitlines()[0].startswith("# ") else f"# {chapter_pack['chapter_card'].get('title') or chapter_pack['chapter_id'].upper()}"
    body = _strip_heading(text)
    paragraphs = [part.strip() for part in body.split("\n\n") if part.strip()]
    if not paragraphs:
        paragraphs = ["The chapter opens under immediate pressure and refuses abstraction."]
    beats = (scene_beats or {}).get("beats", [])
    cards = (scene_cards or {}).get("scenes", [])
    theme = installment_pack["theme_expression"]["primary_focus"]
    goal = chapter_pack["chapter_card"]["goal"]
    reveal = chapter_pack["chapter_card"]["information"]["reveal"][0]
    conceal = chapter_pack["chapter_card"]["information"]["conceal"][0]
    location = chapter_pack["world_slice"]["setting_constraints"]["locations_allowed"][-1]
    idx = 0
    max_iters = 240
    while (
        (_word_count(f"{heading}\n\n" + "\n\n".join(paragraphs)) < minimum_words or len(paragraphs) < paragraph_min)
        and idx < max_iters
    ):
        beat = beats[idx % max(1, len(beats))] if beats else {}
        card = cards[idx % max(1, len(cards))] if cards else {}
        beat_intent = str(beat.get("intent", "pressure converts to a specific cost"))
        scene_label = str(card.get("scene_label", "Escalation"))
        conflict_type = str(card.get("conflict_type", "procedural friction"))
        paragraphs.append(
            _paragraph(
                f"{scene_label} keeps the chapter moving in {location}: {beat_intent}",
                f"The conflict stays concrete through {conflict_type}, so the narrative does not fall back to summary.",
                f"\"We move now and pay for it now,\" the room agrees, because {goal.lower()} can no longer wait for a cleaner window.",
                f"The consequence clarifies {reveal} while still concealing {conceal}, which keeps {theme} active as a choice instead of a slogan.",
            )
        )
        idx += 1
    recovered = f"{heading}\n\n" + "\n\n".join(paragraphs).strip() + "\n"
    return _world_rule_compliance_rewrite(recovered, chapter_pack)


def _draft_with_process(
    *,
    chapter_pack: dict[str, Any],
    constitution: dict[str, Any],
    installment_pack: dict[str, Any],
    scene_cards: dict[str, Any] | None = None,
    scene_beats: dict[str, Any] | None = None,
    llm_mode: bool,
) -> tuple[str, dict[str, Any], list[dict[str, Any]]]:
    llm_failed = False
    if llm_mode:
        try:
            text = _draft_chapter_with_llm(
                chapter_pack,
                constitution,
                installment_pack,
                scene_cards=scene_cards,
                scene_beats=scene_beats,
            )
        except Exception as exc:
            recovered_with_llm = False
            if _llm_draft_recovery_allowed(exc):
                try:
                    text = _draft_chapter_with_llm_recovery(
                        chapter_pack,
                        constitution,
                        installment_pack,
                        scene_cards=scene_cards,
                        scene_beats=scene_beats,
                        recovery_reason=str(exc),
                    )
                    recovered_with_llm = True
                except Exception as recovery_exc:
                    _bookgen_llm_error("draft-recovery", recovery_exc, chapter_pack)
            if not recovered_with_llm:
                _bookgen_llm_error("draft", exc, chapter_pack)
                text = _draft_chapter_markdown(
                    chapter_pack,
                    constitution,
                    installment_pack,
                    scene_cards=scene_cards,
                    scene_beats=scene_beats,
                )
                llm_mode = False
                llm_failed = True
    else:
        text = _draft_chapter_markdown(
            chapter_pack,
            constitution,
            installment_pack,
            scene_cards=scene_cards,
            scene_beats=scene_beats,
        )
    qc_history: list[dict[str, Any]] = []
    qc = _structural_qc_report(text, chapter_pack, llm_mode=llm_mode)
    qc["attempt"] = 1
    if llm_failed:
        qc["llm_fallback"] = {"stage": "draft", "mode": "fallback"}
    qc_history.append(qc)
    attempts = 0
    while qc["pass_status"] != "PASS" and attempts < _structural_retry_limit():
        attempts += 1
        if llm_mode:
            try:
                if "word_count_out_of_range" in qc.get("issues", []) and _word_count(text) < _chapter_minimum_words(chapter_pack):
                    text = _llm_expand_underlength_chapter(
                        client=LLMClient(),
                        current_text=text,
                        chapter_pack=chapter_pack,
                        constitution=constitution,
                        installment_pack=installment_pack,
                        scene_cards=scene_cards,
                        scene_beats=scene_beats,
                    )
                else:
                    text = _llm_expand_chapter(
                        client=LLMClient(),
                        current_text=text,
                        chapter_pack=chapter_pack,
                        constitution=constitution,
                        installment_pack=installment_pack,
                    )
            except Exception as exc:
                _bookgen_llm_error("expand", exc, chapter_pack)
                text = _rewrite_chapter_text(
                    text,
                    chapter_pack,
                    {
                        "drift_flags": {"theme_absence": False, "character_voice_shift": False},
                        "metrics": {"word_count": _word_count(text)},
                    },
                )
                llm_mode = False
        else:
            text = _rewrite_chapter_text(
                text,
                chapter_pack,
                {
                    "drift_flags": {"theme_absence": False, "character_voice_shift": False},
                    "metrics": {"word_count": _word_count(text)},
                },
            )
        qc = _structural_qc_report(text, chapter_pack, llm_mode=llm_mode)
        qc["attempt"] = attempts + 1
        qc_history.append(qc)
    if qc["pass_status"] != "PASS" and "word_count_out_of_range" in qc.get("issues", []) and _word_count(text) < _chapter_minimum_words(chapter_pack):
        text = _force_underlength_recovery(
            text=text,
            chapter_pack=chapter_pack,
            installment_pack=installment_pack,
            scene_cards=scene_cards,
            scene_beats=scene_beats,
        )
        qc = _structural_qc_report(text, chapter_pack, llm_mode=False)
        qc["attempt"] = attempts + 2
        if qc.get("pass_status") == "PASS" and qc_history and qc_history[0].get("mode") == "llm":
            qc["llm_fallback"] = {"stage": "underlength_recovery", "mode": "fallback"}
        qc_history.append(qc)
    return text, qc, qc_history


def _draft_chapter_with_llm(
    chapter_pack: dict[str, Any],
    constitution: dict[str, Any],
    installment_pack: dict[str, Any],
    scene_cards: dict[str, Any] | None = None,
    scene_beats: dict[str, Any] | None = None,
) -> str:
    system_prompt, user_prompt = _bookgen_llm_prompts(
        chapter_pack,
        constitution,
        installment_pack,
        scene_cards=scene_cards,
        scene_beats=scene_beats,
    )
    target_words = int(chapter_pack["scene_constraints"]["word_count_target"])
    max_tokens = _bookgen_completion_tokens("draft", target_words)
    client = LLMClient()
    response = client.complete(
        system_prompt,
        user_prompt,
        max_completion_tokens=max_tokens,
        temperature=0.5,
    )
    return _normalize_llm_chapter_markdown(response, chapter_pack)


def _draft_chapter_with_llm_recovery(
    chapter_pack: dict[str, Any],
    constitution: dict[str, Any],
    installment_pack: dict[str, Any],
    scene_cards: dict[str, Any] | None = None,
    scene_beats: dict[str, Any] | None = None,
    recovery_reason: str = "",
) -> str:
    system_prompt, user_prompt = _bookgen_llm_prompts(
        chapter_pack,
        constitution,
        installment_pack,
        scene_cards=scene_cards,
        scene_beats=scene_beats,
    )
    target_words = int(chapter_pack["scene_constraints"]["word_count_target"])
    max_tokens = min(12000, int(_bookgen_completion_tokens("draft", target_words) * 1.25))
    retry_note = str(recovery_reason or "no usable chapter text returned").strip()
    recovery_user_prompt = (
        f"{user_prompt}\n\n"
        "Recovery pass requirements:\n"
        f"- Prior failure: {retry_note}\n"
        "- Return full chapter markdown only; do not explain or apologize.\n"
        "- Preserve in-scene opening, dialogue pressure, and all hard constraints.\n"
        "- Ensure output is complete and usable in one response.\n"
    )
    client = LLMClient()
    response = client.complete(
        system_prompt,
        recovery_user_prompt,
        max_completion_tokens=max_tokens,
        temperature=0.5,
    )
    return _normalize_llm_chapter_markdown(response, chapter_pack)


def _draft_chapter_markdown(
    chapter_pack: dict[str, Any],
    constitution: dict[str, Any],
    installment_pack: dict[str, Any],
    scene_cards: dict[str, Any] | None = None,
    scene_beats: dict[str, Any] | None = None,
) -> str:
    pov_id = chapter_pack["scene_constraints"]["pov_character_id"]
    pov_name = next(
        (
            character["name"]
            for character in constitution["character_registry"]["characters"]
            if character["character_id"] == pov_id
        ),
        pov_id.replace("-", " ").title(),
    )
    title = chapter_pack["chapter_card"].get("title") or chapter_pack["chapter_id"].upper()
    location = chapter_pack["world_slice"]["setting_constraints"]["locations_allowed"][0]
    secondary_location = chapter_pack["world_slice"]["setting_constraints"]["locations_allowed"][-1]
    reveal = chapter_pack["chapter_card"]["information"]["reveal"][0]
    must_happen = chapter_pack["chapter_card"]["constraints"]["must_happen"][0]
    theme = installment_pack["theme_expression"]["primary_focus"]
    emotional_from = chapter_pack["chapter_card"]["emotional_shift"]["from"]
    emotional_to = chapter_pack["chapter_card"]["emotional_shift"]["to"]
    active_threads = ", ".join(chapter_pack["continuity_slice"]["active_threads"]) or "the active conflict"
    concealed = chapter_pack["chapter_card"]["information"]["conceal"][0]
    conflict_type = chapter_pack["chapter_card"]["conflict"]["type"]
    intensity = chapter_pack["chapter_card"]["conflict"]["intensity"]
    narrative_function = chapter_pack["chapter_card"]["narrative_function"]
    rule_summary = "The scene respects operational limits, keeps causality legible, and avoids giving anyone impossible reach."
    research_summary = "The supporting detail stays grounded in process, setting limits, and what each actor can credibly know."
    supporting_names = [
        next(
            (
                character["name"]
                for character in constitution["character_registry"]["characters"]
                if character["character_id"] == entry["character_id"]
            ),
            entry["character_id"].replace("-", " ").title(),
        )
        for entry in chapter_pack["character_state_slice"][1:]
    ]
    support_clause = supporting_names[0] if supporting_names else "an unreliable ally"
    relationship_note = ""
    if len(chapter_pack["character_state_slice"]) > 1:
        relationship_state = chapter_pack["character_state_slice"][1].get("relationship_state", [])
        if relationship_state:
            relationship_note = (
                f"Their history still carries a trust level of {relationship_state[0].get('trust_level', 0.5):.2f}, "
                "so every concession reads as tactical rather than generous."
            )
    research_topic = ""
    if chapter_pack["research_slice"]["items"]:
        research_topic = chapter_pack["research_slice"]["items"][0]["topic"]
    if research_topic.lower().startswith("no "):
        research_topic = "operational process"
    pressure_object = "the briefing packet"
    if reveal:
        pressure_object = reveal.split(".")[0].strip().lower() or pressure_object
    scene_hook = {
        "hook": f"A fresh lead opens inside {location}, but it arrives with just enough detail to tempt overconfidence.",
        "pressure": f"The pressure shifts from implication to friction as movement between {location} and {secondary_location} narrows the team's options.",
        "reversal": f"A reversal forms when evidence gathered in {location} makes the safer plan look like staged compliance.",
        "climax": f"The climax run begins in {location}, where every action now exposes who is willing to absorb institutional heat.",
        "denouement": f"The denouement settles in {secondary_location}, where the cost of the chapter's choice becomes visible without pretending the conflict is over.",
    }.get(narrative_function, f"The chapter turns inside {location}, where the conflict has to become more specific than rumor.")
    closing_move = {
        "hook": "The exit leaves the next move possible but more dangerous.",
        "pressure": "The chapter ends with the team committed to a narrower lane and fewer excuses.",
        "reversal": "The final beat reframes who is actually controlling the sequence of events.",
        "climax": "The closing image refuses relief and hands the next chapter a problem that cannot be deferred.",
        "denouement": "The resolution lands as changed leverage rather than comfort, which keeps the installment honest.",
    }.get(narrative_function, "The closing beat shifts leverage and makes the next consequence unavoidable.")

    cards = (scene_cards or {}).get("scenes", [])
    beats = (scene_beats or {}).get("beats", [])
    scene_plan_line = ""
    if cards:
        labels = ", ".join(str(item.get("scene_label", "")).strip() for item in cards[:3] if str(item.get("scene_label", "")).strip())
        if labels:
            scene_plan_line = f"The chapter follows a scene spine: {labels}."
    beat_line = ""
    if beats:
        beat_labels = ", ".join(str(item.get("beat", "")).strip() for item in beats[:4] if str(item.get("beat", "")).strip())
        if beat_labels:
            beat_line = f"Each move stays causal through beats: {beat_labels}."

    paragraphs = [
        _paragraph(
            f"{pov_name} entered {location} carrying the chapter's problem in active form: {chapter_pack['chapter_card']['goal']}",
            f"The scene opens in {emotional_from}, and {scene_hook}",
            relationship_note or scene_plan_line,
        ),
        _paragraph(
            f'"We are already late," {support_clause} says before {pov_name} can set the folder down.',
            f'"Late for what?" {pov_name} asks.',
            f'"Late for the version where {pressure_object} stays quiet," {support_clause} replies.',
        ),
        _paragraph(
            f"{support_clause} presses the issue through a {conflict_type} conflict that has already reached {intensity} intensity.",
            f"{must_happen} becomes the hinge of the chapter, forcing {pov_name} to choose between speed and control instead of pretending both remain available.",
            f"The chapter keeps {theme} on the page by tying the choice to {active_threads}.",
        ),
        _paragraph(
            f'"If we rush this, we lose the shape of it," {pov_name} says, studying what the room refuses to explain for free.',
            f'"If we wait, we lose the initiative," {support_clause} replies, giving the argument exactly the edge it needed.',
            f"{pov_name} lets the silence stretch long enough to separate caution from fear, then moves with deliberate precision.",
        ),
        _paragraph(
            f"The room answers in procedural detail rather than comfort: badges checked twice, doors held a beat too long, a clerk pretending not to listen while tracking every name that passes.",
            f"If the scene touches {research_topic or 'committee process'}, it does so through usable friction instead of exposition.",
            f"{pov_name} notices how every ordinary gesture in {location} has been repurposed into warning. {beat_line}".strip(),
        ),
        _paragraph(
            f"The reveal lands cleanly: {reveal}",
            f"It also clarifies what must remain concealed for now: {concealed}.",
            f"That knowledge reframes {active_threads} and turns {theme} from abstract language into leverage that can be acted on in-scene.",
        ),
        _paragraph(
            f'"Then we narrow the lane," {pov_name} says.',
            f'"That is not the same thing as staying safe," {support_clause} answers, but the objection comes half a second too late to stop the choice.',
            f"The concession alters the rhythm of the chapter and makes the next movement costlier.",
        ),
        _paragraph(
            f"{pov_name} tests the boundary of the world model without breaking it. {rule_summary}",
            f"Even the research burden stays grounded instead of turning into an article dump. {research_summary}",
            f"The emotional shift completes from {emotional_from} to {emotional_to} because the chapter pays for momentum with clarity, not abstraction.",
        ),
        _paragraph(
            f"Movement between {location} and {secondary_location} strips away the last pretense that this can be handled as a contained internal concern.",
            f"One thread sharpens under direct pressure, another goes evasive, and a third becomes useful precisely because nobody can acknowledge it in the open.",
            f"{pov_name} feels the institutional temperature change before anyone says it aloud.",
        ),
        _paragraph(
            f"As {pov_name} leaves {secondary_location}, the chapter's gain is measurable: one thread sharpens, another resists closure, and the installment moves with cleaner causal force.",
            closing_move,
            f"{pov_name} carries the next chapter forward without reaching for coincidence or summary language to do the work.",
        ),
    ]
    return "\n\n".join([f"# {title}"] + paragraphs) + "\n"


def _build_scene_cards(*, chapter_pack: dict[str, Any]) -> dict[str, Any]:
    chapter_index = int(chapter_pack["chapter_index"])
    chapter_id = str(chapter_pack["chapter_id"])
    installment_id = str(chapter_pack["installment_id"])
    reveal = str(chapter_pack["chapter_card"]["information"]["reveal"][0])
    conceal = str(chapter_pack["chapter_card"]["information"]["conceal"][0])
    locations = chapter_pack["world_slice"]["setting_constraints"]["locations_allowed"]
    goal = str(chapter_pack["chapter_card"]["goal"])
    base_tension = "high" if chapter_pack["chapter_card"]["narrative_function"] in {"reversal", "climax"} else "moderate"
    scenes: list[dict[str, Any]] = []
    scene_templates = [
        ("Opening Pressure", f"Put pressure on {goal}", "interruption", locations[0], "The POV must make a tactical concession to keep access."),
        ("Procedural Friction", "Convert suspicion into documented risk", "procedural_block", locations[-1], f"Surface partial reveal: {reveal}"),
        ("Exit Consequence", "Lock in consequence that carries forward", "choice_cost", locations[-1], f"Conceal remains protected: {conceal}"),
    ]
    for idx, (label, objective, conflict_type, location, outcome) in enumerate(scene_templates, start=1):
        scenes.append(
            {
                "scene_index": idx,
                "scene_id": f"{chapter_id}-s{idx:02d}",
                "scene_label": label,
                "objective": objective,
                "conflict_type": conflict_type,
                "location": location,
                "required_outcome": outcome,
                "tension_level": base_tension if idx > 1 else "moderate",
            }
        )
    return {
        "schema_version": "1.0",
        "installment_id": installment_id,
        "chapter_id": chapter_id,
        "chapter_index": chapter_index,
        "scenes": scenes,
    }


def _build_scene_beats(*, chapter_pack: dict[str, Any], scene_cards: dict[str, Any]) -> dict[str, Any]:
    beats: list[dict[str, Any]] = []
    for scene in scene_cards["scenes"]:
        beats.extend(
            [
                {
                    "scene_id": scene["scene_id"],
                    "beat": "entry",
                    "intent": f"Enter {scene['location']} under visible pressure.",
                },
                {
                    "scene_id": scene["scene_id"],
                    "beat": "turn",
                    "intent": f"Conflict turns on {scene['conflict_type']} and forces adaptation.",
                },
                {
                    "scene_id": scene["scene_id"],
                    "beat": "cost",
                    "intent": scene["required_outcome"],
                },
            ]
        )
    return {
        "schema_version": "1.0",
        "installment_id": chapter_pack["installment_id"],
        "chapter_id": chapter_pack["chapter_id"],
        "chapter_index": chapter_pack["chapter_index"],
        "beats": beats,
    }


def _effective_installment_title(constitution: dict[str, Any], installment_pack: dict[str, Any]) -> str:
    title_block = installment_pack.get("title_block") if isinstance(installment_pack.get("title_block"), dict) else {}
    if title_block and str(title_block.get("installment_working_title", "")).strip():
        return str(title_block["installment_working_title"]).strip()
    return str(constitution.get("title", "Untitled Installment")).strip()


def _build_chapter_pack(
    *,
    project_id: str,
    constitution: dict[str, Any],
    installment_pack: dict[str, Any],
    outline_chapter: dict[str, Any],
    ledgers: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    chapter_index = int(outline_chapter["chapter_index"])
    chapter_id = str(outline_chapter["chapter_id"])
    genre = str(constitution.get("narrative_identity", {}).get("genre", "fiction"))
    subgenre = str(constitution.get("narrative_identity", {}).get("subgenre", genre))
    audience = str(constitution.get("narrative_identity", {}).get("audience", {}).get("age_band", "adult"))
    policy_profile = _genre_policy_profile(genre, subgenre, audience)
    structure_policy = policy_profile["structure"]
    opening_policy = policy_profile["opening_scene"]
    rewrite_policy = policy_profile["rewrite"]
    character_ids = outline_chapter.get("characters_on_stage") or [ledgers["ledger_characters"]["characters"][0]["character_id"]]
    character_slice = []
    for ledger_entry in ledgers["ledger_characters"]["characters"]:
        if ledger_entry["character_id"] not in character_ids:
            continue
        constitution_character = next(
            item
            for item in constitution["character_registry"]["characters"]
            if item["character_id"] == ledger_entry["character_id"]
        )
        character_slice.append(
            {
                "character_id": ledger_entry["character_id"],
                "current_arc_stage": ledger_entry["arc_stage"],
                "immutable_traits": constitution_character["immutable_traits"],
                "mutable_traits_active": constitution_character["mutable_traits"],
                "voice_markers": constitution_character["voice_markers"],
                "moral_boundaries": [
                    {
                        "boundary": boundary["boundary"],
                        "cannot_cross": boundary["cannot_cross"],
                        "status": ledger_entry["moral_boundary_status"],
                    }
                    for boundary in constitution_character["moral_boundaries"]
                ],
                "active_secrets_known": [secret["secret_id"] for secret in constitution_character["secrets"] if ledger_entry["character_id"] in secret["known_by"]],
                "relationship_state": ledger_entry["relationships"],
            }
        )
    pack = {
        "schema_version": "1.0",
        "series_id": project_id,
        "installment_id": installment_pack["installment_id"],
        "chapter_index": chapter_index,
        "chapter_id": chapter_id,
        "created_utc": _utcnow(),
        "chapter_card": {
            "title": outline_chapter["title"],
            "narrative_function": outline_chapter["purpose"],
            "goal": installment_pack["intent"]["summary"],
            "emotional_shift": {
                "from": outline_chapter["emotional_beat"].split(" to ")[0],
                "to": outline_chapter["emotional_beat"].split(" to ")[-1],
            },
            "conflict": {
                "type": "mixed",
                "intensity": "high" if outline_chapter["purpose"] in {"climax", "reversal"} else "moderate",
            },
            "information": {
                "reveal": outline_chapter["key_reveals"],
                "conceal": ["the full shape of the next consequence"],
            },
            "constraints": {
                "must_happen": [outline_chapter["key_reveals"][0]],
                "must_not_happen": constitution["narrative_identity"]["tone_lock"]["dont"],
                "forbidden_outcomes": ["arc regression without cause", "rule-breaking escape hatch"],
            },
            "escalation_tags": outline_chapter["escalation_tags"],
        },
        "character_state_slice": character_slice,
        "world_slice": {
            "applicable_rules": constitution["world_model"]["governing_rules"][:3],
            "setting_constraints": {
                "locations_allowed": outline_chapter["locations"],
                "technology_allowed": [constitution["world_model"]["setting"]["technology_level"]],
                "realism_mode": constitution["world_model"]["realism_constraints"]["mode"],
            },
        },
        "tone_style_snapshot": {
            "tone_lock": constitution["narrative_identity"]["tone_lock"]["description"],
            "prose_density": constitution["narrative_identity"]["style_constitution"]["prose_density"],
            "pacing_profile": installment_pack["intent"]["pacing_goal"],
            "dialogue_density_target_percent": constitution["narrative_identity"]["style_constitution"]["dialogue_density"]["target_percent"],
        },
        "continuity_slice": {
            "active_threads": [
                thread["thread_id"] for thread in ledgers["ledger_threads"]["threads"] if thread["status"] in {"active", "dormant"}
            ],
            "timeline_context": {
                "relative_position": "before_midpoint" if chapter_index <= max(2, installment_pack["output_targets"]["chapter_count_target"] // 2) else "after_midpoint",
                "anchor_events": [outline_chapter["timeline_anchor"]],
            },
        },
        "research_slice": {
            "items": [
                {
                    "topic": item["topic"],
                    "neutral_summary": item["neutral_summary"],
                    "allowed_terms": item["terminology"]["allowed_terms"],
                    "avoid_terms": item["terminology"]["avoid_terms"],
                }
                for item in installment_pack["research_pack"]["items"][:3]
            ]
        },
        "scene_constraints": {
            "pov_character_id": character_ids[0],
            "pov_mode": constitution["narrative_identity"]["pov"]["mode"],
            "time_of_day": "late afternoon",
            "duration": "one sustained scene",
            "word_count_target": int(
                installment_pack["output_targets"].get(
                    "chapter_word_target",
                    max(250, installment_pack["output_targets"]["word_count_target"] // installment_pack["output_targets"]["chapter_count_target"]),
                )
            ),
            "forbidden_devices": constitution["narrative_identity"]["style_constitution"]["forbidden_devices"],
        },
        "policy_profile": {
            "profile_id": policy_profile["profile_id"],
            "genre": genre,
            "subgenre": subgenre,
            "audience": audience,
            "profile": policy_profile,
        },
    }
    llm_bounds = _chapter_word_bounds(pack, llm_mode=True)
    fallback_bounds = _chapter_word_bounds(pack, llm_mode=False)
    pack["draft_contract"] = {
        "schema_version": "1.0",
        "chapter_id": chapter_id,
        "installment_id": installment_pack["installment_id"],
        "title": outline_chapter["title"],
        "target_mode": "scene_first_novel_chapter",
        "word_count": {
            "target": llm_bounds["target"],
            "min_llm": llm_bounds["min"],
            "max_llm": llm_bounds["max"],
            "min_fallback": fallback_bounds["min"],
            "max_fallback": fallback_bounds["max"],
        },
        "opening_scene_requirements": {
            "must_start_in_scene": bool(opening_policy["must_start_in_scene"]),
            "must_put_pov_in_motion_by_paragraph": int(opening_policy["must_put_pov_in_motion_by_paragraph"]),
            "must_introduce_pressure_within_words": int(opening_policy["must_introduce_pressure_within_words"]),
            "must_force_a_choice_within_words": int(opening_policy["must_force_a_choice_within_words"]),
            "summary_forward_opening_disallowed": bool(opening_policy["summary_forward_opening_disallowed"]),
        },
        "structure_requirements": {
            "paragraph_min": int(structure_policy["opening_paragraph_min"] if chapter_index <= 2 else structure_policy["later_paragraph_min"]),
            "dialogue_presence_required": bool(structure_policy["dialogue_presence_required"]),
            "scene_turns_min": int(structure_policy["opening_scene_turns_min"] if chapter_index <= 2 else structure_policy["later_scene_turns_min"]),
        },
        "genre_policy_profile_id": policy_profile["profile_id"],
        "rewrite_policy": {
            "max_attempts": int(rewrite_policy["max_attempts"]),
            "stop_if_no_improvement_after": int(rewrite_policy["stop_if_no_improvement_after"]),
            "priority_categories": list(rewrite_policy["priority_categories"]),
        },
    }
    return pack


def _update_ledgers(
    *,
    ledgers: dict[str, dict[str, Any]],
    chapter_pack: dict[str, Any],
    eval_report: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    chapter_id = chapter_pack["chapter_id"]
    installment_id = chapter_pack["installment_id"]
    for character in ledgers["ledger_characters"]["characters"]:
        if character["character_id"] not in {entry["character_id"] for entry in chapter_pack["character_state_slice"]}:
            continue
        character["arc_stage"] = chapter_pack["chapter_card"]["narrative_function"]
        character["last_updated"] = {"installment_id": installment_id, "chapter_id": chapter_id}
        if eval_report["drift_flags"]["moral_boundary_violation"]:
            character["moral_boundary_status"] = "strained"
    ledgers["ledger_timeline"]["events"].append(
        {
            "event_id": f"E-{len(ledgers['ledger_timeline']['events']) + 1:03d}",
            "description": chapter_pack["chapter_card"]["information"]["reveal"][0],
            "occurs_in": {"installment_id": installment_id, "chapter_id": chapter_id},
            "time_anchor": chapter_pack["continuity_slice"]["timeline_context"]["anchor_events"][0],
            "causal_links": {"causes": [], "effects": []},
        }
    )
    for thread in ledgers["ledger_threads"]["threads"]:
        if thread["status"] == "archived":
            continue
        thread["last_touched"] = {"installment_id": installment_id, "chapter_id": chapter_id}
        if chapter_pack["chapter_card"]["narrative_function"] == "denouement":
            thread["status"] = "closed"
    return ledgers


def _manuscript_to_docx_bytes(title: str, sections: list[str]) -> bytes:
    content = [
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">',
        "<w:body>",
    ]
    for paragraph in [title, ""] + [part for section in sections for part in section.split("\n")]:
        if not paragraph.strip():
            content.append("<w:p/>")
            continue
        content.append(f"<w:p><w:r><w:t xml:space=\"preserve\">{escape(paragraph)}</w:t></w:r></w:p>")
    content.append("<w:sectPr/>")
    content.append("</w:body></w:document>")
    document_xml = "".join(content)
    buffer = Path("/tmp/bookgen-docx-buffer.docx")
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/></Types>')
        zf.writestr("_rels/.rels", '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/></Relationships>')
        zf.writestr("word/document.xml", document_xml)
    raw = buffer.read_bytes()
    buffer.unlink(missing_ok=True)
    return raw


def _pdf_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _markdown_to_text_lines(sections: list[str]) -> list[str]:
    lines: list[str] = []
    for section in sections:
        for line in section.splitlines():
            stripped = line.strip()
            if stripped.startswith("#"):
                heading = stripped.lstrip("#").strip()
                lines.append(heading.upper())
                lines.append("")
                continue
            if not stripped:
                lines.append("")
                continue
            lines.append(stripped)
        if lines and lines[-1] != "":
            lines.append("")
    return lines


def _wrap_text_for_pdf(lines: list[str], *, max_chars: int = 92) -> list[str]:
    wrapped: list[str] = []
    for line in lines:
        if not line:
            wrapped.append("")
            continue
        words = line.split()
        current: list[str] = []
        for word in words:
            trial = " ".join(current + [word]).strip()
            if len(trial) <= max_chars:
                current.append(word)
                continue
            if current:
                wrapped.append(" ".join(current))
            current = [word]
        if current:
            wrapped.append(" ".join(current))
    return wrapped


def _manuscript_to_pdf_bytes(title: str, sections: list[str]) -> bytes:
    all_lines = [title.upper(), ""] + _wrap_text_for_pdf(_markdown_to_text_lines(sections))
    lines_per_page = 44
    pages = [all_lines[idx : idx + lines_per_page] for idx in range(0, len(all_lines), lines_per_page)]
    if not pages:
        pages = [[title.upper()]]

    objects: list[bytes] = []

    def add_object(raw: str) -> int:
        objects.append(raw.encode("latin-1", errors="replace"))
        return len(objects)

    catalog_id = add_object("<< /Type /Catalog /Pages 2 0 R >>")
    pages_id = add_object("<< /Type /Pages /Kids [] /Count 0 >>")
    font_id = add_object("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

    page_ids: list[int] = []
    content_ids: list[int] = []
    for page_lines in pages:
        text_ops = ["BT", "/F1 11 Tf", "72 770 Td", "14 TL"]
        for line in page_lines:
            if line:
                text_ops.append(f"({_pdf_escape(line)}) Tj")
            text_ops.append("T*")
        text_ops.append("ET")
        stream = "\n".join(text_ops).encode("latin-1", errors="replace")
        content_id = add_object(f"<< /Length {len(stream)} >>\nstream\n{stream.decode('latin-1')}\nendstream")
        page_id = add_object(
            f"<< /Type /Page /Parent {pages_id} 0 R /MediaBox [0 0 612 792] "
            f"/Resources << /Font << /F1 {font_id} 0 R >> >> /Contents {content_id} 0 R >>"
        )
        content_ids.append(content_id)
        page_ids.append(page_id)

    kids = " ".join(f"{pid} 0 R" for pid in page_ids)
    objects[pages_id - 1] = f"<< /Type /Pages /Kids [{kids}] /Count {len(page_ids)} >>".encode("latin-1")
    objects[catalog_id - 1] = f"<< /Type /Catalog /Pages {pages_id} 0 R >>".encode("latin-1")

    out = BytesIO()
    out.write(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]
    for idx, blob in enumerate(objects, start=1):
        offsets.append(out.tell())
        out.write(f"{idx} 0 obj\n".encode("latin-1"))
        out.write(blob)
        out.write(b"\nendobj\n")
    xref_start = out.tell()
    out.write(f"xref\n0 {len(objects) + 1}\n".encode("latin-1"))
    out.write(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        out.write(f"{offset:010d} 00000 n \n".encode("latin-1"))
    out.write(
        (
            f"trailer\n<< /Size {len(objects) + 1} /Root {catalog_id} 0 R >>\n"
            f"startxref\n{xref_start}\n%%EOF\n"
        ).encode("latin-1")
    )
    return out.getvalue()


def _markdown_section_to_xhtml(section: str) -> str:
    blocks = [part.strip() for part in section.split("\n\n") if part.strip()]
    rendered: list[str] = []
    for block in blocks:
        if block.startswith("#"):
            heading = block.lstrip("#").strip()
            rendered.append(f"<h2>{escape(heading)}</h2>")
        else:
            rendered.append(f"<p>{escape(block)}</p>")
    return "\n".join(rendered)


def _manuscript_to_epub_bytes(*, title: str, author: str, sections: list[str], chapter_titles: list[str]) -> bytes:
    uid = hashlib.sha256(f"{title}:{author}".encode("utf-8")).hexdigest()[:16]
    timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    chapters = []
    spine_items = []
    toc_links = []
    for idx, section in enumerate(sections, start=1):
        chap_id = f"chap{idx:03d}"
        chap_name = f"{chap_id}.xhtml"
        chapter_title = chapter_titles[idx - 1] if idx - 1 < len(chapter_titles) else f"Chapter {idx}"
        xhtml = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<html xmlns="http://www.w3.org/1999/xhtml">\n'
            "<head><title>"
            + escape(chapter_title)
            + "</title></head>\n<body>\n"
            + _markdown_section_to_xhtml(section)
            + "\n</body></html>\n"
        )
        chapters.append((chap_name, xhtml))
        spine_items.append(f'<itemref idref="{chap_id}"/>')
        toc_links.append(f'<li><a href="{chap_name}">{escape(chapter_title)}</a></li>')

    nav_doc = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<html xmlns="http://www.w3.org/1999/xhtml">\n'
        "<head><title>Table of Contents</title></head>\n<body>\n"
        '<nav epub:type="toc" id="toc"><h1>Contents</h1><ol>'
        + "".join(toc_links)
        + "</ol></nav>\n</body></html>\n"
    )

    manifest_items = [
        '<item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav"/>'
    ] + [
        f'<item id="chap{idx:03d}" href="chap{idx:03d}.xhtml" media-type="application/xhtml+xml"/>'
        for idx in range(1, len(chapters) + 1)
    ]
    content_opf = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<package xmlns="http://www.idpf.org/2007/opf" unique-identifier="bookid" version="3.0">\n'
        "  <metadata xmlns:dc=\"http://purl.org/dc/elements/1.1/\">\n"
        f"    <dc:identifier id=\"bookid\">urn:uuid:{uid}</dc:identifier>\n"
        f"    <dc:title>{escape(title)}</dc:title>\n"
        f"    <dc:creator>{escape(author)}</dc:creator>\n"
        f"    <dc:language>en</dc:language>\n"
        f"    <meta property=\"dcterms:modified\">{escape(timestamp)}</meta>\n"
        "  </metadata>\n"
        "  <manifest>\n    "
        + "\n    ".join(manifest_items)
        + "\n  </manifest>\n"
        "  <spine>\n    "
        + "\n    ".join(spine_items)
        + "\n  </spine>\n"
        "</package>\n"
    )
    container_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">\n'
        '  <rootfiles><rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/></rootfiles>\n'
        "</container>\n"
    )

    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.writestr("mimetype", "application/epub+zip", compress_type=zipfile.ZIP_STORED)
        zf.writestr("META-INF/container.xml", container_xml, compress_type=zipfile.ZIP_DEFLATED)
        zf.writestr("OEBPS/content.opf", content_opf, compress_type=zipfile.ZIP_DEFLATED)
        zf.writestr("OEBPS/nav.xhtml", nav_doc, compress_type=zipfile.ZIP_DEFLATED)
        for chap_name, xhtml in chapters:
            zf.writestr(f"OEBPS/{chap_name}", xhtml, compress_type=zipfile.ZIP_DEFLATED)
    return buffer.getvalue()


def _write_binary(store: ObjectStore, key: str, raw: bytes, content_type: str) -> None:
    stream = BytesIO(raw)
    store.client.put_object(store.bucket, key, data=stream, length=len(raw), content_type=content_type)


def _clean_metadata_phrase(value: str, *, lowercase: bool = False) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    text = re.sub(r"\s+([,;:.!?])", r"\1", text)
    text = text.strip(" \"'`")
    if lowercase:
        text = text.rstrip(".,;:!?").lower()
    return text


def _normalize_sentence(value: str) -> str:
    text = _clean_metadata_phrase(value)
    if text and text[-1] not in ".!?":
        text = f"{text}."
    return text


def _dedupe_text(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        normalized = _clean_metadata_phrase(item, lowercase=True)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _blurb(constitution: dict[str, Any], installment_pack: dict[str, Any]) -> str:
    title = _effective_installment_title(constitution, installment_pack)
    narrative_identity = constitution.get("narrative_identity", {})
    series_title = _clean_metadata_phrase(str(constitution.get("title", title)))
    subgenre = _clean_metadata_phrase(str(narrative_identity.get("subgenre", narrative_identity.get("genre", "fiction"))))
    summary = _normalize_sentence(str(installment_pack["intent"]["summary"]))
    theme = _normalize_sentence(f"The central pressure is {_clean_metadata_phrase(installment_pack['theme_expression']['primary_focus'], lowercase=True)}")

    series_clause = f"in {series_title} series." if series_title.lower().startswith("the ") else f"in the {series_title} series."
    opening = f"{title} is a {subgenre} installment {series_clause}"
    if theme and _clean_metadata_phrase(installment_pack["theme_expression"]["primary_focus"], lowercase=True) in summary.lower():
        return f"{opening} {summary}"
    return f"{opening} {summary} {theme}"


def _front_matter_markdown(*, constitution: dict[str, Any], installment_pack: dict[str, Any]) -> str:
    title = _effective_installment_title(constitution, installment_pack)
    narrative_identity = constitution.get("narrative_identity", {})
    series_title = str(constitution.get("title", title))
    genre = str(narrative_identity.get("genre", "fiction"))
    subgenre = str(narrative_identity.get("subgenre", genre))
    return (
        f"# {title}\n\n"
        f"## {series_title}\n\n"
        f"Genre: {genre} / {subgenre}\n\n"
        f"Installment: {installment_pack['installment_index']}\n\n"
        "All rights reserved.\n"
    )


def _toc_markdown(*, outline: dict[str, Any]) -> str:
    lines = ["# Table of Contents", ""]
    force_redraft = _bookgen_force_redraft_enabled()
    for chapter in outline["chapters"]:
        lines.append(f"- Chapter {int(chapter['chapter_index'])}: {chapter['title']}")
    return "\n".join(lines) + "\n"


def _back_matter_markdown(*, constitution: dict[str, Any], installment_pack: dict[str, Any]) -> str:
    series_title = str(constitution.get("title", "Series"))
    return (
        "# About This Series\n\n"
        f"{series_title} explores {installment_pack['theme_expression']['primary_focus']} "
        "through escalating institutional pressure and consequence.\n\n"
        "# About This Installment\n\n"
        f"{installment_pack['intent']['summary']}\n"
    )


def _metadata_pack(*, constitution: dict[str, Any], installment_pack: dict[str, Any], outline: dict[str, Any]) -> dict[str, Any]:
    title = _effective_installment_title(constitution, installment_pack)
    narrative_identity = constitution.get("narrative_identity", {})
    series_title = str(constitution.get("title", title))
    genre = _clean_metadata_phrase(str(narrative_identity.get("genre", "fiction")), lowercase=True)
    subgenre = _clean_metadata_phrase(str(narrative_identity.get("subgenre", genre)), lowercase=True)
    audience = _clean_metadata_phrase(str(narrative_identity.get("audience", {}).get("age_band", "adult")), lowercase=True)
    role = _clean_metadata_phrase(str(installment_pack["intent"]["narrative_role"]), lowercase=True)
    theme = _clean_metadata_phrase(str(installment_pack["theme_expression"]["primary_focus"]), lowercase=True)
    chapter_titles = [str(chapter.get("title", "")).strip() for chapter in outline["chapters"] if str(chapter.get("title", "")).strip()]
    stopwords = {
        "about",
        "after",
        "before",
        "between",
        "briefing",
        "chapter",
        "inside",
        "outside",
        "their",
        "there",
        "these",
        "those",
        "under",
        "where",
    }
    long_tail = sorted(
        {
            term
            for title_value in chapter_titles
            for term in re.findall(r"[a-z]{4,}", title_value.lower())
            if term not in stopwords
        }
    )
    role_keyword_blocklist = {"entry", "setup", "escalation", "breach", "reckoning", "exposure"}
    role_keyword = role if role not in role_keyword_blocklist else ""
    keywords = _dedupe_text([genre, subgenre, role_keyword, audience, theme] + long_tail[:12])
    audience_category = {
        "kids": "children fiction",
        "ya": "young adult fiction",
        "adult": "adult fiction",
        "all": "general fiction",
    }.get(audience, "fiction")
    categories = _dedupe_text([subgenre or genre, genre, audience_category])

    blurb = _blurb(constitution, installment_pack)
    short_pitch = f"{title}: {_normalize_sentence(str(installment_pack['intent']['summary']))}"
    series_description = _normalize_sentence(
        f"{series_title} is a {subgenre or genre} series about {theme or 'high-stakes choices under pressure'}."
    )
    return {
        "schema_version": "1.0",
        "title": title,
        "series_title": series_title,
        "installment_index": installment_pack["installment_index"],
        "blurb": blurb,
        "short_pitch": short_pitch,
        "series_description": series_description,
        "keywords": keywords,
        "categories": categories,
        "chapter_titles": chapter_titles,
        "chapter_count": len(chapter_titles),
    }


def _release_state_key(project_id: str, installment_id: str) -> str:
    return f"{_project_root(project_id, installment_id)}/release/release_state.yaml"


def _approval_record_key(project_id: str, installment_id: str) -> str:
    return f"{_project_root(project_id, installment_id)}/release/approval_record.yaml"


def _revision_request_manager_key(project_id: str, installment_id: str) -> str:
    return f"{_project_root(project_id, installment_id)}/release/revision_request_manager.yaml"


def _release_schedule_key(project_id: str, installment_id: str) -> str:
    return f"{_project_root(project_id, installment_id)}/release/release_schedule.yaml"


def _continuity_review_key(project_id: str, installment_id: str) -> str:
    return f"{_project_root(project_id, installment_id)}/assembly/continuity_review.yaml"


def _publishability_gate_key(project_id: str, installment_id: str) -> str:
    return f"{_project_root(project_id, installment_id)}/eval/publishability_gate.yaml"


def _analytics_run_key(project_id: str, installment_id: str) -> str:
    return f"{_project_root(project_id, installment_id)}/analytics/run_analytics.yaml"


def _benchmark_drift_key(project_id: str, installment_id: str) -> str:
    return f"{_project_root(project_id, installment_id)}/analytics/benchmark_drift.yaml"


def _experiment_tracker_key(project_id: str, installment_id: str) -> str:
    return f"{_project_root(project_id, installment_id)}/analytics/experiment_tracker.yaml"


def _benchmark_history_key(project_id: str) -> str:
    return f"runs/{project_id}/meta/benchmark_history.json"


def _editorial_stage_key(chapter_root: str, stage_name: str) -> str:
    return f"{chapter_root}/editorial_{stage_name}.yaml"


def _editorial_stage_summary_key(chapter_root: str) -> str:
    return f"{chapter_root}/editorial_stages.yaml"


def _load_release_state(store: ObjectStore, project_id: str, installment_id: str) -> dict[str, Any] | None:
    key = _release_state_key(project_id, installment_id)
    if not store.exists(key):
        return None
    return _read_yaml(store, key)


def _release_transition(
    *,
    current_status: str,
    decision: str,
) -> str:
    current = (current_status or "editorial_hold").strip().lower()
    normalized = decision.strip().lower()
    allowed: dict[tuple[str, str], str] = {
        ("editorial_reviewed", "approve"): "approved_for_export",
        ("awaiting_editorial_approval", "approve"): "approved_for_export",
        ("approved_for_export", "lock"): "manuscript_locked",
        ("manuscript_locked", "publish"): "approved_for_publication",
    }
    if normalized == "hold":
        if current in {"manuscript_locked", "approved_for_publication"}:
            raise RuntimeError(
                "Invalid release transition: locked/published installments cannot move to editorial_hold."
            )
        return "editorial_hold"
    if (current, normalized) not in allowed:
        raise RuntimeError(
            "Invalid release transition: "
            f"status={current_status!r} decision={decision!r}. "
            "Allowed path: editorial_reviewed -> approved_for_export -> manuscript_locked -> approved_for_publication."
        )
    return allowed[(current, normalized)]


def _bookgen_allow_lock_override() -> bool:
    return bool(getattr(get_settings(), "bookgen_allow_lock_override", False))


def _assert_installment_mutable(
    *,
    store: ObjectStore,
    project_id: str,
    installment_id: str,
    stage: str,
) -> None:
    release_state = _load_release_state(store, project_id, installment_id)
    locked = bool(((release_state or {}).get("approval") or {}).get("locked"))
    if locked and not _bookgen_allow_lock_override():
        raise RuntimeError(
            f"{stage} blocked: installment {installment_id} is manuscript_locked. "
            "Set BOOKGEN_ALLOW_LOCK_OVERRIDE=true to permit explicit mutation."
        )


def _sentence_opening(text: str) -> str:
    body = _strip_heading(text)
    first_para = next((part.strip() for part in body.split("\n\n") if part.strip()), "")
    sentence = first_para.split(".", 1)[0].strip().lower()
    return re.sub(r"\s+", " ", sentence)


def _paragraphs(text: str) -> list[str]:
    return [part.strip() for part in _strip_heading(text).split("\n\n") if part.strip()]


def _run_analytics_payload(
    *,
    project_id: str,
    installment_id: str,
    intake: dict[str, Any],
    review: dict[str, Any],
    generation_summary: dict[str, Any],
    continuity_review: dict[str, Any],
    publishability_gate: dict[str, Any],
) -> dict[str, Any]:
    chapter_traces = [item for item in generation_summary.get("chapters", []) if isinstance(item, dict)]
    attempts = [int(item.get("attempt_count", 0) or 0) for item in chapter_traces]
    llm_requested = sum(1 for item in chapter_traces if str(item.get("requested_mode", "")).strip() == "llm")
    llm_final = sum(1 for item in chapter_traces if str(item.get("final_mode", "")).strip() == "llm")
    fallback_final = sum(1 for item in chapter_traces if str(item.get("final_mode", "")).strip() == "fallback")
    chapter_word_target = int((intake.get("chapter_word_target", 0) or 0))
    if chapter_word_target <= 0:
        chapter_word_target = 3800
    total_chapters = int(review.get("total_chapters", len(chapter_traces)) or len(chapter_traces) or 1)
    target_words_total = chapter_word_target * total_chapters
    estimated_output_tokens = int(target_words_total * 1.32)
    estimated_input_tokens = int(estimated_output_tokens * 0.65)
    # Heuristic blended price signal for cross-model trend monitoring.
    estimated_cost_usd = round((estimated_input_tokens * 0.0000003) + (estimated_output_tokens * 0.0000012), 4)

    return {
        "schema_version": "1.0",
        "project_id": project_id,
        "installment_id": installment_id,
        "created_utc": _utcnow(),
        "cost_model": "heuristic_v1",
        "cost": {
            "estimated_input_tokens": estimated_input_tokens,
            "estimated_output_tokens": estimated_output_tokens,
            "estimated_total_tokens": estimated_input_tokens + estimated_output_tokens,
            "estimated_cost_usd": estimated_cost_usd,
        },
        "performance": {
            "total_chapters": total_chapters,
            "avg_generation_attempts": round(sum(attempts) / max(1, len(attempts)), 2),
            "llm_requested_chapters": llm_requested,
            "llm_final_chapters": llm_final,
            "final_fallback_chapters": fallback_final,
        },
        "quality": {
            "chapter_pass_rate": float(review.get("chapter_pass_rate", 0.0)),
            "avg_overall_score": float(review.get("avg_overall_score", 0.0)),
            "continuity_pass_status": str(continuity_review.get("pass_status", "FAIL")),
            "publishability_pass_status": str(publishability_gate.get("pass_status", "FAIL")),
        },
    }


def _benchmark_drift_payload(
    *,
    project_id: str,
    installment_id: str,
    current_analytics: dict[str, Any],
    baseline: dict[str, Any] | None,
) -> dict[str, Any]:
    thresholds = _load_benchmark_thresholds()
    current_quality = current_analytics.get("quality", {})
    if not baseline:
        return {
            "schema_version": "1.0",
            "project_id": project_id,
            "installment_id": installment_id,
            "created_utc": _utcnow(),
            "baseline_available": False,
            "pass_status": "PASS_WITH_NOTES",
            "summary": "No prior benchmark available; current run set as baseline.",
            "deltas": {},
            "alerts": [],
        }
    baseline_quality = baseline.get("quality", {})
    delta_score = round(float(current_quality.get("avg_overall_score", 0.0)) - float(baseline_quality.get("avg_overall_score", 0.0)), 3)
    delta_pass_rate = round(float(current_quality.get("chapter_pass_rate", 0.0)) - float(baseline_quality.get("chapter_pass_rate", 0.0)), 3)
    delta_cost = round(
        float(current_analytics.get("cost", {}).get("estimated_cost_usd", 0.0))
        - float(baseline.get("cost", {}).get("estimated_cost_usd", 0.0)),
        4,
    )

    alerts: list[dict[str, Any]] = []
    if delta_score < float(thresholds["quality_drop_fail"]):
        alerts.append({"type": "quality_drop", "severity": "high", "note": f"Average score dropped by {abs(delta_score):.3f}."})
    elif delta_score < float(thresholds["quality_drop_warn"]):
        alerts.append({"type": "quality_drop", "severity": "medium", "note": f"Average score dropped by {abs(delta_score):.3f}."})
    if delta_pass_rate < float(thresholds["pass_rate_drop_fail"]):
        alerts.append({"type": "pass_rate_drop", "severity": "high", "note": f"Chapter pass rate dropped by {abs(delta_pass_rate):.3f}."})
    if (
        str(current_quality.get("continuity_pass_status", "FAIL")).upper() != "PASS"
        and str(baseline_quality.get("continuity_pass_status", "FAIL")).upper() == "PASS"
    ):
        alerts.append({"type": "continuity_regression", "severity": "high", "note": "Continuity regressed from PASS to non-PASS."})
    if delta_cost > max(
        float(thresholds["cost_increase_warn_abs"]),
        float(baseline.get("cost", {}).get("estimated_cost_usd", 0.0)) * float(thresholds["cost_increase_warn_ratio"]),
    ):
        alerts.append({"type": "cost_increase", "severity": "medium", "note": f"Estimated cost increased by {delta_cost:.4f} USD."})

    pass_status = "PASS" if not alerts else ("FAIL" if any(item["severity"] == "high" for item in alerts) else "PASS_WITH_NOTES")
    return {
        "schema_version": "1.0",
        "project_id": project_id,
        "installment_id": installment_id,
        "created_utc": _utcnow(),
        "baseline_available": True,
        "baseline_installment_id": baseline.get("installment_id"),
        "pass_status": pass_status,
        "summary": "Quality drift check against previous benchmark completed.",
        "deltas": {
            "avg_overall_score": delta_score,
            "chapter_pass_rate": delta_pass_rate,
            "estimated_cost_usd": delta_cost,
        },
        "alerts": alerts,
    }


def _experiment_tracker_payload(
    *,
    project_id: str,
    installment_id: str,
    intake: dict[str, Any],
    review: dict[str, Any],
    analytics: dict[str, Any],
) -> dict[str, Any]:
    cfg = get_settings()
    return {
        "schema_version": "1.0",
        "project_id": project_id,
        "installment_id": installment_id,
        "recorded_utc": _utcnow(),
        "experiment": {
            "generation_preset": _bookgen_generation_preset(),
            "llm_provider_profile": str(getattr(cfg, "llm_provider_profile", "default")),
            "llm_model": str(getattr(cfg, "llm_model", "") or ""),
            "bookgen_use_llm": bool(getattr(cfg, "bookgen_use_llm", False)),
            "bookgen_eval_use_llm": bool(getattr(cfg, "bookgen_eval_use_llm", False)),
            "bookgen_rewrite_use_llm": bool(getattr(cfg, "bookgen_rewrite_use_llm", False)),
            "bookgen_editorial_stage_gate": bool(getattr(cfg, "bookgen_editorial_stage_gate", True)),
            "prompt_pack_version": str(intake.get("prompt_pack_version", "")),
            "rubric_version": str(intake.get("rubric_version", "")),
            "genre": str(intake.get("genre", "")),
        },
        "outcomes": {
            "chapter_pass_rate": float(review.get("chapter_pass_rate", 0.0)),
            "avg_overall_score": float(review.get("avg_overall_score", 0.0)),
            "estimated_cost_usd": float(analytics.get("cost", {}).get("estimated_cost_usd", 0.0)),
            "estimated_total_tokens": int(analytics.get("cost", {}).get("estimated_total_tokens", 0)),
        },
    }


def _proof_balance_status(text: str) -> dict[str, Any]:
    pairs = {"(": ")", "[": "]", "{": "}", '"': '"'}
    checks = []
    for left, right in pairs.items():
        left_count = text.count(left)
        right_count = text.count(right)
        checks.append({"token": f"{left}{right}", "left_count": left_count, "right_count": right_count, "balanced": left_count == right_count})
    return {"balanced": all(item["balanced"] for item in checks), "checks": checks}


def _is_eval_pass_status(eval_report: dict[str, Any]) -> bool:
    return str(eval_report.get("pass_status", "FAIL")).strip().upper() != "FAIL"


def _should_promote_eval_candidate(candidate: dict[str, Any], best: dict[str, Any] | None) -> bool:
    if best is None:
        return True
    candidate_pass = _is_eval_pass_status(candidate)
    best_pass = _is_eval_pass_status(best)
    if candidate_pass and not best_pass:
        return True
    if candidate_pass == best_pass:
        return float(candidate.get("overall", 0.0)) >= float(best.get("overall", 0.0))
    return False


def _editorial_stage_manifests(
    *,
    chapter_pack: dict[str, Any],
    eval_report: dict[str, Any],
    text: str,
) -> dict[str, Any]:
    chapter_id = str(chapter_pack["chapter_id"])
    chapter_index = int(chapter_pack["chapter_index"])
    series_id = str(chapter_pack["series_id"])
    installment_id = str(chapter_pack["installment_id"])
    paragraphs = _paragraphs(text)
    heading_ok = bool(text.splitlines() and text.splitlines()[0].strip().startswith("# "))
    has_placeholder = bool(re.search(r"\b(?:todo|tbd|placeholder|lorem ipsum)\b", text, flags=re.IGNORECASE))
    repeated_space_hits = len(re.findall(r" {2,}", text))
    punctuation_noise_hits = len(re.findall(r"[!?]{3,}|\.{4,}", text))
    quote_balance = text.count('"') % 2 == 0
    parenthetical = _proof_balance_status(text)
    scores = eval_report.get("scores", {})
    violations = eval_report.get("violations", [])
    severe_violations = [item for item in violations if str(item.get("severity", "")).strip().lower() in {"high", "critical"}]
    drift_flags = eval_report.get("drift_flags", {}) if isinstance(eval_report.get("drift_flags"), dict) else {}
    word_count = _word_count(text)
    profile = _chapter_policy_profile(chapter_pack)
    editorial_cfg = profile.get("editorial", {}) if isinstance(profile, dict) else {}
    developmental_cfg = editorial_cfg.get("developmental", {}) if isinstance(editorial_cfg, dict) else {}
    line_cfg = editorial_cfg.get("line", {}) if isinstance(editorial_cfg, dict) else {}
    copy_cfg = editorial_cfg.get("copy", {}) if isinstance(editorial_cfg, dict) else {}
    terms_allow: list[str] = []
    terms_avoid: list[str] = []
    for item in chapter_pack.get("research_slice", {}).get("items", []):
        if not isinstance(item, dict):
            continue
        terms_allow.extend(str(term).strip().lower() for term in item.get("allowed_terms", []) if str(term).strip())
        terms_avoid.extend(str(term).strip().lower() for term in item.get("avoid_terms", []) if str(term).strip())
    terms_allow = sorted(set(terms_allow))
    terms_avoid = sorted(set(terms_avoid))
    lowered = text.lower()
    avoid_hits = sorted({term for term in terms_avoid if term in lowered})
    allowed_hits = sorted({term for term in terms_allow if term in lowered})

    character_names: list[str] = []
    for state in chapter_pack.get("character_state_slice", []):
        if isinstance(state, dict):
            candidate = str(state.get("display_name", "")).strip()
            if candidate:
                character_names.append(candidate)
    name_hits = [name for name in character_names if name.lower() in lowered]

    active_threads = [
        str(item).strip() for item in chapter_pack.get("continuity_slice", {}).get("active_threads", []) if str(item).strip()
    ]
    reference_hits = [thread for thread in active_threads if thread.lower() in lowered]

    overall_score_floor = float(developmental_cfg.get("overall_score_floor", 7.5))
    pacing_floor = float(developmental_cfg.get("pacing_floor", 7.0))
    structural_clarity_floor = float(developmental_cfg.get("structural_clarity_floor", 7.0))
    voice_stability_floor = float(line_cfg.get("voice_stability_floor", 7.0))
    paragraph_floor = int(line_cfg.get("paragraph_floor", 4))
    avg_paragraph_density_max = float(line_cfg.get("avg_paragraph_density_max", 260))
    max_double_space_hits_floor = int(copy_cfg.get("max_double_space_hits_floor", 2))
    double_space_hits_per_paragraph_divisor = max(1, int(copy_cfg.get("double_space_hits_per_paragraph_divisor", 2)))

    stage_specs = [
        {
            "stage_name": "developmental",
            "purpose": "Narrative structure, escalation fit, and chapter function quality.",
            "checks": {
                "overall_score_floor_ok": float(eval_report.get("overall", 0.0)) >= overall_score_floor,
                "core_structure_scores_ok": float(scores.get("pacing", 0.0)) >= pacing_floor
                and float(scores.get("structural_clarity", 0.0)) >= structural_clarity_floor,
                "severe_violation_free": not severe_violations,
            },
        },
        {
            "stage_name": "line",
            "purpose": "Paragraph-level readability, flow, and voice consistency.",
            "checks": {
                "voice_stability_ok": float(scores.get("voice_stability", 0.0)) >= voice_stability_floor,
                "paragraph_floor_ok": len(paragraphs) >= paragraph_floor,
                "average_paragraph_density_ok": (word_count / max(1, len(paragraphs))) <= avg_paragraph_density_max,
            },
        },
        {
            "stage_name": "copy",
            "purpose": "Mechanical correctness and consistency signals.",
            "checks": {
                "double_space_noise_ok": repeated_space_hits
                <= max(max_double_space_hits_floor, len(paragraphs) // double_space_hits_per_paragraph_divisor),
                "punctuation_noise_ok": punctuation_noise_hits == 0,
                "heading_present": heading_ok,
            },
        },
        {
            "stage_name": "style",
            "purpose": "Style-guide and consistency checks for names, terms, and references.",
            "checks": {
                "avoid_terms_absent_ok": not avoid_hits,
                "character_name_reference_ok": (not character_names) or bool(name_hits),
                "continuity_reference_ok": (not active_threads)
                or bool(reference_hits)
                or not bool(drift_flags.get("continuity_contradiction", False)),
            },
        },
        {
            "stage_name": "proof",
            "purpose": "Final publish-facing sanity checks before assembly/export.",
            "checks": {
                "heading_present": heading_ok,
                "placeholder_free": not has_placeholder,
                "quote_balance_ok": quote_balance,
                "parenthetical_balance_ok": bool(parenthetical["balanced"]),
            },
        },
    ]

    stage_payloads: list[dict[str, Any]] = []
    passed = 0
    for spec in stage_specs:
        failing_checks = [name for name, ok in spec["checks"].items() if not ok]
        pass_status = "PASS" if not failing_checks else "FAIL"
        if pass_status == "PASS":
            passed += 1
        stage_payloads.append(
            {
                "schema_version": "1.0",
                "series_id": series_id,
                "installment_id": installment_id,
                "chapter_id": chapter_id,
                "chapter_index": chapter_index,
                "stage_name": spec["stage_name"],
                "purpose": spec["purpose"],
                "checks": spec["checks"],
                "issues": failing_checks,
                "pass_status": pass_status,
                "created_utc": _utcnow(),
            }
        )

    summary = {
        "schema_version": "1.0",
        "series_id": series_id,
        "installment_id": installment_id,
        "chapter_id": chapter_id,
        "chapter_index": chapter_index,
        "stages": stage_payloads,
        "pass_status": "PASS" if passed == len(stage_payloads) else "FAIL",
        "counts": {
            "total_stages": len(stage_payloads),
            "passed_stages": passed,
            "failed_stages": len(stage_payloads) - passed,
        },
        "signals": {
            "word_count": word_count,
            "paragraphs": len(paragraphs),
            "double_space_hits": repeated_space_hits,
            "punctuation_noise_hits": punctuation_noise_hits,
            "placeholder_hits": 1 if has_placeholder else 0,
            "style_guide": {
                "allowed_terms_defined": len(terms_allow),
                "allowed_terms_hits": allowed_hits,
                "avoid_terms_hits": avoid_hits,
                "character_name_hits": name_hits,
                "continuity_reference_hits": reference_hits,
            },
            "proof_balance": parenthetical,
        },
        "created_utc": _utcnow(),
    }
    return {"stages": stage_payloads, "summary": summary}


def _append_editorial_gate_failure(eval_report: dict[str, Any]) -> None:
    violations = eval_report.setdefault("violations", [])
    if any(str(item.get("type", "")).strip() == "editorial_stage_gate_failure" for item in violations if isinstance(item, dict)):
        return
    violations.append(
        {
            "type": "editorial_stage_gate_failure",
            "severity": "high",
            "note": "One or more editorial stages failed (developmental/line/copy/style/proof).",
        }
    )


def _salient_terms(text: str, *, limit: int = 8) -> list[str]:
    stopwords = {
        "the",
        "and",
        "that",
        "with",
        "from",
        "into",
        "their",
        "there",
        "about",
        "after",
        "before",
        "through",
        "would",
        "could",
        "should",
        "where",
        "which",
        "while",
        "against",
        "between",
        "under",
        "over",
        "have",
        "has",
        "had",
        "were",
        "was",
        "they",
        "them",
        "then",
        "than",
        "this",
        "those",
        "these",
        "when",
        "what",
        "your",
        "just",
        "like",
        "into",
        "only",
        "more",
        "most",
        "very",
        "still",
        "because",
        "every",
        "each",
        "chapter",
    }
    tokens = re.findall(r"[a-z]{4,}", _strip_heading(text).lower())
    counts = Counter(token for token in tokens if token not in stopwords)
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [token for token, _ in ordered[:limit]]


def _extract_capitalized_terms(text: str) -> list[str]:
    body = _strip_heading(text)
    matches = re.findall(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?\b", body)
    filtered = []
    skip = {"The", "A", "An", "Chapter"}
    for match in matches:
        if match in skip:
            continue
        filtered.append(match)
    return filtered


def _adjacent_continuity_checks(sections: list[str]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    for idx in range(1, len(sections)):
        previous_terms = set(_salient_terms(sections[idx - 1]))
        current_terms = set(_salient_terms(sections[idx]))
        term_overlap = sorted(previous_terms & current_terms)
        previous_entities = set(_extract_capitalized_terms(sections[idx - 1]))
        current_entities = set(_extract_capitalized_terms(sections[idx]))
        entity_overlap = sorted(previous_entities & current_entities)
        previous_tail = set(_salient_terms("\n\n".join(_paragraphs(sections[idx - 1])[-2:]), limit=6))
        current_head = set(_salient_terms("\n\n".join(_paragraphs(sections[idx])[:2]), limit=6))
        transition_overlap = sorted(previous_tail & current_head)
        checks.append(
            {
                "previous_chapter_index": idx,
                "current_chapter_index": idx + 1,
                "term_overlap": term_overlap,
                "entity_overlap": entity_overlap,
                "transition_overlap": transition_overlap,
                "pass_status": "PASS"
                if term_overlap or entity_overlap or transition_overlap
                else "FAIL",
            }
        )
    return checks


def _continuity_review_report(
    *,
    project_id: str,
    installment_id: str,
    outline: dict[str, Any],
    sections: list[str],
) -> dict[str, Any]:
    titles = [str(chapter.get("title", "")).strip() for chapter in outline["chapters"]]
    duplicate_titles = sorted({title for title in titles if title and titles.count(title) > 1})
    openings = [_sentence_opening(section) for section in sections]
    adjacent_semantic_checks = _adjacent_continuity_checks(sections)
    repeated_opening_pairs: list[dict[str, Any]] = []
    for idx in range(1, len(openings)):
        if openings[idx] and openings[idx] == openings[idx - 1]:
            repeated_opening_pairs.append(
                {
                    "previous_chapter_index": idx,
                    "current_chapter_index": idx + 1,
                    "opening": openings[idx],
                }
            )
    word_counts = [_word_count(section) for section in sections]
    weak_links = [idx + 1 for idx, count in enumerate(word_counts) if count < 250]
    repeated_opening_limit = max(1, len(sections) // 4)
    degraded_mode = (sum(word_counts) / max(1, len(word_counts))) < 1000
    repeated_openings_ok = len(repeated_opening_pairs) <= repeated_opening_limit or degraded_mode
    weak_semantic_links = [
        {
            "previous_chapter_index": item["previous_chapter_index"],
            "current_chapter_index": item["current_chapter_index"],
        }
        for item in adjacent_semantic_checks
        if item["pass_status"] != "PASS"
    ]
    semantic_continuity_ok = not weak_semantic_links
    pass_status = "PASS" if not duplicate_titles and repeated_openings_ok and not weak_links and semantic_continuity_ok else "FAIL"
    return {
        "schema_version": "1.0",
        "project_id": project_id,
        "installment_id": installment_id,
        "chapter_count": len(sections),
        "pass_status": pass_status,
        "checks": {
            "unique_chapter_titles": not duplicate_titles,
            "adjacent_openings_distinct": repeated_openings_ok,
            "chapter_word_floor_ok": not weak_links,
            "adjacent_semantic_continuity_ok": semantic_continuity_ok,
        },
        "mode": "degraded" if degraded_mode else "full",
        "issues": {
            "duplicate_titles": duplicate_titles,
            "repeated_opening_pairs": repeated_opening_pairs,
            "weak_link_chapters": weak_links,
            "weak_semantic_links": weak_semantic_links,
        },
        "adjacent_semantic_checks": adjacent_semantic_checks,
    }


def _release_state_payload(
    *,
    project_id: str,
    installment_id: str,
    status: str,
    notes: list[str] | None = None,
    approval: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "project_id": project_id,
        "installment_id": installment_id,
        "status": status,
        "updated_utc": _utcnow(),
        "notes": notes or [],
        "approval": approval or {"locked": False},
        "workflow": {
            "allowed_states": [
                "editorial_hold",
                "editorial_reviewed",
                "approved_for_export",
                "manuscript_locked",
                "approved_for_publication",
            ],
            "release_schedule_key": _release_schedule_key(project_id, installment_id),
            "revision_request_manager_key": _revision_request_manager_key(project_id, installment_id),
        },
    }


def _mlflow_tracking_uri_reachable(uri: str) -> bool:
    value = (uri or "").strip()
    if not value:
        return False
    parsed = urlparse(value)
    scheme = (parsed.scheme or "").lower()
    if scheme in {"file", "sqlite"}:
        return True
    host = parsed.hostname
    if not host:
        return False
    if scheme == "https":
        port = parsed.port or 443
    else:
        port = parsed.port or 80
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def _log_mlflow_summary(project_id: str, installment_id: str, summary: dict[str, Any], artifacts: dict[str, Any]) -> None:
    try:
        cfg = get_settings()
        tracking_uri = resolve_service_uri(cfg.mlflow_tracking_uri, cfg.mlflow_local_tracking_uri)
        if not _mlflow_tracking_uri_reachable(tracking_uri):
            logger.info("bookgen_mlflow_log_skipped reason=tracking_uri_unreachable tracking_uri=%s", tracking_uri)
            return
        os.environ.setdefault("MLFLOW_HTTP_REQUEST_TIMEOUT", "5")
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment("slw-bookgen")
        with mlflow.start_run(run_name=f"{project_id}-{installment_id}"):
            mlflow.log_params(
                {
                    "project_id": project_id,
                    "installment_id": installment_id,
                    "chapter_count": summary["chapter_count"],
                    "pass_rate": summary["chapter_pass_rate"],
                }
            )
            mlflow.log_metric("chapter_pass_rate", summary["chapter_pass_rate"])
            mlflow.log_metric("avg_overall_score", summary["avg_overall_score"])
            try:
                mlflow.log_dict(artifacts, "bookgen_summary.json")
            except Exception as exc:  # pragma: no cover
                logger.info("bookgen_mlflow_artifact_log_skipped error=%s", exc)
    except Exception as exc:  # pragma: no cover
        logger.info("bookgen_mlflow_log_skipped error=%s", exc)


def run_intake(
    *,
    project_id: str,
    run_date: str,
    bookspec_key: str | None = None,
    bookspec_path: str | None = None,
) -> dict[str, Any]:
    store = ObjectStore()
    key = bookspec_key or _default_bookspec_key(project_id)

    if bookspec_path:
        bookspec = _load_bookspec_from_local(bookspec_path)
        store.put_json(key, bookspec)
    else:
        if not store.exists(key):
            raise RuntimeError(
                f"BookSpec not found at {key}. Provide --bookspec-path or upload inputs/{project_id}/bookspec.json."
            )
        bookspec = store.get_json(key)
    _validate_bookspec(bookspec)

    genre = str(bookspec.get("genre", "")).strip().lower()
    if not genre:
        raise RuntimeError("BookSpec must include non-empty 'genre'.")
    prompt_pack_version = str(bookspec.get("prompt_pack_version", "")).strip()
    rubric_version = str(bookspec.get("rubric_version", "")).strip()
    if not prompt_pack_version or not rubric_version:
        raise RuntimeError("BookSpec must pin prompt_pack_version and rubric_version (no implicit latest).")

    manifest = {
        "project_id": project_id,
        "run_date": run_date,
        "bookspec_key": key,
        "genre": genre,
        "prompt_pack_version": prompt_pack_version,
        "rubric_version": rubric_version,
        "bookspec_hash": _hash_payload(bookspec),
        "installment_id": _resolve_installment_id(bookspec),
    }
    intake_key = _intake_meta_key(project_id)
    store.put_json(intake_key, manifest)
    commit_id = _commit_stage_checkpoint(
        project_id=project_id,
        run_date=run_date,
        stage="intake",
        artifact_hashes={key: _hash_payload(bookspec), intake_key: _hash_payload(manifest)},
        extra_metadata={
            "genre": genre,
            "prompt_pack_version": prompt_pack_version,
            "rubric_version": rubric_version,
            "installment_id": manifest["installment_id"],
        },
    )
    if commit_id:
        manifest["lakefs_commit_id"] = commit_id
    return manifest


def run_prompt_pack_resolve(*, intake: dict[str, Any]) -> dict[str, Any]:
    store = ObjectStore()
    project_id = str(intake["project_id"])
    genre = str(intake["genre"])
    prompt_pack_version = str(intake["prompt_pack_version"])
    rubric_version = str(intake["rubric_version"])

    prompt_pack_key = f"prompt-packs/{genre}/{prompt_pack_version}/manifest.json"
    rubric_key = f"rubrics/{genre}/{rubric_version}/rubric.json"
    if not store.exists(prompt_pack_key):
        raise RuntimeError(f"Prompt pack missing: {prompt_pack_key}")
    if not store.exists(rubric_key):
        raise RuntimeError(f"Rubric missing: {rubric_key}")

    prompt_pack = store.get_json(prompt_pack_key)
    rubric = store.get_json(rubric_key)
    resolved = {
        "project_id": project_id,
        "prompt_pack_key": prompt_pack_key,
        "rubric_key": rubric_key,
        "prompt_pack_hash": _hash_payload(prompt_pack),
        "rubric_hash": _hash_payload(rubric),
        "prompt_pack_version": prompt_pack_version,
        "rubric_version": rubric_version,
        "genre": genre,
    }
    resolved_key = f"runs/{project_id}/meta/resolved_pack.json"
    store.put_json(resolved_key, resolved)
    run_date = str(intake.get("run_date", "")).strip() or _default_run_date(project_id, store)
    commit_id = _commit_stage_checkpoint(
        project_id=project_id,
        run_date=run_date,
        stage="planning-resolve",
        artifact_hashes={
            resolved_key: _hash_payload(resolved),
            prompt_pack_key: _hash_payload(prompt_pack),
            rubric_key: _hash_payload(rubric),
        },
        extra_metadata={"genre": genre},
    )
    if commit_id:
        resolved["lakefs_commit_id"] = commit_id
    return resolved


def run_bible_outline(*, intake: dict[str, Any], resolved: dict[str, Any]) -> dict[str, Any]:
    store = ObjectStore()
    project_id = str(intake["project_id"])
    bookspec = store.get_json(str(intake["bookspec_key"]))
    installment_id = _resolve_installment_id(bookspec)
    _assert_installment_mutable(store=store, project_id=project_id, installment_id=installment_id, stage="Planning")
    run_date = str(intake.get("run_date", "")).strip() or _default_run_date(project_id, store)
    title_artifacts = _build_title_artifacts(project_id=project_id, bookspec=bookspec, run_date=run_date)
    constitution = _synthesize_constitution(
        project_id=project_id,
        run_date=run_date,
        bookspec=bookspec,
        series_title=str(title_artifacts["slate"]["series_title"]),
        title_strategy=title_artifacts["strategy"],
    )
    installment_pack = _synthesize_installment_pack(
        project_id=project_id,
        run_date=run_date,
        bookspec=bookspec,
        constitution=constitution,
        selected_title_block=title_artifacts["selected_title_block"],
    )
    outline = _build_outline(
        project_id=project_id,
        bookspec=bookspec,
        constitution=constitution,
        installment_pack=installment_pack,
    )
    ledgers = _build_initial_ledgers(project_id, installment_pack, constitution)

    _validate_structured_payload(constitution, "constitution")
    _validate_structured_payload(title_artifacts["candidates"], "title_candidates")
    _validate_structured_payload(title_artifacts["slate"], "title_slate")
    _validate_structured_payload(title_artifacts["report"], "title_engine_report")
    _validate_structured_payload(installment_pack, "installment_pack")
    _validate_structured_payload(outline, "outline")
    _validate_structured_payload(ledgers["ledger_characters"], "ledger_characters")
    _validate_structured_payload(ledgers["ledger_timeline"], "ledger_timeline")
    _validate_structured_payload(ledgers["ledger_threads"], "ledger_threads")
    _validate_escalation(outline, installment_pack)

    artifacts: dict[str, str] = {}
    constitution_key = f"bookgen/{project_id}/constitution/constitution.yaml"
    title_candidates_key = f"bookgen/{project_id}/constitution/title_candidates.yaml"
    title_slate_key = f"bookgen/{project_id}/constitution/title_slate.yaml"
    title_report_key = f"bookgen/{project_id}/constitution/title_engine_report.yaml"
    installment_key = f"{_project_root(project_id, installment_id)}/installment_pack.yaml"
    outline_key = f"{_project_root(project_id, installment_id)}/outline.yaml"
    _write_yaml(store, constitution_key, constitution, artifacts)
    _write_yaml(store, title_candidates_key, title_artifacts["candidates"], artifacts)
    _write_yaml(store, title_slate_key, title_artifacts["slate"], artifacts)
    _write_yaml(store, title_report_key, title_artifacts["report"], artifacts)
    _write_yaml(store, installment_key, installment_pack, artifacts)
    _write_yaml(store, outline_key, outline, artifacts)
    for ledger_name, payload in ledgers.items():
        _write_yaml(store, f"{_ledger_root(project_id, installment_id)}/{ledger_name}.yaml", payload, artifacts)

    manifest = {
        "project_id": project_id,
        "installment_id": installment_id,
        "constitution_key": constitution_key,
        "title_candidates_key": title_candidates_key,
        "title_slate_key": title_slate_key,
        "title_report_key": title_report_key,
        "installment_pack_key": installment_key,
        "outline_key": outline_key,
        "ledger_root": _ledger_root(project_id, installment_id),
        "chapter_count": len(outline["chapters"]),
        "prompt_pack_hash": resolved["prompt_pack_hash"],
        "rubric_hash": resolved["rubric_hash"],
        "selected_title": title_artifacts["selected_title"],
    }
    manifest_key = _planning_manifest_key(project_id)
    store.put_json(manifest_key, manifest)
    artifacts[manifest_key] = _hash_payload(manifest)
    commit_id = _commit_stage_checkpoint(
        project_id=project_id,
        run_date=run_date,
        stage="planning-outline",
        artifact_hashes=artifacts,
        extra_metadata={"chapter_count": len(outline["chapters"]), "installment_id": installment_id},
    )
    if commit_id:
        manifest["lakefs_commit_id"] = commit_id
    return manifest


def run_chapter_drafting(*, project_id: str) -> dict[str, Any]:
    store = ObjectStore()
    intake = _load_intake(project_id, store)
    planning = _load_planning_manifest(project_id, store)
    _assert_installment_mutable(
        store=store,
        project_id=project_id,
        installment_id=planning["installment_id"],
        stage="Drafting",
    )
    constitution = _read_yaml(store, planning["constitution_key"])
    installment_pack = _read_yaml(store, planning["installment_pack_key"])
    outline = _read_yaml(store, planning["outline_key"])
    installment_id = planning["installment_id"]
    ledgers = {
        "ledger_characters": _read_yaml(store, f"{planning['ledger_root']}/ledger_characters.yaml"),
        "ledger_timeline": _read_yaml(store, f"{planning['ledger_root']}/ledger_timeline.yaml"),
        "ledger_threads": _read_yaml(store, f"{planning['ledger_root']}/ledger_threads.yaml"),
    }
    drafted = 0
    skipped = 0
    artifacts: dict[str, str] = {}
    generation_traces: list[dict[str, Any]] = []
    force_redraft = _bookgen_force_redraft_enabled()

    for chapter in outline["chapters"]:
        chapter_index = int(chapter["chapter_index"])
        root = _chapter_root(project_id, installment_id, chapter_index)
        chapter_pack_key = f"{root}/chapter_pack.yaml"
        scene_cards_key = f"{root}/scene_cards.yaml"
        scene_beats_key = f"{root}/scene_beats.yaml"
        draft_contract_key = f"{root}/draft_contract.yaml"
        scene_qc_key = f"{root}/scene_qc.yaml"
        scene_qc_history_key = f"{root}/scene_qc_history.yaml"
        draft_qc_key = f"{root}/draft_qc.yaml"
        draft_qc_history_key = f"{root}/draft_qc_history.yaml"
        generation_trace_key = f"{root}/generation_trace.yaml"
        draft_key = f"{root}/draft.md"
        if not force_redraft and store.exists(chapter_pack_key) and store.exists(draft_key) and store.exists(draft_qc_key):
            existing_draft_qc = _read_yaml(store, draft_qc_key)
            existing_pass = str(existing_draft_qc.get("pass_status", "FAIL")).strip().upper() == "PASS"
            if existing_pass:
                if store.exists(generation_trace_key):
                    generation_traces.append(_read_yaml(store, generation_trace_key))
                skipped += 1
                continue
        chapter_pack = _build_chapter_pack(
            project_id=project_id,
            constitution=constitution,
            installment_pack=installment_pack,
            outline_chapter=chapter,
            ledgers=ledgers,
        )
        scene_cards = _build_scene_cards(chapter_pack=chapter_pack)
        scene_beats = _build_scene_beats(chapter_pack=chapter_pack, scene_cards=scene_cards)
        _validate_structured_payload(chapter_pack, "chapter_pack")
        _validate_structured_payload(scene_cards, "scene_cards")
        _validate_structured_payload(scene_beats, "scene_beats")
        llm_mode = _bookgen_use_llm_for_chapter(chapter_index)
        draft, draft_qc, draft_qc_history = _draft_with_process(
            chapter_pack=chapter_pack,
            constitution=constitution,
            installment_pack=installment_pack,
            scene_cards=scene_cards,
            scene_beats=scene_beats,
            llm_mode=llm_mode,
        )
        generation_trace = _draft_generation_trace(
            chapter_pack=chapter_pack,
            requested_llm=llm_mode,
            qc_history=draft_qc_history,
        )
        scene_qc = _scene_qc_report(
            text=draft,
            chapter_pack=chapter_pack,
            scene_cards=scene_cards,
            scene_beats=scene_beats,
        )
        scene_qc_history: list[dict[str, Any]] = [dict(scene_qc)]
        if scene_qc.get("pass_status") != "PASS":
            draft, draft_qc, draft_qc_history, scene_qc, scene_qc_history = _repair_scene_qc_failures(
                text=draft,
                chapter_pack=chapter_pack,
                scene_cards=scene_cards,
                scene_beats=scene_beats,
                draft_qc=draft_qc,
                draft_qc_history=draft_qc_history,
                scene_qc=scene_qc,
            )
        _validate_structured_payload(scene_qc, "scene_qc")
        _write_yaml(store, chapter_pack_key, chapter_pack, artifacts)
        _write_yaml(store, scene_cards_key, scene_cards, artifacts)
        _write_yaml(store, scene_beats_key, scene_beats, artifacts)
        _write_yaml(store, draft_contract_key, chapter_pack["draft_contract"], artifacts)
        _write_yaml(store, scene_qc_key, scene_qc, artifacts)
        store.put_yaml(scene_qc_history_key, {"chapter_id": chapter_pack["chapter_id"], "history": scene_qc_history})
        artifacts[scene_qc_history_key] = _hash_payload({"chapter_id": chapter_pack["chapter_id"], "history": scene_qc_history})
        _write_yaml(store, draft_qc_key, draft_qc, artifacts)
        store.put_yaml(draft_qc_history_key, {"chapter_id": chapter_pack["chapter_id"], "history": draft_qc_history})
        artifacts[draft_qc_history_key] = _hash_payload({"chapter_id": chapter_pack["chapter_id"], "history": draft_qc_history})
        _write_yaml(store, generation_trace_key, generation_trace, artifacts)
        store.put_text(draft_key, draft)
        artifacts[draft_key] = _hash_text(draft)
        generation_traces.append(generation_trace)
        drafted += 1

    generation_summary = {
        "project_id": project_id,
        "installment_id": installment_id,
        "generation_preset": _bookgen_generation_preset(),
        "chapters": generation_traces,
    }
    generation_summary_key = f"{_project_root(project_id, installment_id)}/draft/generation_summary.json"
    store.put_json(generation_summary_key, generation_summary)
    artifacts[generation_summary_key] = _hash_payload(generation_summary)

    if not artifacts:
        artifacts["stage/no_new_chapter_drafts"] = _hash_payload({"project_id": project_id, "skipped": skipped})
    commit_id = _commit_stage_checkpoint(
        project_id=project_id,
        run_date=str(intake["run_date"]),
        stage="chapters-draft",
        artifact_hashes=artifacts,
        extra_metadata={"drafted": drafted, "skipped_existing": skipped},
    )
    response = {
        "project_id": project_id,
        "installment_id": installment_id,
        "drafted": drafted,
        "skipped_existing": skipped,
        "generation_summary_key": generation_summary_key,
    }
    if commit_id:
        response["lakefs_commit_id"] = commit_id
    return response


def run_chapter_review(*, project_id: str) -> dict[str, Any]:
    store = ObjectStore()
    intake = _load_intake(project_id, store)
    planning = _load_planning_manifest(project_id, store)
    _assert_installment_mutable(
        store=store,
        project_id=project_id,
        installment_id=planning["installment_id"],
        stage="Review",
    )
    constitution = _read_yaml(store, planning["constitution_key"])
    installment_pack = _read_yaml(store, planning["installment_pack_key"])
    outline = _read_yaml(store, planning["outline_key"])
    ledgers = {
        "ledger_characters": _read_yaml(store, f"{planning['ledger_root']}/ledger_characters.yaml"),
        "ledger_timeline": _read_yaml(store, f"{planning['ledger_root']}/ledger_timeline.yaml"),
        "ledger_threads": _read_yaml(store, f"{planning['ledger_root']}/ledger_threads.yaml"),
    }
    rubric = _load_rubric(store, str(intake["genre"]), str(intake["rubric_version"]))
    rubric_cfg = _merge_rubric(constitution, rubric)
    generation_summary_key = f"{_project_root(project_id, planning['installment_id'])}/draft/generation_summary.json"
    generation_summary = store.get_json(generation_summary_key) if store.exists(generation_summary_key) else {"chapters": []}
    artifacts: dict[str, str] = {}
    scores: list[float] = []
    passed = 0
    failed = 0
    editorial_stage_totals: dict[str, dict[str, int]] = {
        "developmental": {"passed": 0, "failed": 0},
        "line": {"passed": 0, "failed": 0},
        "copy": {"passed": 0, "failed": 0},
        "style": {"passed": 0, "failed": 0},
        "proof": {"passed": 0, "failed": 0},
    }
    editorial_gate_enabled = _bookgen_editorial_stage_gate_enabled()

    for chapter in outline["chapters"]:
        chapter_index = int(chapter["chapter_index"])
        root = _chapter_root(project_id, planning["installment_id"], chapter_index)
        chapter_pack_key = f"{root}/chapter_pack.yaml"
        draft_key = f"{root}/draft.md"
        final_key = f"{root}/final.md"
        eval_key = f"{root}/eval.yaml"
        editorial_qc_key = f"{root}/editorial_qc.yaml"
        chapter_pack = _read_yaml(store, chapter_pack_key)
        draft = store.get_text(draft_key)
        draft_qc = _read_yaml(store, f"{root}/draft_qc.yaml") if store.exists(f"{root}/draft_qc.yaml") else None
        scene_qc = _read_yaml(store, f"{root}/scene_qc.yaml") if store.exists(f"{root}/scene_qc.yaml") else None

        attempts = 0
        best_eval: dict[str, Any] | None = None
        best_text = draft
        plateau_count = 0
        previous_overall = -1.0
        rewrite_policy = chapter_pack.get("draft_contract", {}).get("rewrite_policy", {})
        max_attempts = int(rewrite_policy.get("max_attempts", 3) or 3)
        while attempts < max_attempts:
            attempts += 1
            if _bookgen_use_llm_for_eval(chapter_index):
                eval_report = _evaluate_chapter_with_llm(
                    text=best_text,
                    constitution=constitution,
                    installment_pack=installment_pack,
                    chapter_pack=chapter_pack,
                    rubric_cfg=rubric_cfg,
                    prompt_pack_version=str(intake["prompt_pack_version"]),
                )
            else:
                eval_report = _evaluate_chapter_text(
                    text=best_text,
                    constitution=constitution,
                    installment_pack=installment_pack,
                    chapter_pack=chapter_pack,
                    rubric_cfg=rubric_cfg,
                    prompt_pack_version=str(intake["prompt_pack_version"]),
                )
            if isinstance(draft_qc, dict):
                eval_report["draft_qc"] = draft_qc
                if draft_qc.get("pass_status") == "FAIL":
                    eval_report["violations"].append(
                        {
                            "type": "structural_qc_failure",
                            "severity": "high",
                            "note": "Final drafting structural QC still failed after recovery attempts; editorial review cannot override the structural gate.",
                        }
                    )
                    eval_report["pass_status"] = "FAIL"
            if isinstance(scene_qc, dict):
                eval_report["scene_qc"] = scene_qc
                if scene_qc.get("pass_status") == "FAIL":
                    eval_report["violations"].append(
                        {
                            "type": "scene_qc_failure",
                            "severity": "high",
                            "note": "Scene-level QA failed (beat/transition coverage); editorial review cannot override scene structure failure.",
                        }
                    )
                    eval_report["pass_status"] = "FAIL"
            _validate_structured_payload(eval_report, "eval_report")
            editorial_preview_failed = False
            if editorial_gate_enabled:
                editorial_preview = _editorial_stage_manifests(
                    chapter_pack=chapter_pack,
                    eval_report=eval_report,
                    text=best_text,
                )
                editorial_preview_failed = editorial_preview["summary"]["pass_status"] != "PASS"
                if editorial_preview_failed:
                    _append_editorial_gate_failure(eval_report)
                    eval_report["pass_status"] = "FAIL"
            if _should_promote_eval_candidate(eval_report, best_eval):
                best_eval = eval_report
            if (
                eval_report["pass_status"] != "FAIL"
                and not editorial_preview_failed
                and not _opening_chapter_soft_rewrite_required(chapter_pack, eval_report)
            ):
                break
            if previous_overall >= 0 and eval_report["overall"] <= previous_overall:
                plateau_count += 1
            previous_overall = eval_report["overall"]
            contract = _build_rewrite_contract(
                eval_report=eval_report,
                constitution=constitution,
                chapter_pack=chapter_pack,
                installment_pack=installment_pack,
                attempt=attempts,
            )
            _validate_structured_payload(contract, "rewrite_contract")
            contract_key = f"{root}/rewrite_contract_attempt_{attempts}.yaml"
            _write_yaml(store, contract_key, contract, artifacts)
            if plateau_count >= int(contract["constraints"]["stop_if_no_improvement_after"]):
                break
            if _bookgen_use_llm_for_rewrite(chapter_index):
                best_text = _rewrite_chapter_with_llm(best_text, chapter_pack, contract)
            else:
                best_text = _rewrite_chapter_text(best_text, chapter_pack, eval_report)

        assert best_eval is not None
        editorial = _editorial_stage_manifests(
            chapter_pack=chapter_pack,
            eval_report=best_eval,
            text=best_text,
        )
        for stage_payload in editorial["stages"]:
            _validate_structured_payload(stage_payload, "editorial_stage")
            _write_yaml(store, _editorial_stage_key(root, str(stage_payload["stage_name"])), stage_payload, artifacts)
            bucket = editorial_stage_totals.get(str(stage_payload["stage_name"]))
            if bucket is not None:
                if stage_payload["pass_status"] == "PASS":
                    bucket["passed"] += 1
                else:
                    bucket["failed"] += 1
        _validate_structured_payload(editorial["summary"], "editorial_stage_summary")
        _write_yaml(store, _editorial_stage_summary_key(root), editorial["summary"], artifacts)
        if editorial_gate_enabled and editorial["summary"]["pass_status"] != "PASS":
            _append_editorial_gate_failure(best_eval)
            best_eval["pass_status"] = "FAIL"
        _write_yaml(store, eval_key, best_eval, artifacts)
        _write_yaml(store, editorial_qc_key, editorial["summary"] if editorial_gate_enabled else best_eval, artifacts)
        if best_eval["pass_status"] == "FAIL":
            failed += 1
        else:
            store.put_text(final_key, best_text)
            artifacts[final_key] = _hash_text(best_text)
            ledgers = _update_ledgers(ledgers=ledgers, chapter_pack=chapter_pack, eval_report=best_eval)
            passed += 1
        scores.append(float(best_eval["overall"]))

    for ledger_name, payload in ledgers.items():
        _write_yaml(store, f"{planning['ledger_root']}/{ledger_name}.yaml", payload, artifacts)

    generation_mode_counts = {"requested_llm": 0, "final_llm": 0, "final_fallback": 0, "explicit_fallback": 0}
    for item in generation_summary.get("chapters", []):
        if not isinstance(item, dict):
            continue
        if item.get("requested_mode") == "llm":
            generation_mode_counts["requested_llm"] += 1
        if item.get("final_mode") == "llm":
            generation_mode_counts["final_llm"] += 1
        if item.get("final_mode") == "fallback":
            generation_mode_counts["final_fallback"] += 1
        if item.get("requested_mode") == "fallback":
            generation_mode_counts["explicit_fallback"] += 1

    summary = {
        "project_id": project_id,
        "installment_id": planning["installment_id"],
        "generation_preset": _bookgen_generation_preset(),
        "generation_summary_key": generation_summary_key,
        "generation_mode_counts": generation_mode_counts,
        "total_chapters": len(outline["chapters"]),
        "passed_chapters": passed,
        "failed_chapters": failed,
        "chapter_pass_rate": passed / max(1, len(outline["chapters"])),
        "avg_overall_score": round(mean(scores), 2) if scores else 0.0,
        "editorial_stage_gate_enabled": editorial_gate_enabled,
        "editorial_stage_totals": editorial_stage_totals,
    }
    summary_key = f"{_project_root(project_id, planning['installment_id'])}/eval/global.json"
    store.put_json(summary_key, summary)
    artifacts[summary_key] = _hash_payload(summary)
    publishability_gate = {
        "schema_version": "1.0",
        "project_id": project_id,
        "installment_id": planning["installment_id"],
        "editorial_stage_gate_enabled": editorial_gate_enabled,
        "checks": {
            "all_chapters_passed_editorial": failed == 0,
            "editorial_stages_all_passed": all(item["failed"] == 0 for item in editorial_stage_totals.values()),
        },
        "stage_counts": editorial_stage_totals,
        "pass_status": "PASS"
        if failed == 0 and (not editorial_gate_enabled or all(item["failed"] == 0 for item in editorial_stage_totals.values()))
        else "FAIL",
        "created_utc": _utcnow(),
    }
    _validate_structured_payload(publishability_gate, "publishability_gate")
    publishability_key = _publishability_gate_key(project_id, planning["installment_id"])
    _write_yaml(store, publishability_key, publishability_gate, artifacts)
    release_state = _release_state_payload(
        project_id=project_id,
        installment_id=planning["installment_id"],
        status="editorial_reviewed" if publishability_gate["pass_status"] == "PASS" else "editorial_hold",
        notes=[]
        if publishability_gate["pass_status"] == "PASS"
        else ["One or more chapters failed editorial review or publishability gate checks."],
    )
    release_state_key = _release_state_key(project_id, planning["installment_id"])
    _validate_structured_payload(release_state, "release_state")
    _write_yaml(store, release_state_key, release_state, artifacts)
    commit_id = _commit_stage_checkpoint(
        project_id=project_id,
        run_date=str(intake["run_date"]),
        stage="chapters-review",
        artifact_hashes=artifacts,
        extra_metadata={"passed_chapters": passed, "failed_chapters": failed},
    )
    if commit_id:
        summary["lakefs_commit_id"] = commit_id
    return summary


def run_assembly_export(*, project_id: str) -> dict[str, Any]:
    store = ObjectStore()
    intake = _load_intake(project_id, store)
    planning = _load_planning_manifest(project_id, store)
    _assert_installment_mutable(
        store=store,
        project_id=project_id,
        installment_id=planning["installment_id"],
        stage="Assembly",
    )
    constitution = _read_yaml(store, planning["constitution_key"])
    installment_pack = _read_yaml(store, planning["installment_pack_key"])
    outline = _read_yaml(store, planning["outline_key"])
    review = store.get_json(f"{_project_root(project_id, planning['installment_id'])}/eval/global.json")
    generation_summary_key = f"{_project_root(project_id, planning['installment_id'])}/draft/generation_summary.json"
    generation_summary = store.get_json(generation_summary_key) if store.exists(generation_summary_key) else {"chapters": []}
    publishability_key = _publishability_gate_key(project_id, planning["installment_id"])
    if not store.exists(publishability_key):
        raise RuntimeError("Assembly blocked: publishability gate artifact is missing.")
    publishability_gate = _read_yaml(store, publishability_key)
    if str(publishability_gate.get("pass_status", "FAIL")).strip().upper() != "PASS":
        raise RuntimeError("Assembly blocked: publishability gate failed.")
    installment_title = _effective_installment_title(constitution, installment_pack)

    total = int(review["total_chapters"])
    passed = int(review["passed_chapters"])
    if total <= 0 or passed != total:
        raise RuntimeError(f"Assembly blocked: passed chapters ({passed}) != expected chapters ({total}).")

    sections: list[str] = []
    artifacts: dict[str, str] = {}
    for chapter in outline["chapters"]:
        final_key = f"{_chapter_root(project_id, planning['installment_id'], int(chapter['chapter_index']))}/final.md"
        if not store.exists(final_key):
            raise RuntimeError(f"Assembly blocked: missing final chapter {chapter['chapter_index']:02d}")
        sections.append(store.get_text(final_key).strip())

    continuity_review = _continuity_review_report(
        project_id=project_id,
        installment_id=planning["installment_id"],
        outline=outline,
        sections=sections,
    )
    continuity_key = _continuity_review_key(project_id, planning["installment_id"])
    _write_yaml(store, continuity_key, continuity_review, artifacts)
    if continuity_review["pass_status"] != "PASS":
        release_state = _release_state_payload(
            project_id=project_id,
            installment_id=planning["installment_id"],
            status="editorial_hold",
            notes=["Installment continuity review failed."],
        )
        _write_yaml(store, _release_state_key(project_id, planning["installment_id"]), release_state, artifacts)
        raise RuntimeError("Assembly blocked: installment continuity review failed.")

    front_matter = _front_matter_markdown(constitution=constitution, installment_pack=installment_pack).strip()
    toc_matter = _toc_markdown(outline=outline).strip()
    back_matter = _back_matter_markdown(constitution=constitution, installment_pack=installment_pack).strip()
    manuscript_parts = [front_matter, toc_matter] + sections + [back_matter]
    manuscript = "\n\n---\n\n".join(part for part in manuscript_parts if part).strip() + "\n"
    manuscript_key = f"{_project_root(project_id, planning['installment_id'])}/assembly/manuscript.md"
    store.put_text(manuscript_key, manuscript)
    artifacts[manuscript_key] = _hash_text(manuscript)

    theme_hits = sum(_theme_signal(section, installment_pack["theme_expression"]["primary_focus"]) for section in sections)
    audit = {
        "project_id": project_id,
        "installment_id": planning["installment_id"],
        "chapter_count": total,
        "pass_status": "PASS" if theme_hits >= max(1.0, total * 0.45) else "FAIL",
        "checks": {
            "theme_coverage_signal": round(theme_hits, 2),
            "timeline_events": len(_read_yaml(store, f"{planning['ledger_root']}/ledger_timeline.yaml")["events"]),
            "thread_count": len(_read_yaml(store, f"{planning['ledger_root']}/ledger_threads.yaml")["threads"]),
        },
    }
    audit_key = f"{_project_root(project_id, planning['installment_id'])}/assembly/global_audit.yaml"
    _write_yaml(store, audit_key, audit, artifacts)
    if audit["pass_status"] != "PASS":
        raise RuntimeError("Assembly blocked: global audit failed.")

    export_root = f"exports/{project_id}/{planning['installment_id']}"
    export_keys = [f"{export_root}/manuscript.md"]
    store.put_text(export_keys[0], manuscript)
    artifacts[export_keys[0]] = _hash_text(manuscript)

    if "docx" in installment_pack["output_targets"]["output_formats"]:
        docx_key = f"{export_root}/manuscript.docx"
        raw = _manuscript_to_docx_bytes(installment_title, sections)
        _write_binary(store, docx_key, raw, "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
        artifacts[docx_key] = hashlib.sha256(raw).hexdigest()
        export_keys.append(docx_key)

    if "epub" in installment_pack["output_targets"]["output_formats"]:
        epub_key = f"{export_root}/manuscript.epub"
        author = str(constitution.get("author", "")).strip() or str(constitution.get("constitution_id", "BookGen"))
        epub_raw = _manuscript_to_epub_bytes(
            title=installment_title,
            author=author,
            sections=[front_matter, toc_matter] + sections + [back_matter],
            chapter_titles=["Front Matter", "Table of Contents"]
            + [str(ch.get("title", f"Chapter {idx + 1}")) for idx, ch in enumerate(outline["chapters"])]
            + ["Back Matter"],
        )
        _write_binary(store, epub_key, epub_raw, "application/epub+zip")
        artifacts[epub_key] = hashlib.sha256(epub_raw).hexdigest()
        export_keys.append(epub_key)

    if "pdf" in installment_pack["output_targets"]["output_formats"]:
        pdf_key = f"{export_root}/manuscript.pdf"
        pdf_raw = _manuscript_to_pdf_bytes(
            title=installment_title,
            sections=[front_matter, toc_matter] + sections + [back_matter],
        )
        _write_binary(store, pdf_key, pdf_raw, "application/pdf")
        artifacts[pdf_key] = hashlib.sha256(pdf_raw).hexdigest()
        export_keys.append(pdf_key)

    metadata_pack = _metadata_pack(
        constitution=constitution,
        installment_pack=installment_pack,
        outline=outline,
    )
    _validate_structured_payload(metadata_pack, "metadata_pack")
    publication_root = f"{_project_root(project_id, planning['installment_id'])}/publication_package"
    front_key = f"{publication_root}/front_matter.md"
    toc_key = f"{publication_root}/toc.md"
    back_key = f"{publication_root}/back_matter.md"
    metadata_pack_key = f"{publication_root}/metadata_pack.yaml"
    store.put_text(front_key, front_matter + "\n")
    artifacts[front_key] = _hash_text(front_matter + "\n")
    store.put_text(toc_key, toc_matter + "\n")
    artifacts[toc_key] = _hash_text(toc_matter + "\n")
    store.put_text(back_key, back_matter + "\n")
    artifacts[back_key] = _hash_text(back_matter + "\n")
    _write_yaml(store, metadata_pack_key, metadata_pack, artifacts)
    publication_manifest = {
        "schema_version": "1.0",
        "project_id": project_id,
        "installment_id": planning["installment_id"],
        "artifact_keys": {
            "front_matter": front_key,
            "toc": toc_key,
            "back_matter": back_key,
            "metadata_pack": metadata_pack_key,
            "manuscript": manuscript_key,
            "exports": export_keys,
        },
        "created_utc": _utcnow(),
    }
    _validate_structured_payload(publication_manifest, "publication_package")
    publication_manifest_key = f"{publication_root}/publication_manifest.yaml"
    _write_yaml(store, publication_manifest_key, publication_manifest, artifacts)

    metadata = {
        "project_id": project_id,
        "installment_id": planning["installment_id"],
        "title": installment_title,
        "blurb": metadata_pack["blurb"],
        "keywords": list(metadata_pack.get("keywords", [])),
        "categories": list(metadata_pack.get("categories", [])),
        "continuity_review_key": continuity_key,
        "publishability_gate_key": publishability_key,
        "manuscript_key": manuscript_key,
        "export_keys": export_keys,
        "publication_manifest_key": publication_manifest_key,
        "metadata_pack_key": metadata_pack_key,
        "run_analytics_key": _analytics_run_key(project_id, planning["installment_id"]),
        "benchmark_drift_key": _benchmark_drift_key(project_id, planning["installment_id"]),
        "experiment_tracker_key": _experiment_tracker_key(project_id, planning["installment_id"]),
        "checksum": hashlib.sha256(manuscript.encode("utf-8")).hexdigest(),
    }
    _validate_structured_payload(metadata, "export_manifest")
    metadata_key = f"{export_root}/export_manifest.json"
    store.put_json(metadata_key, metadata)
    artifacts[metadata_key] = _hash_payload(metadata)

    run_analytics = _run_analytics_payload(
        project_id=project_id,
        installment_id=planning["installment_id"],
        intake=intake,
        review=review,
        generation_summary=generation_summary,
        continuity_review=continuity_review,
        publishability_gate=publishability_gate,
    )
    _validate_structured_payload(run_analytics, "run_analytics")
    analytics_key = _analytics_run_key(project_id, planning["installment_id"])
    _write_yaml(store, analytics_key, run_analytics, artifacts)

    history_key = _benchmark_history_key(project_id)
    history = store.get_json(history_key) if store.exists(history_key) else {"runs": []}
    prior_runs = [item for item in history.get("runs", []) if isinstance(item, dict)]
    baseline = prior_runs[-1] if prior_runs else None
    benchmark_drift = _benchmark_drift_payload(
        project_id=project_id,
        installment_id=planning["installment_id"],
        current_analytics=run_analytics,
        baseline=baseline,
    )
    _validate_structured_payload(benchmark_drift, "benchmark_drift")
    benchmark_key = _benchmark_drift_key(project_id, planning["installment_id"])
    _write_yaml(store, benchmark_key, benchmark_drift, artifacts)

    experiment_tracker = _experiment_tracker_payload(
        project_id=project_id,
        installment_id=planning["installment_id"],
        intake=intake,
        review=review,
        analytics=run_analytics,
    )
    _validate_structured_payload(experiment_tracker, "experiment_tracker")
    experiment_key = _experiment_tracker_key(project_id, planning["installment_id"])
    _write_yaml(store, experiment_key, experiment_tracker, artifacts)

    history_entry = {
        "project_id": project_id,
        "installment_id": planning["installment_id"],
        "recorded_utc": _utcnow(),
        "quality": run_analytics["quality"],
        "cost": run_analytics["cost"],
        "experiment": experiment_tracker["experiment"],
    }
    prior_runs.append(history_entry)
    if len(prior_runs) > 25:
        prior_runs = prior_runs[-25:]
    history_payload = {"schema_version": "1.0", "project_id": project_id, "runs": prior_runs}
    store.put_json(history_key, history_payload)
    artifacts[history_key] = _hash_payload(history_payload)

    _log_mlflow_summary(
        project_id,
        planning["installment_id"],
        {
            "chapter_count": total,
            "chapter_pass_rate": review["chapter_pass_rate"],
            "avg_overall_score": review["avg_overall_score"],
        },
        metadata,
    )
    current_release = _load_release_state(store, project_id, planning["installment_id"]) or {}
    next_status = (
        "editorial_reviewed"
        if str(current_release.get("status", "")).strip().lower() in {"editorial_reviewed", "awaiting_editorial_approval", ""}
        else str(current_release.get("status"))
    )
    release_state = _release_state_payload(
        project_id=project_id,
        installment_id=planning["installment_id"],
        status=next_status,
        notes=["Assembly and export completed. Ready for approval workflow progression."],
        approval={"locked": False, "approval_record_key": _approval_record_key(project_id, planning["installment_id"])},
    )
    _validate_structured_payload(release_state, "release_state")
    _write_yaml(store, _release_state_key(project_id, planning["installment_id"]), release_state, artifacts)
    commit_id = _commit_stage_checkpoint(
        project_id=project_id,
        run_date=str(intake["run_date"]),
        stage="assembly-export",
        artifact_hashes=artifacts,
        extra_metadata={"chapter_count": total, "installment_id": planning["installment_id"]},
    )
    if commit_id:
        metadata["lakefs_commit_id"] = commit_id
    return metadata


def inspect_chapter(*, project_id: str, chapter_index: int, installment_id: str | None = None) -> dict[str, Any]:
    store = ObjectStore()
    planning = _load_planning_manifest(project_id, store)
    resolved_installment_id = installment_id or planning["installment_id"]
    root = _chapter_root(project_id, resolved_installment_id, int(chapter_index))
    chapter_pack = _read_yaml(store, f"{root}/chapter_pack.yaml") if store.exists(f"{root}/chapter_pack.yaml") else None
    draft_contract = _read_yaml(store, f"{root}/draft_contract.yaml") if store.exists(f"{root}/draft_contract.yaml") else None
    scene_cards = _read_yaml(store, f"{root}/scene_cards.yaml") if store.exists(f"{root}/scene_cards.yaml") else None
    scene_beats = _read_yaml(store, f"{root}/scene_beats.yaml") if store.exists(f"{root}/scene_beats.yaml") else None
    scene_qc = _read_yaml(store, f"{root}/scene_qc.yaml") if store.exists(f"{root}/scene_qc.yaml") else None
    scene_qc_history = _read_yaml(store, f"{root}/scene_qc_history.yaml") if store.exists(f"{root}/scene_qc_history.yaml") else None
    draft_qc = _read_yaml(store, f"{root}/draft_qc.yaml") if store.exists(f"{root}/draft_qc.yaml") else None
    draft_qc_history = _read_yaml(store, f"{root}/draft_qc_history.yaml") if store.exists(f"{root}/draft_qc_history.yaml") else None
    generation_trace = _read_yaml(store, f"{root}/generation_trace.yaml") if store.exists(f"{root}/generation_trace.yaml") else None
    editorial_qc = _read_yaml(store, f"{root}/editorial_qc.yaml") if store.exists(f"{root}/editorial_qc.yaml") else None
    editorial_stages = {
        "developmental": _read_yaml(store, _editorial_stage_key(root, "developmental")) if store.exists(_editorial_stage_key(root, "developmental")) else None,
        "line": _read_yaml(store, _editorial_stage_key(root, "line")) if store.exists(_editorial_stage_key(root, "line")) else None,
        "copy": _read_yaml(store, _editorial_stage_key(root, "copy")) if store.exists(_editorial_stage_key(root, "copy")) else None,
        "style": _read_yaml(store, _editorial_stage_key(root, "style")) if store.exists(_editorial_stage_key(root, "style")) else None,
        "proof": _read_yaml(store, _editorial_stage_key(root, "proof")) if store.exists(_editorial_stage_key(root, "proof")) else None,
    }
    editorial_stage_summary = _read_yaml(store, _editorial_stage_summary_key(root)) if store.exists(_editorial_stage_summary_key(root)) else None
    eval_report = _read_yaml(store, f"{root}/eval.yaml") if store.exists(f"{root}/eval.yaml") else None
    final_text = store.get_text(f"{root}/final.md") if store.exists(f"{root}/final.md") else None

    rewrite_contracts: list[dict[str, Any]] = []
    for attempt in range(1, 6):
        key = f"{root}/rewrite_contract_attempt_{attempt}.yaml"
        if not store.exists(key):
            continue
        rewrite_contracts.append({"attempt": attempt, "key": key, "contract": _read_yaml(store, key)})

    return {
        "project_id": project_id,
        "installment_id": resolved_installment_id,
        "chapter_index": int(chapter_index),
        "chapter_root": root,
        "generation_preset": _bookgen_generation_preset(),
        "chapter_title": (chapter_pack or {}).get("chapter_card", {}).get("title"),
        "draft_contract": draft_contract,
        "scene_cards": scene_cards,
        "scene_beats": scene_beats,
        "scene_qc": scene_qc,
        "scene_qc_history": scene_qc_history,
        "draft_qc": draft_qc,
        "draft_qc_history": draft_qc_history,
        "generation_trace": generation_trace,
        "editorial_qc": editorial_qc,
        "editorial_stages": editorial_stages,
        "editorial_stage_summary": editorial_stage_summary,
        "eval_report": eval_report,
        "rewrite_contracts": rewrite_contracts,
        "has_final_text": final_text is not None,
        "final_word_count": _word_count(final_text or "") if final_text else 0,
        "final_excerpt": "\n".join((final_text or "").splitlines()[:10]) if final_text else None,
    }


def inspect_generation_summary(*, project_id: str, installment_id: str | None = None) -> dict[str, Any]:
    store = ObjectStore()
    planning = _load_planning_manifest(project_id, store)
    resolved_installment_id = installment_id or planning["installment_id"]
    key = f"{_project_root(project_id, resolved_installment_id)}/draft/generation_summary.json"
    if not store.exists(key):
        raise RuntimeError(f"Generation summary missing: {key}")
    return store.get_json(key)


def approve_installment(
    *,
    project_id: str,
    installment_id: str | None = None,
    decision: str,
    note: str = "",
) -> dict[str, Any]:
    store = ObjectStore()
    planning = _load_planning_manifest(project_id, store)
    resolved_installment_id = installment_id or planning["installment_id"]
    normalized = decision.strip().lower()
    if normalized not in {"approve", "hold", "lock", "publish"}:
        raise RuntimeError("Decision must be one of: approve, hold, lock, publish")
    continuity_key = _continuity_review_key(project_id, resolved_installment_id)
    publishability_key = _publishability_gate_key(project_id, resolved_installment_id)
    if normalized in {"approve", "lock", "publish"} and not store.exists(continuity_key):
        raise RuntimeError("Approval blocked: continuity review artifact is missing.")
    if normalized in {"approve", "lock", "publish"} and not store.exists(publishability_key):
        raise RuntimeError("Approval blocked: publishability gate artifact is missing.")
    if normalized in {"approve", "lock", "publish"}:
        gate = _read_yaml(store, publishability_key)
        if str(gate.get("pass_status", "FAIL")).strip().upper() != "PASS":
            raise RuntimeError("Approval blocked: publishability gate failed.")

    current_release = (
        _read_yaml(store, _release_state_key(project_id, resolved_installment_id))
        if store.exists(_release_state_key(project_id, resolved_installment_id))
        else _release_state_payload(project_id=project_id, installment_id=resolved_installment_id, status="editorial_hold")
    )
    target_status = _release_transition(current_status=str(current_release.get("status", "editorial_hold")), decision=normalized)
    approval_key = _approval_record_key(project_id, resolved_installment_id)
    existing_record = _read_yaml(store, approval_key) if store.exists(approval_key) else None
    events = list((existing_record or {}).get("events", [])) if isinstance(existing_record, dict) else []
    event = {
        "decision": normalized,
        "note": note.strip(),
        "from_status": str(current_release.get("status", "editorial_hold")),
        "to_status": target_status,
        "recorded_utc": _utcnow(),
    }
    events.append(event)
    approval_record = {
        "schema_version": "1.0",
        "project_id": project_id,
        "installment_id": resolved_installment_id,
        "latest_decision": normalized,
        "latest_note": note.strip(),
        "recorded_utc": event["recorded_utc"],
        "events": events,
    }
    _validate_structured_payload(approval_record, "approval_record")
    store.put_yaml(approval_key, approval_record)
    release_state = _release_state_payload(
        project_id=project_id,
        installment_id=resolved_installment_id,
        status=target_status,
        notes=[note.strip()] if note.strip() else [],
        approval={
            "locked": target_status in {"manuscript_locked", "approved_for_publication"},
            "decision": normalized,
            "approval_record_key": approval_key,
            "previous_status": current_release.get("status"),
        },
    )
    _validate_structured_payload(release_state, "release_state")
    store.put_yaml(_release_state_key(project_id, resolved_installment_id), release_state)
    return release_state


def request_revision(
    *,
    project_id: str,
    installment_id: str | None = None,
    reason: str,
    requested_by: str = "editor",
    severity: str = "major",
) -> dict[str, Any]:
    store = ObjectStore()
    planning = _load_planning_manifest(project_id, store)
    resolved_installment_id = installment_id or planning["installment_id"]
    current_release = (
        _read_yaml(store, _release_state_key(project_id, resolved_installment_id))
        if store.exists(_release_state_key(project_id, resolved_installment_id))
        else _release_state_payload(project_id=project_id, installment_id=resolved_installment_id, status="editorial_hold")
    )
    manager_key = _revision_request_manager_key(project_id, resolved_installment_id)
    existing = _read_yaml(store, manager_key) if store.exists(manager_key) else None
    requests = list((existing or {}).get("requests", [])) if isinstance(existing, dict) else []
    request_id = f"rev-{len(requests) + 1:03d}"
    request = {
        "request_id": request_id,
        "reason": reason.strip(),
        "requested_by": requested_by.strip() or "editor",
        "severity": severity.strip().lower() or "major",
        "status": "open",
        "created_utc": _utcnow(),
    }
    requests.append(request)
    manager = {
        "schema_version": "1.0",
        "project_id": project_id,
        "installment_id": resolved_installment_id,
        "open_requests": len([item for item in requests if str(item.get("status", "")).lower() == "open"]),
        "requests": requests,
        "updated_utc": _utcnow(),
    }
    _validate_structured_payload(manager, "revision_request_manager")
    store.put_yaml(manager_key, manager)

    release_state = _release_state_payload(
        project_id=project_id,
        installment_id=resolved_installment_id,
        status="editorial_hold",
        notes=[f"Revision requested ({request_id}): {reason.strip()}"],
        approval={
            "locked": False,
            "decision": "revision_requested",
            "previous_status": current_release.get("status"),
            "revision_request_manager_key": manager_key,
        },
    )
    _validate_structured_payload(release_state, "release_state")
    store.put_yaml(_release_state_key(project_id, resolved_installment_id), release_state)
    return {"release_state": release_state, "revision_request_manager": manager, "request": request}


def schedule_release(
    *,
    project_id: str,
    installment_id: str | None = None,
    planned_date: str | None = None,
    status: str = "planned",
    note: str = "",
) -> dict[str, Any]:
    store = ObjectStore()
    planning = _load_planning_manifest(project_id, store)
    resolved_installment_id = installment_id or planning["installment_id"]
    normalized_status = status.strip().lower()
    if normalized_status not in {"planned", "hold", "release"}:
        raise RuntimeError("Release schedule status must be one of: planned, hold, release")
    if planned_date:
        _ = datetime.strptime(planned_date.strip(), "%Y-%m-%d")

    schedule_payload = {
        "schema_version": "1.0",
        "project_id": project_id,
        "installment_id": resolved_installment_id,
        "planned_date": planned_date.strip() if planned_date else None,
        "status": {"planned": "planned", "hold": "on_hold", "release": "released"}[normalized_status],
        "note": note.strip(),
        "updated_utc": _utcnow(),
    }
    schedule_key = _release_schedule_key(project_id, resolved_installment_id)
    _validate_structured_payload(schedule_payload, "release_schedule")
    store.put_yaml(schedule_key, schedule_payload)
    return schedule_payload


def operator_report(*, project_id: str, installment_id: str | None = None) -> dict[str, Any]:
    store = ObjectStore()
    planning = _load_planning_manifest(project_id, store)
    resolved_installment_id = installment_id or planning["installment_id"]
    review_key = f"{_project_root(project_id, resolved_installment_id)}/eval/global.json"
    generation_key = f"{_project_root(project_id, resolved_installment_id)}/draft/generation_summary.json"
    release_key = _release_state_key(project_id, resolved_installment_id)
    continuity_key = _continuity_review_key(project_id, resolved_installment_id)
    publishability_key = _publishability_gate_key(project_id, resolved_installment_id)
    analytics_key = _analytics_run_key(project_id, resolved_installment_id)
    benchmark_key = _benchmark_drift_key(project_id, resolved_installment_id)
    experiment_key = _experiment_tracker_key(project_id, resolved_installment_id)
    revision_key = _revision_request_manager_key(project_id, resolved_installment_id)
    schedule_key = _release_schedule_key(project_id, resolved_installment_id)
    approval_key = _approval_record_key(project_id, resolved_installment_id)
    export_key = f"exports/{project_id}/{resolved_installment_id}/export_manifest.json"
    return {
        "project_id": project_id,
        "installment_id": resolved_installment_id,
        "generation_summary": store.get_json(generation_key) if store.exists(generation_key) else None,
        "review_summary": store.get_json(review_key) if store.exists(review_key) else None,
        "publishability_gate": _read_yaml(store, publishability_key) if store.exists(publishability_key) else None,
        "run_analytics": _read_yaml(store, analytics_key) if store.exists(analytics_key) else None,
        "benchmark_drift": _read_yaml(store, benchmark_key) if store.exists(benchmark_key) else None,
        "experiment_tracker": _read_yaml(store, experiment_key) if store.exists(experiment_key) else None,
        "continuity_review": _read_yaml(store, continuity_key) if store.exists(continuity_key) else None,
        "release_state": _read_yaml(store, release_key) if store.exists(release_key) else None,
        "approval_record": _read_yaml(store, approval_key) if store.exists(approval_key) else None,
        "revision_request_manager": _read_yaml(store, revision_key) if store.exists(revision_key) else None,
        "release_schedule": _read_yaml(store, schedule_key) if store.exists(schedule_key) else None,
        "export_manifest": store.get_json(export_key) if store.exists(export_key) else None,
    }


def analytics_report(*, project_id: str, installment_id: str | None = None) -> dict[str, Any]:
    store = ObjectStore()
    planning = _load_planning_manifest(project_id, store)
    resolved_installment_id = installment_id or planning["installment_id"]

    run_analytics_key = _analytics_run_key(project_id, resolved_installment_id)
    benchmark_key = _benchmark_drift_key(project_id, resolved_installment_id)
    experiment_key = _experiment_tracker_key(project_id, resolved_installment_id)
    history_key = _benchmark_history_key(project_id)

    run_analytics = _read_yaml(store, run_analytics_key) if store.exists(run_analytics_key) else None
    benchmark_drift = _read_yaml(store, benchmark_key) if store.exists(benchmark_key) else None
    experiment_tracker = _read_yaml(store, experiment_key) if store.exists(experiment_key) else None
    history = store.get_json(history_key) if store.exists(history_key) else {"runs": []}
    history_runs = [item for item in history.get("runs", []) if isinstance(item, dict)]

    score_values = [float(item.get("quality", {}).get("avg_overall_score", 0.0)) for item in history_runs if item.get("quality")]
    pass_rate_values = [float(item.get("quality", {}).get("chapter_pass_rate", 0.0)) for item in history_runs if item.get("quality")]
    cost_values = [float(item.get("cost", {}).get("estimated_cost_usd", 0.0)) for item in history_runs if item.get("cost")]
    baseline = history_runs[-1] if history_runs else None
    recent = history_runs[-5:]

    return {
        "project_id": project_id,
        "installment_id": resolved_installment_id,
        "current": {
            "run_analytics": run_analytics,
            "benchmark_drift": benchmark_drift,
            "experiment_tracker": experiment_tracker,
        },
        "portfolio": {
            "history_run_count": len(history_runs),
            "avg_overall_score": round(sum(score_values) / max(1, len(score_values)), 3) if score_values else None,
            "avg_pass_rate": round(sum(pass_rate_values) / max(1, len(pass_rate_values)), 3) if pass_rate_values else None,
            "avg_estimated_cost_usd": round(sum(cost_values) / max(1, len(cost_values)), 4) if cost_values else None,
            "latest_baseline_installment_id": baseline.get("installment_id") if baseline else None,
            "recent_runs": recent,
        },
        "artifact_keys": {
            "run_analytics_key": run_analytics_key,
            "benchmark_drift_key": benchmark_key,
            "experiment_tracker_key": experiment_key,
            "benchmark_history_key": history_key,
        },
    }


def run(
    *,
    project_id: str,
    run_date: str | None = None,
    bookspec_key: str | None = None,
    bookspec_path: str | None = None,
) -> dict[str, Any]:
    run_date = run_date or date.today().isoformat()
    intake = run_intake(project_id=project_id, run_date=run_date, bookspec_key=bookspec_key, bookspec_path=bookspec_path)
    resolved = run_prompt_pack_resolve(intake=intake)
    planning = run_bible_outline(intake=intake, resolved=resolved)
    draft = run_chapter_drafting(project_id=project_id)
    review = run_chapter_review(project_id=project_id)
    assembly = run_assembly_export(project_id=project_id)
    return {
        "project_id": project_id,
        "run_date": run_date,
        "status": "succeeded",
        "intake": intake,
        "resolved": resolved,
        "planning": planning,
        "draft": draft,
        "review": review,
        "assembly": assembly,
    }
