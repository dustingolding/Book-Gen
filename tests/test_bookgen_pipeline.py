from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from app.services import bookgen


class _MemoryClient:
    def __init__(self, data: dict[str, bytes]) -> None:
        self._data = data

    def put_object(self, bucket: str, key: str, data, length: int, content_type: str) -> None:
        del bucket, content_type
        self._data[key] = data.read(length)


class MemoryStore:
    def __init__(self, data: dict[str, bytes]) -> None:
        self._data = data
        self.bucket = "test"
        self.client = _MemoryClient(data)

    def put_json(self, key: str, payload: dict) -> None:
        self._data[key] = json.dumps(payload).encode("utf-8")

    def get_json(self, key: str) -> dict:
        return json.loads(self._data[key].decode("utf-8"))

    def put_yaml(self, key: str, payload: dict) -> None:
        self._data[key] = yaml.safe_dump(payload, sort_keys=False).encode("utf-8")

    def get_yaml(self, key: str) -> dict:
        return yaml.safe_load(self._data[key].decode("utf-8"))

    def put_text(self, key: str, text: str, content_type: str = "text/plain") -> None:
        del content_type
        self._data[key] = text.encode("utf-8")

    def get_text(self, key: str) -> str:
        return self._data[key].decode("utf-8")

    def exists(self, key: str) -> bool:
        return key in self._data


def _closed_session_title_spec() -> dict:
    return {
        "project_id": "closed-session-series",
        "title": "The Closed Session",
        "series_title": "The Closed Session",
        "genre": "thriller",
        "subgenre": "contemporary institutional thriller",
        "target_audience": "adult",
        "tone": "Grounded, restrained, hyper-realistic political tension.",
        "themes": ["Power protects itself.", "Truth has collateral damage."],
        "word_count_target": 95000,
        "chapter_count": 24,
        "planned_series_length": 5,
        "installment_id": "book-01",
        "installment_index": 1,
        "pov": "third_limited",
        "protagonist_goal": "Expose corruption without triggering institutional collapse.",
        "core_conflict": "Every lead forces Adrian to choose between exposure and stability.",
        "stakes": "The most complete truth could fracture the system it is meant to save.",
        "narrative_role": "setup",
        "stakes_level": "high",
        "output_formats": ["md", "docx"],
        "characters": [
            {"name": "Adrian Cole", "role": "protagonist"},
            {"name": "Maya Rios", "role": "supporting"},
            {"name": "Thomas Avery", "role": "antagonist"},
        ],
        "setting_rules": [
            "Classified information cannot be publicly disclosed without severe consequences.",
            "Institutional processes slow scandal rather than accelerate it.",
        ],
        "prompt_pack_version": "v1",
        "rubric_version": "v1",
        "series_title_strategy": {
            "naming_family": "institutional procedural phrases",
            "tonal_requirements": ["serious", "politically credible", "restrained", "procedural"],
            "avoid": ["generic action thriller wording", "spy pulp phrasing", "overly metaphorical titles"],
            "lexical_banks": {
                "nouns": ["session", "record", "brief", "motion", "rule", "ledger", "order", "markup", "hearing", "file", "vote", "docket"],
                "adjectives": ["closed", "quiet", "redacted", "sealed", "public", "final", "classified", "hidden", "open", "silent"],
                "verbs_disallowed": ["hunt", "kill", "strike", "revenge"],
            },
        },
        "installment_title_briefs": [
            {
                "installment_id": "book-01",
                "installment_index": 1,
                "arc_role": "entry",
                "exposure_level": "low",
                "stakes_level": "high",
                "semantic_targets": {"must_imply": ["secrecy", "procedure", "first breach"], "must_avoid": ["finality", "apocalypse"]},
            },
            {
                "installment_id": "book-02",
                "installment_index": 2,
                "arc_role": "escalation",
                "exposure_level": "medium",
                "stakes_level": "high",
                "semantic_targets": {"must_imply": ["hidden record", "quiet leverage", "containment strain"], "must_avoid": ["final reckoning"]},
            },
            {
                "installment_id": "book-03",
                "installment_index": 3,
                "arc_role": "breach",
                "exposure_level": "medium",
                "stakes_level": "high",
                "semantic_targets": {"must_imply": ["formal action", "institutional crack", "public risk"], "must_avoid": ["resolution"]},
            },
            {
                "installment_id": "book-04",
                "installment_index": 4,
                "arc_role": "reckoning",
                "exposure_level": "high",
                "stakes_level": "high",
                "semantic_targets": {"must_imply": ["accountability", "formal confrontation", "irreversible record"], "must_avoid": ["entry-level secrecy"]},
            },
            {
                "installment_id": "book-05",
                "installment_index": 5,
                "arc_role": "exposure",
                "exposure_level": "high",
                "stakes_level": "high",
                "semantic_targets": {"must_imply": ["public consequence", "reckoning", "record"], "must_avoid": ["entry-level secrecy"]},
            },
        ],
    }


def _juvenile_series_spec() -> dict:
    spec = _closed_session_title_spec()
    spec["project_id"] = "time-tinkers-lab"
    spec["title"] = "The Rusty Machine"
    spec["series_title"] = "The Time-Tinkers' Lab"
    spec["genre"] = "juvenile fiction"
    spec["subgenre"] = "middle-grade STEM time travel adventure"
    spec["target_audience"] = "kids"
    spec["tone"] = "Fast-moving middle-grade adventure focused on curiosity, consequence, and teamwork."
    spec["setting_rules"] = [
        "Time travel happens only through the Chrono-Engine.",
        "A reckless jump can destabilize the timeline.",
    ]
    return spec


def test_bookgen_pipeline_end_to_end(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    monkeypatch.chdir(root)

    backing: dict[str, bytes] = {}
    monkeypatch.setattr(bookgen, "ObjectStore", lambda: MemoryStore(backing))
    monkeypatch.setattr(bookgen, "_commit_stage_checkpoint", lambda **kwargs: None)
    monkeypatch.setattr(bookgen, "_log_mlflow_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        bookgen,
        "get_settings",
        lambda: SimpleNamespace(
            bookgen_use_llm=False,
            bookgen_llm_chapter_limit=0,
            bookgen_eval_use_llm=False,
            bookgen_eval_llm_chapter_limit=0,
            bookgen_rewrite_use_llm=False,
            bookgen_rewrite_llm_chapter_limit=0,
            bookgen_title_critic_use_llm=False,
            bookgen_title_critic_shortlist_size=5,
            llm_endpoint=None,
        ),
    )

    project_id = "demo-thriller-001"
    bookspec = json.loads((root / "docs/bookgen/bookspec.sample.json").read_text(encoding="utf-8"))
    backing[f"inputs/{project_id}/bookspec.json"] = json.dumps(bookspec).encode("utf-8")
    backing["prompt-packs/thriller/v1/manifest.json"] = json.dumps(
        {"genre": "thriller", "version": "v1", "structure": {"acts": 3}}
    ).encode("utf-8")
    backing["rubrics/thriller/v1/rubric.json"] = json.dumps(
        {
            "genre": "thriller",
            "version": "v1",
            "chapter_min_words": 120,
            "pass_overall": 7.0,
            "hard_fail": [
                {"category": "world_rule_compliance", "min_score": 7.0},
                {"category": "character_consistency", "min_score": 7.0},
            ],
        }
    ).encode("utf-8")

    intake = bookgen.run_intake(project_id=project_id, run_date="2026-02-10")
    resolved = bookgen.run_prompt_pack_resolve(intake=intake)
    planning = bookgen.run_bible_outline(intake=intake, resolved=resolved)
    draft = bookgen.run_chapter_drafting(project_id=project_id)
    review = bookgen.run_chapter_review(project_id=project_id)
    assembly = bookgen.run_assembly_export(project_id=project_id)

    assert planning["chapter_count"] == bookspec["chapter_count"]
    assert draft["drafted"] == bookspec["chapter_count"]
    assert review["failed_chapters"] == 0
    assert review["passed_chapters"] == bookspec["chapter_count"]
    assert review["generation_mode_counts"]["explicit_fallback"] == bookspec["chapter_count"]
    assert assembly["title"] == bookspec["title"]
    assert assembly["export_keys"][0].endswith("manuscript.md")
    assert any(key.endswith("constitution.yaml") for key in backing)
    assert any(key.endswith("chapter_pack.yaml") for key in backing)
    assert any(key.endswith("scene_cards.yaml") for key in backing)
    assert any(key.endswith("scene_beats.yaml") for key in backing)
    assert any(key.endswith("draft_contract.yaml") for key in backing)
    assert any(key.endswith("scene_qc.yaml") for key in backing)
    assert any(key.endswith("draft_qc.yaml") for key in backing)
    assert any(key.endswith("generation_trace.yaml") for key in backing)
    assert any(key.endswith("eval.yaml") for key in backing)
    assert any(key.endswith("editorial_qc.yaml") for key in backing)
    assert any(key.endswith("continuity_review.yaml") for key in backing)
    assert any(key.endswith("release_state.yaml") for key in backing)
    assert any(key.endswith("manuscript.docx") for key in backing)


def test_title_engine_generates_closed_session_slate():
    slate = bookgen._build_title_artifacts(
        project_id="closed-session-series",
        bookspec=_closed_session_title_spec(),
        run_date="2026-03-04",
    )["slate"]

    titles = [item["selected_title"] for item in slate["installments"]]
    assert titles == [
        "The Closed Session",
        "The Quiet Record",
        "The Redacted Vote",
        "The Final Motion",
        "The Public Ledger",
    ]


def test_outline_generates_real_chapter_titles():
    spec = _closed_session_title_spec()
    title_artifacts = bookgen._build_title_artifacts(project_id=spec["project_id"], bookspec=spec, run_date="2026-03-04")
    constitution = bookgen._synthesize_constitution(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        series_title=title_artifacts["slate"]["series_title"],
        title_strategy=title_artifacts["strategy"],
    )
    installment_pack = bookgen._synthesize_installment_pack(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        constitution=constitution,
        selected_title_block=title_artifacts["selected_title_block"],
    )
    outline = bookgen._build_outline(
        project_id=spec["project_id"],
        bookspec=spec,
        constitution=constitution,
        installment_pack=installment_pack,
    )
    assert outline["chapters"][0]["title"] == "Closed Door"
    assert not outline["chapters"][0]["title"].startswith("Hook ")


def test_planning_persists_title_artifacts(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    monkeypatch.chdir(root)

    backing: dict[str, bytes] = {}
    monkeypatch.setattr(bookgen, "ObjectStore", lambda: MemoryStore(backing))
    monkeypatch.setattr(bookgen, "_commit_stage_checkpoint", lambda **kwargs: None)
    monkeypatch.setattr(bookgen, "_log_mlflow_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        bookgen,
        "get_settings",
        lambda: SimpleNamespace(
            bookgen_use_llm=False,
            bookgen_llm_chapter_limit=0,
            bookgen_eval_use_llm=False,
            bookgen_eval_llm_chapter_limit=0,
            bookgen_rewrite_use_llm=False,
            bookgen_rewrite_llm_chapter_limit=0,
            bookgen_title_critic_use_llm=False,
            bookgen_title_critic_shortlist_size=5,
            llm_endpoint=None,
        ),
    )

    project_id = "closed-session-series"
    bookspec = _closed_session_title_spec()
    backing[f"inputs/{project_id}/bookspec.json"] = json.dumps(bookspec).encode("utf-8")
    backing["prompt-packs/thriller/v1/manifest.json"] = json.dumps(
        {"genre": "thriller", "version": "v1", "structure": {"acts": 3}}
    ).encode("utf-8")
    backing["rubrics/thriller/v1/rubric.json"] = json.dumps(
        {
            "genre": "thriller",
            "version": "v1",
            "chapter_min_words": 120,
            "pass_overall": 7.0,
            "hard_fail": [
                {"category": "world_rule_compliance", "min_score": 7.0},
                {"category": "character_consistency", "min_score": 7.0},
            ],
        }
    ).encode("utf-8")

    intake = bookgen.run_intake(project_id=project_id, run_date="2026-03-04")
    resolved = bookgen.run_prompt_pack_resolve(intake=intake)
    planning = bookgen.run_bible_outline(intake=intake, resolved=resolved)

    assert planning["selected_title"] == "The Closed Session"
    assert backing["bookgen/closed-session-series/constitution/title_slate.yaml"]
    installment_pack = yaml.safe_load(
        backing["bookgen/closed-session-series/installments/book-01/installment_pack.yaml"].decode("utf-8")
    )
    assert installment_pack["title_block"]["installment_working_title"] == "The Closed Session"


def test_canonical_markdown_compiles_to_bookspec():
    raw = Path("/home/dustin/log-anomaly-lab/The Closed Session Input.md").read_text(encoding="utf-8")
    bookspec = bookgen._compile_canonical_markdown_intake(
        raw,
        source_path="/home/dustin/log-anomaly-lab/The Closed Session Input.md",
    )

    assert bookspec["project_id"] == "closed-session-series"
    assert bookspec["title"] == "The Closed Session"
    assert bookspec["planned_series_length"] == 5
    assert bookspec["prompt_pack_version"] == "v1"
    assert bookspec["rubric_version"] == "v1"
    assert len(bookspec["installment_title_briefs"]) == 5


def test_genre_policy_profile_selection_for_juvenile():
    profile = bookgen._genre_policy_profile(
        "juvenile fiction",
        "middle-grade STEM time travel adventure",
        "kids",
    )
    assert profile["profile_id"] == "juvenile_adventure"
    assert profile["structure"]["opening_paragraph_min"] == 7
    assert profile["rewrite"]["max_attempts"] == 2


def test_chapter_pack_applies_genre_policy_profile():
    spec = _juvenile_series_spec()
    title_artifacts = bookgen._build_title_artifacts(project_id=spec["project_id"], bookspec=spec, run_date="2026-03-04")
    constitution = bookgen._synthesize_constitution(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        series_title=title_artifacts["slate"]["series_title"],
        title_strategy=title_artifacts["strategy"],
    )
    installment_pack = bookgen._synthesize_installment_pack(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        constitution=constitution,
        selected_title_block=title_artifacts["selected_title_block"],
    )
    outline = bookgen._build_outline(
        project_id=spec["project_id"],
        bookspec=spec,
        constitution=constitution,
        installment_pack=installment_pack,
    )
    ledgers = bookgen._build_initial_ledgers(spec["project_id"], installment_pack, constitution)
    chapter_pack = bookgen._build_chapter_pack(
        project_id=spec["project_id"],
        constitution=constitution,
        installment_pack=installment_pack,
        outline_chapter=outline["chapters"][0],
        ledgers=ledgers,
    )
    draft_contract = chapter_pack["draft_contract"]
    assert draft_contract["genre_policy_profile_id"] == "juvenile_adventure"
    assert draft_contract["structure_requirements"]["paragraph_min"] == 7
    assert draft_contract["structure_requirements"]["scene_turns_min"] == 3
    assert draft_contract["opening_scene_requirements"]["must_force_a_choice_within_words"] == 700
    assert draft_contract["rewrite_policy"]["max_attempts"] == 2
    assert draft_contract["rewrite_policy"]["stop_if_no_improvement_after"] == 1


def test_rewrite_contract_uses_genre_profile_priorities():
    spec = _juvenile_series_spec()
    title_artifacts = bookgen._build_title_artifacts(project_id=spec["project_id"], bookspec=spec, run_date="2026-03-04")
    constitution = bookgen._synthesize_constitution(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        series_title=title_artifacts["slate"]["series_title"],
        title_strategy=title_artifacts["strategy"],
    )
    installment_pack = bookgen._synthesize_installment_pack(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        constitution=constitution,
        selected_title_block=title_artifacts["selected_title_block"],
    )
    outline = bookgen._build_outline(
        project_id=spec["project_id"],
        bookspec=spec,
        constitution=constitution,
        installment_pack=installment_pack,
    )
    ledgers = bookgen._build_initial_ledgers(spec["project_id"], installment_pack, constitution)
    chapter_pack = bookgen._build_chapter_pack(
        project_id=spec["project_id"],
        constitution=constitution,
        installment_pack=installment_pack,
        outline_chapter=outline["chapters"][0],
        ledgers=ledgers,
    )
    eval_report = {
        "scores": {
            "pacing": 6.9,
            "structural_clarity": 7.1,
            "world_rule_compliance": 6.4,
            "character_consistency": 7.2,
            "voice_stability": 7.5,
            "thematic_coherence": 7.8,
            "originality": 7.6,
            "escalation_compliance": 7.4,
        },
        "rewrite_recommendations": [
            {"category": "pacing", "action": "Increase momentum through shorter exchanges and immediate stakes."},
            {"category": "structural_clarity", "action": "Sharpen scene transitions and causal links."},
        ],
    }
    contract = bookgen._build_rewrite_contract(
        eval_report=eval_report,
        constitution=constitution,
        chapter_pack=chapter_pack,
        installment_pack=installment_pack,
        attempt=1,
    )
    assert contract["constraints"]["max_attempts"] == 2
    assert contract["constraints"]["stop_if_no_improvement_after"] == 1
    assert [item["category"] for item in contract["improve"]] == ["pacing", "structural_clarity"]


def test_title_critic_can_rerank_finalists(monkeypatch):
    monkeypatch.setattr(
        bookgen,
        "get_settings",
        lambda: SimpleNamespace(
            bookgen_title_critic_use_llm=True,
            bookgen_title_critic_shortlist_size=4,
            llm_endpoint="stub://llm",
        ),
    )

    class StubLLMClient:
        def complete(self, system_prompt: str, user_prompt: str, *, max_completion_tokens: int | None = None, temperature: float = 0.2) -> str:
            del system_prompt, user_prompt, max_completion_tokens, temperature
            return json.dumps(
                {
                    "selected_title": "The Committee Rule",
                    "rationale": "It sounds more specific and institutional than the generic finalists.",
                    "stronger_alternates": ["The Closed Markup", "The Final Motion"],
                }
            )

    monkeypatch.setattr(bookgen, "LLMClient", StubLLMClient)
    spec = _closed_session_title_spec()
    slate = bookgen._build_title_artifacts(project_id="closed-session-series", bookspec=spec, run_date="2026-03-04")["slate"]
    assert slate["installments"][3]["selected_title"] == "The Committee Rule"


def test_bookgen_uses_llm_for_limited_chapters(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    monkeypatch.chdir(root)

    backing: dict[str, bytes] = {}
    monkeypatch.setattr(bookgen, "ObjectStore", lambda: MemoryStore(backing))
    monkeypatch.setattr(bookgen, "_commit_stage_checkpoint", lambda **kwargs: None)
    monkeypatch.setattr(bookgen, "_log_mlflow_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        bookgen,
        "get_settings",
        lambda: SimpleNamespace(
            bookgen_use_llm=True,
            bookgen_llm_chapter_limit=1,
            bookgen_eval_use_llm=False,
            bookgen_eval_llm_chapter_limit=0,
            bookgen_rewrite_use_llm=False,
            bookgen_rewrite_llm_chapter_limit=0,
            bookgen_title_critic_use_llm=False,
            bookgen_title_critic_shortlist_size=5,
            llm_endpoint="stub://llm",
        ),
    )

    class StubLLMClient:
        def complete(self, system_prompt: str, user_prompt: str, *, max_completion_tokens: int | None = None, temperature: float = 0.2) -> str:
            del system_prompt, user_prompt, max_completion_tokens, temperature
            return "# LLM Chapter\n\nThis chapter came from the LLM path.\n\nIt still returns markdown.\n"

    monkeypatch.setattr(bookgen, "LLMClient", StubLLMClient)

    project_id = "demo-thriller-llm-001"
    bookspec = json.loads((root / "docs/bookgen/bookspec.sample.json").read_text(encoding="utf-8"))
    bookspec["chapter_count"] = 2
    backing[f"inputs/{project_id}/bookspec.json"] = json.dumps(bookspec).encode("utf-8")
    backing["prompt-packs/thriller/v1/manifest.json"] = json.dumps(
        {"genre": "thriller", "version": "v1", "structure": {"acts": 3}}
    ).encode("utf-8")
    backing["rubrics/thriller/v1/rubric.json"] = json.dumps(
        {
            "genre": "thriller",
            "version": "v1",
            "chapter_min_words": 120,
            "pass_overall": 7.0,
            "hard_fail": [
                {"category": "world_rule_compliance", "min_score": 7.0},
                {"category": "character_consistency", "min_score": 7.0},
            ],
        }
    ).encode("utf-8")

    intake = bookgen.run_intake(project_id=project_id, run_date="2026-02-10")
    resolved = bookgen.run_prompt_pack_resolve(intake=intake)
    bookgen.run_bible_outline(intake=intake, resolved=resolved)
    draft = bookgen.run_chapter_drafting(project_id=project_id)

    assert draft["drafted"] == 2
    ch1 = backing["bookgen/demo-thriller-llm-001/installments/book-01/chapters/ch-01/draft.md"].decode("utf-8")
    ch2 = backing["bookgen/demo-thriller-llm-001/installments/book-01/chapters/ch-02/draft.md"].decode("utf-8")
    assert "This chapter came from the LLM path." in ch1
    assert "This chapter came from the LLM path." not in ch2


def test_llm_draft_expands_underlength_summary_forward_opening(monkeypatch):
    spec = _closed_session_title_spec()
    title_artifacts = bookgen._build_title_artifacts(project_id=spec["project_id"], bookspec=spec, run_date="2026-03-04")
    constitution = bookgen._synthesize_constitution(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        series_title=title_artifacts["slate"]["series_title"],
        title_strategy=title_artifacts["strategy"],
    )
    installment_pack = bookgen._synthesize_installment_pack(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        constitution=constitution,
        selected_title_block=title_artifacts["selected_title_block"],
    )
    outline = bookgen._build_outline(
        project_id=spec["project_id"],
        bookspec=spec,
        constitution=constitution,
        installment_pack=installment_pack,
    )
    ledgers = bookgen._build_initial_ledgers(spec["project_id"], installment_pack, constitution)
    chapter_pack = bookgen._build_chapter_pack(
        project_id=spec["project_id"],
        constitution=constitution,
        installment_pack=installment_pack,
        outline_chapter=outline["chapters"][0],
        ledgers=ledgers,
    )

    class StubLLMClient:
        calls = 0

        def complete(self, system_prompt: str, user_prompt: str, *, max_completion_tokens: int | None = None, temperature: float = 0.2) -> str:
            del system_prompt, max_completion_tokens, temperature
            self.__class__.calls += 1
            if self.__class__.calls == 1:
                return "# Closed Door\n\nAdrian had spent the better part of the day thinking about what had happened.\n"
            paragraphs = [
                '"Move," Maya said, catching Adrian at the committee door before he could turn caution into delay. '
                + " ".join(["signal"] * 360),
                "Adrian crossed the anteroom toward the sealed conference suite, badge already in hand, and the clerk at the inner desk "
                "looked up just long enough to register that the usual script had broken. "
                + " ".join(["pressure"] * 360),
                '"If Lang is already inside, we do this clean," Adrian said. Maya answered without lowering her voice: '
                '"Clean stopped being an option when someone edited the witness packet." '
                + " ".join(["record"] * 360),
                "By the time they reached the soundproof corridor outside the hearing room, two staff attorneys had changed positions "
                "and one of them was on the phone with his back turned to the door. "
                + " ".join(["corridor"] * 360),
                '"You want the short version or the honest one?" Maya asked. Adrian pushed the folder into her hands and said, '
                '"Give me the version that forces a choice before Avery does." '
                + " ".join(["choice"] * 360),
                "After the inner latch released, the room gave them fluorescent stillness, fresh coffee, and a revised seating chart "
                "that moved Lang three chairs closer to the ranking member. "
                + " ".join(["markup"] * 360),
                '"That was not the plan," Adrian said. Maya glanced at the amended agenda and replied, '
                '"No. It is the warning." '
                + " ".join(["warning"] * 360),
                "Then Adrian stepped to the witness table, understood who had changed the geometry of the morning, and adjusted before the first question could trap him. "
                + " ".join(["motion"] * 360),
            ]
            return "# Closed Door\n\n" + "\n\n".join(paragraphs) + "\n"

    monkeypatch.setattr(bookgen, "LLMClient", StubLLMClient)
    draft, draft_qc, _ = bookgen._draft_with_process(
        chapter_pack=chapter_pack,
        constitution=constitution,
        installment_pack=installment_pack,
        llm_mode=True,
    )
    assert bookgen._word_count(draft) >= 3000
    assert draft_qc["pass_status"] == "PASS"
    assert "had spent the better part of the day" not in "\n".join(draft.splitlines()[:6]).lower()


def test_bookgen_hybrid_eval_rewrites_one_chapter(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    monkeypatch.chdir(root)

    backing: dict[str, bytes] = {}
    monkeypatch.setattr(bookgen, "ObjectStore", lambda: MemoryStore(backing))
    monkeypatch.setattr(bookgen, "_commit_stage_checkpoint", lambda **kwargs: None)
    monkeypatch.setattr(bookgen, "_log_mlflow_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        bookgen,
        "get_settings",
        lambda: SimpleNamespace(
            bookgen_use_llm=False,
            bookgen_llm_chapter_limit=0,
            bookgen_eval_use_llm=True,
            bookgen_eval_llm_chapter_limit=1,
            bookgen_rewrite_use_llm=True,
            bookgen_rewrite_llm_chapter_limit=1,
            bookgen_title_critic_use_llm=False,
            bookgen_title_critic_shortlist_size=5,
            llm_endpoint="stub://llm",
        ),
    )

    class StubLLMClient:
        eval_calls = 0

        def complete(self, system_prompt: str, user_prompt: str, *, max_completion_tokens: int | None = None, temperature: float = 0.2) -> str:
            del system_prompt, max_completion_tokens, temperature
            if "Evaluate this chapter" in user_prompt:
                self.__class__.eval_calls += 1
                if self.__class__.eval_calls == 1:
                    return json.dumps(
                        {
                            "scores": {
                                "thematic_coherence": 6.0,
                                "character_consistency": 8.0,
                                "voice_stability": 7.0,
                                "world_rule_compliance": 8.0,
                                "escalation_compliance": 8.0,
                                "pacing": 5.0,
                                "structural_clarity": 6.0,
                                "originality": 6.5,
                            },
                            "drift_flags": {
                                "character_voice_shift": False,
                                "moral_boundary_violation": False,
                                "theme_absence": True,
                                "escalation_violation": False,
                                "world_rule_break": False,
                                "continuity_contradiction": False,
                            },
                            "violations": [
                                {"type": "theme_absence", "severity": "moderate", "note": "Theme is too faint."}
                            ],
                            "rewrite_recommendations": [
                                {"category": "theme", "action": "Strengthen the theme through a harder choice."}
                            ],
                            "pass_status": "FAIL",
                            "summary": "Needs revision.",
                        }
                    )
                return json.dumps(
                    {
                        "scores": {
                            "thematic_coherence": 8.5,
                            "character_consistency": 8.4,
                            "voice_stability": 8.1,
                            "world_rule_compliance": 8.8,
                            "escalation_compliance": 8.4,
                            "pacing": 8.0,
                            "structural_clarity": 8.0,
                            "originality": 7.8,
                        },
                        "drift_flags": {
                            "character_voice_shift": False,
                            "moral_boundary_violation": False,
                            "theme_absence": False,
                            "escalation_violation": False,
                            "world_rule_break": False,
                            "continuity_contradiction": False,
                        },
                        "violations": [],
                        "rewrite_recommendations": [],
                        "pass_status": "PASS",
                        "summary": "Revision fixed the issues.",
                    }
                )
            return "# Rewritten Chapter\n\nThe theme now surfaces through a costly decision.\n\nA sharper consequence closes the scene.\n"

    monkeypatch.setattr(bookgen, "LLMClient", StubLLMClient)

    project_id = "demo-thriller-hybrid-001"
    bookspec = json.loads((root / "docs/bookgen/bookspec.sample.json").read_text(encoding="utf-8"))
    bookspec["chapter_count"] = 1
    backing[f"inputs/{project_id}/bookspec.json"] = json.dumps(bookspec).encode("utf-8")
    backing["prompt-packs/thriller/v1/manifest.json"] = json.dumps(
        {"genre": "thriller", "version": "v1", "structure": {"acts": 3}}
    ).encode("utf-8")
    backing["rubrics/thriller/v1/rubric.json"] = json.dumps(
        {
            "genre": "thriller",
            "version": "v1",
            "chapter_min_words": 120,
            "pass_overall": 7.0,
            "hard_fail": [
                {"category": "world_rule_compliance", "min_score": 7.0},
                {"category": "character_consistency", "min_score": 7.0},
            ],
        }
    ).encode("utf-8")

    intake = bookgen.run_intake(project_id=project_id, run_date="2026-02-10")
    resolved = bookgen.run_prompt_pack_resolve(intake=intake)
    bookgen.run_bible_outline(intake=intake, resolved=resolved)
    bookgen.run_chapter_drafting(project_id=project_id)
    review = bookgen.run_chapter_review(project_id=project_id)

    assert review["passed_chapters"] == 1
    final_text = backing["bookgen/demo-thriller-hybrid-001/installments/book-01/chapters/ch-01/final.md"].decode("utf-8")
    eval_report = yaml.safe_load(
        backing["bookgen/demo-thriller-hybrid-001/installments/book-01/chapters/ch-01/eval.yaml"].decode("utf-8")
    )
    assert "The theme now surfaces through a costly decision." in final_text
    assert eval_report["model"] == "hybrid-drift-v1"
    assert eval_report["pass_status"] in {"PASS", "PASS_WITH_NOTES"}


def test_opening_chapter_soft_rewrite_trigger():
    chapter_pack = {"chapter_index": 1}
    eval_report = {
        "scores": {"pacing": 8.1, "originality": 8.0},
        "rewrite_recommendations": [
            {"category": "pacing", "action": "Sharpen the opening tension."},
            {"category": "originality", "action": "Make the opening interaction less familiar."},
        ],
    }
    assert bookgen._opening_chapter_soft_rewrite_required(chapter_pack, eval_report) is True

    late_chapter_pack = {"chapter_index": 5}
    assert bookgen._opening_chapter_soft_rewrite_required(late_chapter_pack, eval_report) is False


def test_smoke_preset_applies_bounded_llm_defaults(monkeypatch):
    monkeypatch.setattr(
        bookgen,
        "get_settings",
        lambda: SimpleNamespace(
            bookgen_generation_preset="smoke",
            bookgen_use_llm=True,
            bookgen_llm_chapter_limit=0,
            bookgen_eval_use_llm=True,
            bookgen_eval_llm_chapter_limit=0,
            bookgen_rewrite_use_llm=True,
            bookgen_rewrite_llm_chapter_limit=0,
            bookgen_structural_retry_limit=0,
            bookgen_title_critic_use_llm=False,
            llm_endpoint="stub://llm",
        ),
    )

    assert bookgen._bookgen_generation_preset() == "smoke"
    assert bookgen._bookgen_use_llm_for_chapter(1) is True
    assert bookgen._bookgen_use_llm_for_chapter(2) is False
    assert bookgen._bookgen_use_llm_for_eval(1) is True
    assert bookgen._bookgen_use_llm_for_eval(2) is False
    assert bookgen._bookgen_use_llm_for_rewrite(1) is True
    assert bookgen._bookgen_use_llm_for_rewrite(2) is False
    assert bookgen._structural_retry_limit() == 1


def test_smoke_prompt_adds_hard_chapter_floor(monkeypatch):
    monkeypatch.setattr(
        bookgen,
        "get_settings",
        lambda: SimpleNamespace(
            bookgen_generation_preset="smoke",
            bookgen_use_llm=True,
            bookgen_llm_chapter_limit=0,
            bookgen_eval_use_llm=False,
            bookgen_eval_llm_chapter_limit=0,
            bookgen_rewrite_use_llm=False,
            bookgen_rewrite_llm_chapter_limit=0,
            bookgen_structural_retry_limit=0,
            bookgen_title_critic_use_llm=False,
            bookgen_title_critic_shortlist_size=5,
            llm_endpoint="stub://llm",
        ),
    )
    spec = _closed_session_title_spec()
    title_artifacts = bookgen._build_title_artifacts(project_id=spec["project_id"], bookspec=spec, run_date="2026-03-04")
    constitution = bookgen._synthesize_constitution(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        series_title=title_artifacts["slate"]["series_title"],
        title_strategy=title_artifacts["strategy"],
    )
    installment_pack = bookgen._synthesize_installment_pack(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        constitution=constitution,
        selected_title_block=title_artifacts["selected_title_block"],
    )
    outline = bookgen._build_outline(
        project_id=spec["project_id"],
        bookspec=spec,
        constitution=constitution,
        installment_pack=installment_pack,
    )
    ledgers = bookgen._build_initial_ledgers(spec["project_id"], installment_pack, constitution)
    chapter_pack = bookgen._build_chapter_pack(
        project_id=spec["project_id"],
        constitution=constitution,
        installment_pack=installment_pack,
        outline_chapter=outline["chapters"][0],
        ledgers=ledgers,
    )
    _, user_prompt = bookgen._bookgen_llm_prompts(chapter_pack, constitution, installment_pack)
    assert "Smoke-mode enforcement" in user_prompt
    assert "Deliver a real chapter, not a teaser" in user_prompt


def test_deterministic_chapter_writer_meets_opening_structure_floor():
    spec = _closed_session_title_spec()
    title_artifacts = bookgen._build_title_artifacts(project_id=spec["project_id"], bookspec=spec, run_date="2026-03-04")
    constitution = bookgen._synthesize_constitution(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        series_title=title_artifacts["slate"]["series_title"],
        title_strategy=title_artifacts["strategy"],
    )
    installment_pack = bookgen._synthesize_installment_pack(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        constitution=constitution,
        selected_title_block=title_artifacts["selected_title_block"],
    )
    outline = bookgen._build_outline(
        project_id=spec["project_id"],
        bookspec=spec,
        constitution=constitution,
        installment_pack=installment_pack,
    )
    ledgers = bookgen._build_initial_ledgers(spec["project_id"], installment_pack, constitution)
    chapter_pack = bookgen._build_chapter_pack(
        project_id=spec["project_id"],
        constitution=constitution,
        installment_pack=installment_pack,
        outline_chapter=outline["chapters"][0],
        ledgers=ledgers,
    )

    draft = bookgen._draft_chapter_markdown(chapter_pack, constitution, installment_pack)
    qc = bookgen._structural_qc_report(draft, chapter_pack, llm_mode=False)

    assert qc["pass_status"] == "PASS"
    assert qc["metrics"]["paragraph_count"] >= 8


def test_final_structural_qc_failure_blocks_editorial_pass(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    monkeypatch.chdir(root)

    backing: dict[str, bytes] = {}
    monkeypatch.setattr(bookgen, "ObjectStore", lambda: MemoryStore(backing))
    monkeypatch.setattr(bookgen, "_commit_stage_checkpoint", lambda **kwargs: None)
    monkeypatch.setattr(bookgen, "_log_mlflow_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        bookgen,
        "get_settings",
        lambda: SimpleNamespace(
            bookgen_generation_preset="smoke",
            bookgen_use_llm=True,
            bookgen_llm_chapter_limit=0,
            bookgen_eval_use_llm=True,
            bookgen_eval_llm_chapter_limit=0,
            bookgen_rewrite_use_llm=False,
            bookgen_rewrite_llm_chapter_limit=0,
            bookgen_structural_retry_limit=1,
            bookgen_title_critic_use_llm=False,
            bookgen_title_critic_shortlist_size=5,
            llm_endpoint="stub://llm",
        ),
    )

    class StubLLMClient:
        def complete(self, system_prompt: str, user_prompt: str, *, max_completion_tokens: int | None = None, temperature: float = 0.2) -> str:
            del system_prompt, max_completion_tokens, temperature
            if "Evaluate this chapter" in user_prompt:
                return json.dumps(
                    {
                        "scores": {
                            "thematic_coherence": 8.6,
                            "character_consistency": 8.4,
                            "voice_stability": 8.2,
                            "world_rule_compliance": 8.8,
                            "escalation_compliance": 8.1,
                            "pacing": 8.0,
                            "structural_clarity": 8.0,
                            "originality": 7.9,
                        },
                        "drift_flags": {
                            "character_voice_shift": False,
                            "moral_boundary_violation": False,
                            "theme_absence": False,
                            "escalation_violation": False,
                            "world_rule_break": False,
                            "continuity_contradiction": False,
                        },
                        "violations": [],
                        "rewrite_recommendations": [],
                        "pass_status": "PASS",
                        "summary": "Editorially acceptable.",
                    }
                )
            return "# Closed Door\n\nToo short to satisfy the structural floor.\n"

    monkeypatch.setattr(bookgen, "LLMClient", StubLLMClient)
    monkeypatch.setattr(bookgen, "_force_underlength_recovery", lambda **kwargs: kwargs["text"])
    monkeypatch.setattr(bookgen, "_llm_top_up_underlength_chapter", lambda **kwargs: kwargs["current_text"])

    project_id = "demo-thriller-structural-block-001"
    bookspec = json.loads((root / "docs/bookgen/bookspec.sample.json").read_text(encoding="utf-8"))
    bookspec["chapter_count"] = 1
    backing[f"inputs/{project_id}/bookspec.json"] = json.dumps(bookspec).encode("utf-8")
    backing["prompt-packs/thriller/v1/manifest.json"] = json.dumps(
        {"genre": "thriller", "version": "v1", "structure": {"acts": 3}}
    ).encode("utf-8")
    backing["rubrics/thriller/v1/rubric.json"] = json.dumps(
        {
            "genre": "thriller",
            "version": "v1",
            "chapter_min_words": 120,
            "pass_overall": 7.0,
            "hard_fail": [
                {"category": "world_rule_compliance", "min_score": 7.0},
                {"category": "character_consistency", "min_score": 7.0},
            ],
        }
    ).encode("utf-8")

    intake = bookgen.run_intake(project_id=project_id, run_date="2026-03-04")
    resolved = bookgen.run_prompt_pack_resolve(intake=intake)
    bookgen.run_bible_outline(intake=intake, resolved=resolved)
    draft = bookgen.run_chapter_drafting(project_id=project_id)
    review = bookgen.run_chapter_review(project_id=project_id)

    assert draft["drafted"] == 1
    assert review["passed_chapters"] == 0
    assert review["failed_chapters"] == 1
    eval_report = yaml.safe_load(
        backing["bookgen/demo-thriller-structural-block-001/installments/book-01/chapters/ch-01/eval.yaml"].decode("utf-8")
    )
    assert eval_report["pass_status"] == "FAIL"
    assert any(item["type"] == "structural_qc_failure" for item in eval_report["violations"])


def test_inspect_chapter_surfaces_process_artifacts(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    monkeypatch.chdir(root)

    backing: dict[str, bytes] = {}
    monkeypatch.setattr(bookgen, "ObjectStore", lambda: MemoryStore(backing))
    monkeypatch.setattr(bookgen, "_commit_stage_checkpoint", lambda **kwargs: None)
    monkeypatch.setattr(bookgen, "_log_mlflow_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        bookgen,
        "get_settings",
        lambda: SimpleNamespace(
            bookgen_generation_preset="production",
            bookgen_use_llm=False,
            bookgen_llm_chapter_limit=0,
            bookgen_eval_use_llm=False,
            bookgen_eval_llm_chapter_limit=0,
            bookgen_rewrite_use_llm=False,
            bookgen_rewrite_llm_chapter_limit=0,
            bookgen_structural_retry_limit=0,
            bookgen_title_critic_use_llm=False,
            bookgen_title_critic_shortlist_size=5,
            llm_endpoint=None,
        ),
    )

    project_id = "demo-thriller-inspect-001"
    bookspec = json.loads((root / "docs/bookgen/bookspec.sample.json").read_text(encoding="utf-8"))
    bookspec["chapter_count"] = 1
    backing[f"inputs/{project_id}/bookspec.json"] = json.dumps(bookspec).encode("utf-8")
    backing["prompt-packs/thriller/v1/manifest.json"] = json.dumps(
        {"genre": "thriller", "version": "v1", "structure": {"acts": 3}}
    ).encode("utf-8")
    backing["rubrics/thriller/v1/rubric.json"] = json.dumps(
        {
            "genre": "thriller",
            "version": "v1",
            "chapter_min_words": 120,
            "pass_overall": 7.0,
            "hard_fail": [
                {"category": "world_rule_compliance", "min_score": 7.0},
                {"category": "character_consistency", "min_score": 7.0},
            ],
        }
    ).encode("utf-8")

    intake = bookgen.run_intake(project_id=project_id, run_date="2026-03-04")
    resolved = bookgen.run_prompt_pack_resolve(intake=intake)
    bookgen.run_bible_outline(intake=intake, resolved=resolved)
    bookgen.run_chapter_drafting(project_id=project_id)
    bookgen.run_chapter_review(project_id=project_id)
    details = bookgen.inspect_chapter(project_id=project_id, chapter_index=1)
    generation_summary = bookgen.inspect_generation_summary(project_id=project_id)

    assert details["project_id"] == project_id
    assert details["chapter_index"] == 1
    assert details["draft_contract"] is not None
    assert details["scene_qc"] is not None
    assert details["draft_qc"] is not None
    assert details["draft_qc_history"] is not None
    assert details["generation_trace"] is not None
    assert details["editorial_qc"] is not None
    assert details["eval_report"] is not None
    assert isinstance(details["has_final_text"], bool)
    assert generation_summary["project_id"] == project_id
    assert len(generation_summary["chapters"]) == 1


def test_llm_draft_timeout_falls_back_to_deterministic(monkeypatch):
    monkeypatch.setattr(
        bookgen,
        "get_settings",
        lambda: SimpleNamespace(
            bookgen_generation_preset="smoke",
            bookgen_use_llm=True,
            bookgen_llm_chapter_limit=1,
            bookgen_eval_use_llm=False,
            bookgen_eval_llm_chapter_limit=0,
            bookgen_rewrite_use_llm=False,
            bookgen_rewrite_llm_chapter_limit=0,
            bookgen_structural_retry_limit=1,
            bookgen_title_critic_use_llm=False,
            bookgen_title_critic_shortlist_size=5,
            llm_endpoint="stub://llm",
        ),
    )

    class StubLLMClient:
        def complete(self, *args, **kwargs):
            raise RuntimeError("timeout")

    monkeypatch.setattr(bookgen, "LLMClient", StubLLMClient)

    spec = _closed_session_title_spec()
    title_artifacts = bookgen._build_title_artifacts(project_id=spec["project_id"], bookspec=spec, run_date="2026-03-04")
    constitution = bookgen._synthesize_constitution(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        series_title=title_artifacts["slate"]["series_title"],
        title_strategy=title_artifacts["strategy"],
    )
    installment_pack = bookgen._synthesize_installment_pack(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        constitution=constitution,
        selected_title_block=title_artifacts["selected_title_block"],
    )
    outline = bookgen._build_outline(
        project_id=spec["project_id"],
        bookspec=spec,
        constitution=constitution,
        installment_pack=installment_pack,
    )
    ledgers = bookgen._build_initial_ledgers(spec["project_id"], installment_pack, constitution)
    chapter_pack = bookgen._build_chapter_pack(
        project_id=spec["project_id"],
        constitution=constitution,
        installment_pack=installment_pack,
        outline_chapter=outline["chapters"][0],
        ledgers=ledgers,
    )

    draft, draft_qc, history = bookgen._draft_with_process(
        chapter_pack=chapter_pack,
        constitution=constitution,
        installment_pack=installment_pack,
        llm_mode=True,
    )

    assert draft.startswith("# ")
    assert draft_qc["pass_status"] == "PASS"
    assert history[0]["llm_fallback"]["stage"] == "draft"


def test_llm_draft_uses_gpt5_recovery_after_empty_content(monkeypatch):
    monkeypatch.setattr(
        bookgen,
        "get_settings",
        lambda: SimpleNamespace(
            bookgen_generation_preset="smoke",
            bookgen_use_llm=True,
            bookgen_llm_chapter_limit=1,
            bookgen_eval_use_llm=False,
            bookgen_eval_llm_chapter_limit=0,
            bookgen_rewrite_use_llm=False,
            bookgen_rewrite_llm_chapter_limit=0,
            bookgen_structural_retry_limit=0,
            bookgen_title_critic_use_llm=False,
            bookgen_title_critic_shortlist_size=5,
            llm_endpoint="https://api.openai.com/v1/chat/completions",
            llm_provider_profile="gpt5",
            llm_model="gpt-5",
        ),
    )

    spec = _closed_session_title_spec()
    title_artifacts = bookgen._build_title_artifacts(project_id=spec["project_id"], bookspec=spec, run_date="2026-03-04")
    constitution = bookgen._synthesize_constitution(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        series_title=title_artifacts["slate"]["series_title"],
        title_strategy=title_artifacts["strategy"],
    )
    installment_pack = bookgen._synthesize_installment_pack(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        constitution=constitution,
        selected_title_block=title_artifacts["selected_title_block"],
    )
    outline = bookgen._build_outline(
        project_id=spec["project_id"],
        bookspec=spec,
        constitution=constitution,
        installment_pack=installment_pack,
    )
    ledgers = bookgen._build_initial_ledgers(spec["project_id"], installment_pack, constitution)
    chapter_pack = bookgen._build_chapter_pack(
        project_id=spec["project_id"],
        constitution=constitution,
        installment_pack=installment_pack,
        outline_chapter=outline["chapters"][0],
        ledgers=ledgers,
    )
    chapter_pack["draft_contract"]["word_count"]["min_llm"] = 150
    chapter_pack["draft_contract"]["word_count"]["max_llm"] = 9000
    chapter_pack["draft_contract"]["structure_requirements"]["paragraph_min"] = 3
    chapter_pack["draft_contract"]["structure_requirements"]["scene_turns_min"] = 1
    chapter_pack["draft_contract"]["structure_requirements"]["dialogue_presence_required"] = False

    monkeypatch.setattr(
        bookgen,
        "_draft_chapter_with_llm",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("LLM response contained empty content")),
    )
    called = {"recovery": 0}

    def _recovery(*args, **kwargs):
        called["recovery"] += 1
        return bookgen._draft_chapter_markdown(
            chapter_pack,
            constitution,
            installment_pack,
        )

    monkeypatch.setattr(bookgen, "_draft_chapter_with_llm_recovery", _recovery)

    draft, draft_qc, history = bookgen._draft_with_process(
        chapter_pack=chapter_pack,
        constitution=constitution,
        installment_pack=installment_pack,
        llm_mode=True,
    )

    assert called["recovery"] == 1
    assert draft.startswith("# ")
    assert draft_qc["pass_status"] == "PASS"
    assert draft_qc["mode"] == "llm"
    assert all(item.get("llm_fallback") is None for item in history if isinstance(item, dict))


def test_llm_draft_skips_gpt5_recovery_for_quota_errors(monkeypatch):
    monkeypatch.setattr(
        bookgen,
        "get_settings",
        lambda: SimpleNamespace(
            bookgen_generation_preset="smoke",
            bookgen_use_llm=True,
            bookgen_llm_chapter_limit=1,
            bookgen_eval_use_llm=False,
            bookgen_eval_llm_chapter_limit=0,
            bookgen_rewrite_use_llm=False,
            bookgen_rewrite_llm_chapter_limit=0,
            bookgen_structural_retry_limit=1,
            bookgen_title_critic_use_llm=False,
            bookgen_title_critic_shortlist_size=5,
            llm_endpoint="https://api.openai.com/v1/chat/completions",
            llm_provider_profile="gpt5",
            llm_model="gpt-5",
        ),
    )

    spec = _closed_session_title_spec()
    title_artifacts = bookgen._build_title_artifacts(project_id=spec["project_id"], bookspec=spec, run_date="2026-03-04")
    constitution = bookgen._synthesize_constitution(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        series_title=title_artifacts["slate"]["series_title"],
        title_strategy=title_artifacts["strategy"],
    )
    installment_pack = bookgen._synthesize_installment_pack(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        constitution=constitution,
        selected_title_block=title_artifacts["selected_title_block"],
    )
    outline = bookgen._build_outline(
        project_id=spec["project_id"],
        bookspec=spec,
        constitution=constitution,
        installment_pack=installment_pack,
    )
    ledgers = bookgen._build_initial_ledgers(spec["project_id"], installment_pack, constitution)
    chapter_pack = bookgen._build_chapter_pack(
        project_id=spec["project_id"],
        constitution=constitution,
        installment_pack=installment_pack,
        outline_chapter=outline["chapters"][0],
        ledgers=ledgers,
    )

    monkeypatch.setattr(
        bookgen,
        "_draft_chapter_with_llm",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("LLM request failed (status=429)")),
    )
    called = {"recovery": 0}

    def _recovery(*args, **kwargs):
        called["recovery"] += 1
        return "# Should not run\n"

    monkeypatch.setattr(bookgen, "_draft_chapter_with_llm_recovery", _recovery)

    draft, draft_qc, history = bookgen._draft_with_process(
        chapter_pack=chapter_pack,
        constitution=constitution,
        installment_pack=installment_pack,
        llm_mode=True,
    )

    assert called["recovery"] == 0
    assert draft.startswith("# ")
    assert draft_qc["pass_status"] == "PASS"
    assert draft_qc["mode"] == "fallback"
    assert history[0]["llm_fallback"]["stage"] == "draft"


def test_underlength_llm_path_uses_scene_expansion(monkeypatch):
    monkeypatch.setattr(
        bookgen,
        "get_settings",
        lambda: SimpleNamespace(
            bookgen_generation_preset="smoke",
            bookgen_use_llm=True,
            bookgen_llm_chapter_limit=1,
            bookgen_eval_use_llm=False,
            bookgen_eval_llm_chapter_limit=0,
            bookgen_rewrite_use_llm=False,
            bookgen_rewrite_llm_chapter_limit=0,
            bookgen_structural_retry_limit=1,
            bookgen_title_critic_use_llm=False,
            bookgen_title_critic_shortlist_size=5,
            llm_endpoint="stub://llm",
        ),
    )

    spec = _closed_session_title_spec()
    title_artifacts = bookgen._build_title_artifacts(project_id=spec["project_id"], bookspec=spec, run_date="2026-03-04")
    constitution = bookgen._synthesize_constitution(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        series_title=title_artifacts["slate"]["series_title"],
        title_strategy=title_artifacts["strategy"],
    )
    installment_pack = bookgen._synthesize_installment_pack(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        constitution=constitution,
        selected_title_block=title_artifacts["selected_title_block"],
    )
    outline = bookgen._build_outline(
        project_id=spec["project_id"],
        bookspec=spec,
        constitution=constitution,
        installment_pack=installment_pack,
    )
    ledgers = bookgen._build_initial_ledgers(spec["project_id"], installment_pack, constitution)
    chapter_pack = bookgen._build_chapter_pack(
        project_id=spec["project_id"],
        constitution=constitution,
        installment_pack=installment_pack,
        outline_chapter=outline["chapters"][0],
        ledgers=ledgers,
    )
    chapter_pack["draft_contract"]["word_count"]["min_llm"] = 200
    chapter_pack["draft_contract"]["word_count"]["max_llm"] = 1800
    chapter_pack["scene_constraints"]["word_count_target"] = 350
    scene_cards = bookgen._build_scene_cards(chapter_pack=chapter_pack)
    scene_beats = bookgen._build_scene_beats(chapter_pack=chapter_pack, scene_cards=scene_cards)

    monkeypatch.setattr(
        bookgen,
        "_draft_chapter_with_llm",
        lambda *args, **kwargs: "# The Motion\n\n\"Short line,\" Maya said.\n\nAdrian nodded.\n",
    )
    called = {"underlength": 0}

    def _expand(*args, **kwargs):
        called["underlength"] += 1
        return bookgen._draft_chapter_markdown(
            chapter_pack,
            constitution,
            installment_pack,
            scene_cards=scene_cards,
            scene_beats=scene_beats,
        )

    monkeypatch.setattr(bookgen, "_llm_expand_underlength_chapter", _expand)

    draft, draft_qc, history = bookgen._draft_with_process(
        chapter_pack=chapter_pack,
        constitution=constitution,
        installment_pack=installment_pack,
        scene_cards=scene_cards,
        scene_beats=scene_beats,
        llm_mode=True,
    )

    assert called["underlength"] == 1
    assert draft_qc["pass_status"] == "PASS"
    assert len(history) >= 2
    assert draft.startswith("# ")


def test_expand_underlength_adds_continuation_until_minimum(monkeypatch):
    monkeypatch.setattr(
        bookgen,
        "get_settings",
        lambda: SimpleNamespace(
            bookgen_generation_preset="smoke",
            llm_provider_profile="default",
            llm_model="gpt-4o",
        ),
    )

    spec = _closed_session_title_spec()
    title_artifacts = bookgen._build_title_artifacts(project_id=spec["project_id"], bookspec=spec, run_date="2026-03-04")
    constitution = bookgen._synthesize_constitution(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        series_title=title_artifacts["slate"]["series_title"],
        title_strategy=title_artifacts["strategy"],
    )
    installment_pack = bookgen._synthesize_installment_pack(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        constitution=constitution,
        selected_title_block=title_artifacts["selected_title_block"],
    )
    outline = bookgen._build_outline(
        project_id=spec["project_id"],
        bookspec=spec,
        constitution=constitution,
        installment_pack=installment_pack,
    )
    ledgers = bookgen._build_initial_ledgers(spec["project_id"], installment_pack, constitution)
    chapter_pack = bookgen._build_chapter_pack(
        project_id=spec["project_id"],
        constitution=constitution,
        installment_pack=installment_pack,
        outline_chapter=outline["chapters"][0],
        ledgers=ledgers,
    )
    chapter_pack["draft_contract"]["word_count"]["min_llm"] = 260
    chapter_pack["draft_contract"]["word_count"]["max_llm"] = 1600
    chapter_pack["scene_constraints"]["word_count_target"] = 420

    class StubClient:
        def __init__(self):
            self.calls = 0

        def complete(self, system_prompt: str, user_prompt: str, *, max_completion_tokens: int | None = None, temperature: float = 0.2) -> str:
            del system_prompt, user_prompt, max_completion_tokens, temperature
            self.calls += 1
            if self.calls == 1:
                return "# The Motion\n\n\"Too short,\" Maya said.\n"
            return "Adrian moved first, pushed through the committee corridor, and forced a decision under pressure. " + " ".join(["detail"] * 260)

    client = StubClient()
    out = bookgen._llm_expand_underlength_chapter(
        client=client,
        current_text="# The Motion\n\n\"Too short,\" Maya said.\n",
        chapter_pack=chapter_pack,
        constitution=constitution,
        installment_pack=installment_pack,
        scene_cards=None,
        scene_beats=None,
    )

    assert client.calls >= 2
    assert out.startswith("# ")
    assert bookgen._word_count(out) >= chapter_pack["draft_contract"]["word_count"]["min_llm"]


def test_underlength_recovery_forces_structural_floor_when_llm_stays_short(monkeypatch):
    monkeypatch.setattr(
        bookgen,
        "get_settings",
        lambda: SimpleNamespace(
            bookgen_generation_preset="smoke",
            bookgen_use_llm=True,
            bookgen_llm_chapter_limit=1,
            bookgen_eval_use_llm=False,
            bookgen_eval_llm_chapter_limit=0,
            bookgen_rewrite_use_llm=False,
            bookgen_rewrite_llm_chapter_limit=0,
            bookgen_structural_retry_limit=1,
            bookgen_title_critic_use_llm=False,
            bookgen_title_critic_shortlist_size=5,
            llm_endpoint="stub://llm",
        ),
    )

    spec = _closed_session_title_spec()
    title_artifacts = bookgen._build_title_artifacts(project_id=spec["project_id"], bookspec=spec, run_date="2026-03-04")
    constitution = bookgen._synthesize_constitution(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        series_title=title_artifacts["slate"]["series_title"],
        title_strategy=title_artifacts["strategy"],
    )
    installment_pack = bookgen._synthesize_installment_pack(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        constitution=constitution,
        selected_title_block=title_artifacts["selected_title_block"],
    )
    outline = bookgen._build_outline(
        project_id=spec["project_id"],
        bookspec=spec,
        constitution=constitution,
        installment_pack=installment_pack,
    )
    ledgers = bookgen._build_initial_ledgers(spec["project_id"], installment_pack, constitution)
    chapter_pack = bookgen._build_chapter_pack(
        project_id=spec["project_id"],
        constitution=constitution,
        installment_pack=installment_pack,
        outline_chapter=outline["chapters"][0],
        ledgers=ledgers,
    )
    chapter_pack["draft_contract"]["word_count"]["min_llm"] = 350
    chapter_pack["draft_contract"]["word_count"]["max_llm"] = 1900
    chapter_pack["scene_constraints"]["word_count_target"] = 450
    scene_cards = bookgen._build_scene_cards(chapter_pack=chapter_pack)
    scene_beats = bookgen._build_scene_beats(chapter_pack=chapter_pack, scene_cards=scene_cards)

    monkeypatch.setattr(
        bookgen,
        "_draft_chapter_with_llm",
        lambda *args, **kwargs: "# The Motion\n\n\"Short,\" Maya said.\n",
    )
    monkeypatch.setattr(
        bookgen,
        "_llm_expand_underlength_chapter",
        lambda *args, **kwargs: "# The Motion\n\nStill short.\n",
    )

    draft, draft_qc, history = bookgen._draft_with_process(
        chapter_pack=chapter_pack,
        constitution=constitution,
        installment_pack=installment_pack,
        scene_cards=scene_cards,
        scene_beats=scene_beats,
        llm_mode=True,
    )

    assert draft_qc["pass_status"] == "PASS"
    assert draft_qc["mode"] == "fallback"
    assert any(item.get("llm_fallback", {}).get("stage") == "underlength_recovery" for item in history if isinstance(item, dict))
    assert bookgen._word_count(draft) >= chapter_pack["draft_contract"]["word_count"]["min_llm"]


def test_world_rule_rewrite_injects_sealed_process_language():
    chapter_pack = {
        "chapter_id": "ch-01",
        "chapter_card": {"title": "The Motion", "goal": "Prevent a reckless leak.", "emotional_shift": {"to": "pressure"}},
        "scene_constraints": {"pov_character_id": "adrian-cole", "word_count_target": 1000},
    }
    eval_report = {
        "drift_flags": {"theme_absence": False, "character_voice_shift": False, "world_rule_break": True},
        "metrics": {"word_count": 800},
    }
    rewritten = bookgen._rewrite_chapter_text(
        "# The Motion\n\nHe leaked classified files to the press without consequence.\n",
        chapter_pack,
        eval_report,
    )
    lowered = rewritten.lower()
    assert "sealed process" in lowered or "sealed classified notice" in lowered
    assert "legal liability" in lowered or "consequences" in lowered


def test_world_rule_rewrite_uses_juvenile_policy_language():
    chapter_pack = {
        "chapter_id": "ch-01",
        "chapter_card": {"title": "The Rusty Machine", "goal": "Protect the timeline.", "emotional_shift": {"to": "pressure"}},
        "scene_constraints": {"pov_character_id": "maya", "word_count_target": 1000},
        "policy_profile": {
            "profile_id": "juvenile_adventure",
            "profile": bookgen._genre_policy_profile("juvenile fiction", "middle-grade STEM adventure", "kids"),
        },
    }
    eval_report = {
        "drift_flags": {"theme_absence": False, "character_voice_shift": False, "world_rule_break": True},
        "metrics": {"word_count": 800},
    }
    rewritten = bookgen._rewrite_chapter_text(
        "# The Rusty Machine\n\nThey leaked the experiment publicly before checking safety.\n",
        chapter_pack,
        eval_report,
    )
    lowered = rewritten.lower()
    assert "approved adult" in lowered or "safety protocol" in lowered
    assert "timeline" in lowered or "consequences" in lowered


def test_scene_qc_rewrite_recovers_scene_qc_failures():
    chapter_pack = {
        "chapter_id": "ch-01",
        "chapter_card": {
            "title": "The Rusty Machine",
            "information": {"reveal": ["The Chrono-Engine has a hidden fail-safe."]},
        },
    }
    scene_cards = {
        "scenes": [
            {"scene_label": "Garage Bench"},
            {"scene_label": "Power Test"},
        ]
    }
    scene_beats = {
        "beats": [
            {"intent": "Timeline pressure spikes during the power test."},
            {"intent": "Maya must choose between caution and speed."},
            {"intent": "The fail-safe reveal changes the plan."},
        ]
    }
    text = "# The Rusty Machine\n\nMaya stared at the machine and hesitated.\n"
    before = bookgen._scene_qc_report(
        text=text,
        chapter_pack=chapter_pack,
        scene_cards=scene_cards,
        scene_beats=scene_beats,
    )
    assert before["pass_status"] == "FAIL"
    rewritten = bookgen._scene_qc_rewrite(
        text=text,
        chapter_pack=chapter_pack,
        scene_cards=scene_cards,
        scene_beats=scene_beats,
        issues=before["issues"],
    )
    after = bookgen._scene_qc_report(
        text=rewritten,
        chapter_pack=chapter_pack,
        scene_cards=scene_cards,
        scene_beats=scene_beats,
    )
    assert after["pass_status"] == "PASS"


def test_run_chapter_drafting_repairs_scene_qc_before_review(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    monkeypatch.chdir(root)

    backing: dict[str, bytes] = {}
    monkeypatch.setattr(bookgen, "ObjectStore", lambda: MemoryStore(backing))
    monkeypatch.setattr(bookgen, "_commit_stage_checkpoint", lambda **kwargs: None)
    monkeypatch.setattr(bookgen, "_log_mlflow_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        bookgen,
        "get_settings",
        lambda: SimpleNamespace(
            bookgen_use_llm=False,
            bookgen_llm_chapter_limit=0,
            bookgen_eval_use_llm=False,
            bookgen_eval_llm_chapter_limit=0,
            bookgen_rewrite_use_llm=False,
            bookgen_rewrite_llm_chapter_limit=0,
            bookgen_title_critic_use_llm=False,
            bookgen_title_critic_shortlist_size=5,
            llm_endpoint=None,
        ),
    )

    project_id = "demo-thriller-scene-qc-repair-001"
    bookspec = json.loads((root / "docs/bookgen/bookspec.sample.json").read_text(encoding="utf-8"))
    bookspec["chapter_count"] = 1
    backing[f"inputs/{project_id}/bookspec.json"] = json.dumps(bookspec).encode("utf-8")
    backing["prompt-packs/thriller/v1/manifest.json"] = json.dumps(
        {"genre": "thriller", "version": "v1", "structure": {"acts": 3}}
    ).encode("utf-8")
    backing["rubrics/thriller/v1/rubric.json"] = json.dumps(
        {
            "genre": "thriller",
            "version": "v1",
            "chapter_min_words": 120,
            "pass_overall": 7.0,
            "hard_fail": [
                {"category": "world_rule_compliance", "min_score": 7.0},
                {"category": "character_consistency", "min_score": 7.0},
            ],
        }
    ).encode("utf-8")

    original_scene_qc_report = bookgen._scene_qc_report
    calls = {"count": 0}

    def flaky_scene_qc_report(*, text, chapter_pack, scene_cards, scene_beats):
        calls["count"] += 1
        if calls["count"] == 1:
            return {
                "schema_version": "1.0",
                "chapter_id": chapter_pack["chapter_id"],
                "pass_status": "FAIL",
                "checks": {
                    "scene_count_defined": True,
                    "paragraphs_support_scene_count": False,
                    "beat_coverage_ok": False,
                    "reveal_reference_present": False,
                    "scene_turns_ok": False,
                },
                "metrics": {
                    "scene_count": 2,
                    "beat_count": 3,
                    "beat_coverage_ratio": 0.0,
                    "paragraph_count": 1,
                    "scene_turns": 1,
                },
                "issues": [
                    "paragraphs_support_scene_count",
                    "beat_coverage_ok",
                    "reveal_reference_present",
                    "scene_turns_ok",
                ],
            }
        return original_scene_qc_report(
            text=text,
            chapter_pack=chapter_pack,
            scene_cards=scene_cards,
            scene_beats=scene_beats,
        )

    monkeypatch.setattr(bookgen, "_scene_qc_report", flaky_scene_qc_report)

    intake = bookgen.run_intake(project_id=project_id, run_date="2026-03-05")
    resolved = bookgen.run_prompt_pack_resolve(intake=intake)
    bookgen.run_bible_outline(intake=intake, resolved=resolved)
    draft = bookgen.run_chapter_drafting(project_id=project_id)

    assert draft["drafted"] == 1
    inspected = bookgen.inspect_chapter(project_id=project_id, chapter_index=1)
    assert inspected["scene_qc"]["pass_status"] == "PASS"
    assert inspected["scene_qc_history"] is not None
    assert len(inspected["scene_qc_history"]["history"]) >= 2


def test_mlflow_log_skips_when_tracking_uri_unreachable(monkeypatch):
    monkeypatch.setattr(
        bookgen,
        "get_settings",
        lambda: SimpleNamespace(mlflow_tracking_uri="http://mlflow.invalid:5000", mlflow_local_tracking_uri=None),
    )
    monkeypatch.setattr(bookgen, "resolve_service_uri", lambda remote, local: remote or local)
    monkeypatch.setattr(bookgen, "_mlflow_tracking_uri_reachable", lambda uri: False)

    called = {"set_tracking_uri": 0}

    class StubMLflow:
        @staticmethod
        def set_tracking_uri(uri):
            del uri
            called["set_tracking_uri"] += 1

    monkeypatch.setattr(bookgen, "mlflow", StubMLflow)
    bookgen._log_mlflow_summary(
        project_id="demo",
        installment_id="book-01",
        summary={"chapter_count": 1, "chapter_pass_rate": 1.0, "avg_overall_score": 8.0},
        artifacts={"x": "y"},
    )
    assert called["set_tracking_uri"] == 0


def test_mlflow_log_runs_when_tracking_uri_reachable(monkeypatch):
    monkeypatch.setattr(
        bookgen,
        "get_settings",
        lambda: SimpleNamespace(mlflow_tracking_uri="http://127.0.0.1:15000", mlflow_local_tracking_uri=None),
    )
    monkeypatch.setattr(bookgen, "resolve_service_uri", lambda remote, local: remote or local)
    monkeypatch.setattr(bookgen, "_mlflow_tracking_uri_reachable", lambda uri: True)

    calls = {"set_tracking_uri": 0, "set_experiment": 0, "start_run": 0, "log_metric": 0, "log_dict": 0}

    class _RunCtx:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            return False

    class StubMLflow:
        @staticmethod
        def set_tracking_uri(uri):
            del uri
            calls["set_tracking_uri"] += 1

        @staticmethod
        def set_experiment(name):
            del name
            calls["set_experiment"] += 1

        @staticmethod
        def start_run(run_name=None):
            del run_name
            calls["start_run"] += 1
            return _RunCtx()

        @staticmethod
        def log_params(payload):
            del payload

        @staticmethod
        def log_metric(name, value):
            del name, value
            calls["log_metric"] += 1

        @staticmethod
        def log_dict(payload, artifact_file):
            del payload, artifact_file
            calls["log_dict"] += 1

    monkeypatch.setattr(bookgen, "mlflow", StubMLflow)
    bookgen._log_mlflow_summary(
        project_id="demo",
        installment_id="book-01",
        summary={"chapter_count": 1, "chapter_pass_rate": 1.0, "avg_overall_score": 8.0},
        artifacts={"x": "y"},
    )
    assert calls["set_tracking_uri"] == 1
    assert calls["set_experiment"] == 1
    assert calls["start_run"] == 1
    assert calls["log_metric"] >= 2
    assert calls["log_dict"] == 1


def test_llm_eval_timeout_falls_back_to_deterministic(monkeypatch):
    monkeypatch.setattr(
        bookgen,
        "get_settings",
        lambda: SimpleNamespace(
            bookgen_generation_preset="smoke",
            bookgen_use_llm=False,
            bookgen_llm_chapter_limit=0,
            bookgen_eval_use_llm=True,
            bookgen_eval_llm_chapter_limit=1,
            bookgen_rewrite_use_llm=False,
            bookgen_rewrite_llm_chapter_limit=0,
            bookgen_structural_retry_limit=1,
            bookgen_title_critic_use_llm=False,
            bookgen_title_critic_shortlist_size=5,
            llm_endpoint="stub://llm",
        ),
    )

    class StubLLMClient:
        def complete(self, *args, **kwargs):
            raise RuntimeError("timeout")

    monkeypatch.setattr(bookgen, "LLMClient", StubLLMClient)

    spec = _closed_session_title_spec()
    title_artifacts = bookgen._build_title_artifacts(project_id=spec["project_id"], bookspec=spec, run_date="2026-03-04")
    constitution = bookgen._synthesize_constitution(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        series_title=title_artifacts["slate"]["series_title"],
        title_strategy=title_artifacts["strategy"],
    )
    installment_pack = bookgen._synthesize_installment_pack(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        constitution=constitution,
        selected_title_block=title_artifacts["selected_title_block"],
    )
    outline = bookgen._build_outline(
        project_id=spec["project_id"],
        bookspec=spec,
        constitution=constitution,
        installment_pack=installment_pack,
    )
    ledgers = bookgen._build_initial_ledgers(spec["project_id"], installment_pack, constitution)
    chapter_pack = bookgen._build_chapter_pack(
        project_id=spec["project_id"],
        constitution=constitution,
        installment_pack=installment_pack,
        outline_chapter=outline["chapters"][0],
        ledgers=ledgers,
    )
    draft = bookgen._draft_chapter_markdown(chapter_pack, constitution, installment_pack)
    rubric_cfg = bookgen._merge_rubric(
        constitution,
        {
            "chapter_min_words": 120,
            "pass_overall": 7.0,
            "hard_fail": [
                {"category": "world_rule_compliance", "min_score": 7.0},
                {"category": "character_consistency", "min_score": 7.0},
            ],
        },
    )

    eval_report = bookgen._evaluate_chapter_with_llm(
        text=draft,
        constitution=constitution,
        installment_pack=installment_pack,
        chapter_pack=chapter_pack,
        rubric_cfg=rubric_cfg,
        prompt_pack_version="v1",
    )

    assert eval_report["model"] == "deterministic-drift-v1"
    assert eval_report["llm_fallback"]["stage"] == "eval"


def test_resolve_service_uri_prefers_local_when_cluster_host_unresolvable(monkeypatch):
    from app import service_endpoints

    monkeypatch.setattr(service_endpoints, "_host_is_resolvable", lambda host: False)
    monkeypatch.setattr(service_endpoints, "_uri_is_reachable", lambda uri: uri == "http://127.0.0.1:15000")

    resolved = service_endpoints.resolve_service_uri(
        "http://mlflow.log-anomaly.svc.cluster.local:5000",
        "http://127.0.0.1:15000",
    )

    assert resolved == "http://127.0.0.1:15000"


def test_approve_installment_transitions_release_state(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    monkeypatch.chdir(root)

    backing: dict[str, bytes] = {}
    monkeypatch.setattr(bookgen, "ObjectStore", lambda: MemoryStore(backing))
    monkeypatch.setattr(bookgen, "_commit_stage_checkpoint", lambda **kwargs: None)
    monkeypatch.setattr(bookgen, "_log_mlflow_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        bookgen,
        "get_settings",
        lambda: SimpleNamespace(
            bookgen_generation_preset="production",
            bookgen_use_llm=False,
            bookgen_llm_chapter_limit=0,
            bookgen_eval_use_llm=False,
            bookgen_eval_llm_chapter_limit=0,
            bookgen_rewrite_use_llm=False,
            bookgen_rewrite_llm_chapter_limit=0,
            bookgen_structural_retry_limit=0,
            bookgen_title_critic_use_llm=False,
            bookgen_title_critic_shortlist_size=5,
            llm_endpoint=None,
            mlflow_tracking_uri="http://127.0.0.1:15000",
            mlflow_local_tracking_uri="http://127.0.0.1:15000",
        ),
    )

    project_id = "demo-thriller-approval-001"
    bookspec = json.loads((root / "docs/bookgen/bookspec.sample.json").read_text(encoding="utf-8"))
    bookspec["chapter_count"] = 1
    backing[f"inputs/{project_id}/bookspec.json"] = json.dumps(bookspec).encode("utf-8")
    backing["prompt-packs/thriller/v1/manifest.json"] = json.dumps(
        {"genre": "thriller", "version": "v1", "structure": {"acts": 3}}
    ).encode("utf-8")
    backing["rubrics/thriller/v1/rubric.json"] = json.dumps(
        {
            "genre": "thriller",
            "version": "v1",
            "chapter_min_words": 120,
            "pass_overall": 7.0,
            "hard_fail": [
                {"category": "world_rule_compliance", "min_score": 7.0},
                {"category": "character_consistency", "min_score": 7.0},
            ],
        }
    ).encode("utf-8")

    intake = bookgen.run_intake(project_id=project_id, run_date="2026-03-04")
    resolved = bookgen.run_prompt_pack_resolve(intake=intake)
    bookgen.run_bible_outline(intake=intake, resolved=resolved)
    bookgen.run_chapter_drafting(project_id=project_id)
    bookgen.run_chapter_review(project_id=project_id)
    bookgen.run_assembly_export(project_id=project_id)

    release = bookgen.approve_installment(project_id=project_id, decision="approve", note="Editorial signoff complete.")
    report = bookgen.operator_report(project_id=project_id)

    assert release["status"] == "approved_for_export"
    assert report["release_state"]["status"] == "approved_for_export"
    assert report["continuity_review"]["pass_status"] == "PASS"


def test_locked_installment_blocks_mutating_stages_without_override(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    monkeypatch.chdir(root)

    backing: dict[str, bytes] = {}
    monkeypatch.setattr(bookgen, "ObjectStore", lambda: MemoryStore(backing))
    monkeypatch.setattr(bookgen, "_commit_stage_checkpoint", lambda **kwargs: None)
    monkeypatch.setattr(bookgen, "_log_mlflow_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        bookgen,
        "get_settings",
        lambda: SimpleNamespace(
            bookgen_generation_preset="production",
            bookgen_use_llm=False,
            bookgen_llm_chapter_limit=0,
            bookgen_eval_use_llm=False,
            bookgen_eval_llm_chapter_limit=0,
            bookgen_rewrite_use_llm=False,
            bookgen_rewrite_llm_chapter_limit=0,
            bookgen_structural_retry_limit=0,
            bookgen_title_critic_use_llm=False,
            bookgen_title_critic_shortlist_size=5,
            bookgen_allow_lock_override=False,
            llm_endpoint=None,
            mlflow_tracking_uri="http://127.0.0.1:15000",
            mlflow_local_tracking_uri="http://127.0.0.1:15000",
        ),
    )

    project_id = "demo-thriller-lock-001"
    bookspec = json.loads((root / "docs/bookgen/bookspec.sample.json").read_text(encoding="utf-8"))
    bookspec["chapter_count"] = 1
    backing[f"inputs/{project_id}/bookspec.json"] = json.dumps(bookspec).encode("utf-8")
    backing["prompt-packs/thriller/v1/manifest.json"] = json.dumps(
        {"genre": "thriller", "version": "v1", "structure": {"acts": 3}}
    ).encode("utf-8")
    backing["rubrics/thriller/v1/rubric.json"] = json.dumps(
        {
            "genre": "thriller",
            "version": "v1",
            "chapter_min_words": 120,
            "pass_overall": 7.0,
            "hard_fail": [
                {"category": "world_rule_compliance", "min_score": 7.0},
                {"category": "character_consistency", "min_score": 7.0},
            ],
        }
    ).encode("utf-8")

    intake = bookgen.run_intake(project_id=project_id, run_date="2026-03-04")
    resolved = bookgen.run_prompt_pack_resolve(intake=intake)
    planning = bookgen.run_bible_outline(intake=intake, resolved=resolved)
    bookgen.run_chapter_drafting(project_id=project_id)
    bookgen.run_chapter_review(project_id=project_id)
    bookgen.run_assembly_export(project_id=project_id)
    release = bookgen.approve_installment(project_id=project_id, decision="lock", note="Freeze after approval.")

    with pytest.raises(RuntimeError, match="manuscript_locked"):
        bookgen.run_chapter_drafting(project_id=project_id)

    with pytest.raises(RuntimeError, match="manuscript_locked"):
        bookgen.run_chapter_review(project_id=project_id)

    with pytest.raises(RuntimeError, match="manuscript_locked"):
        bookgen.run_assembly_export(project_id=project_id)

    with pytest.raises(RuntimeError, match="manuscript_locked"):
        bookgen.run_bible_outline(intake=intake, resolved=resolved)

    assert release["status"] == "manuscript_locked"
    assert release["approval"]["locked"] is True
    assert planning["installment_id"] == release["installment_id"]


def test_continuity_review_flags_adjacent_semantic_break():
    outline = {
        "chapters": [
            {"chapter_index": 1, "title": "Closed Door"},
            {"chapter_index": 2, "title": "Silent Corridor"},
        ]
    }

    report = bookgen._continuity_review_report(
        project_id="closed-session-series",
        installment_id="book-01",
        outline=outline,
        sections=[
            "# Closed Door\n\nAdrian Cole waited outside the committee room while Maya Rios counted the votes and Lang reviewed the sealed docket.",
            "# Silent Corridor\n\nVolcanic ash drifted over a fishing village as gulls broke across the harbor and Elias watched trawlers disappear into sleet.",
        ],
    )

    assert report["pass_status"] == "FAIL"
    assert report["checks"]["adjacent_semantic_continuity_ok"] is False
    assert report["issues"]["weak_semantic_links"] == [{"previous_chapter_index": 1, "current_chapter_index": 2}]
    assert report["adjacent_semantic_checks"][0]["pass_status"] == "FAIL"


def test_bounded_run_respects_explicit_chapter_word_target():
    spec = _closed_session_title_spec()
    spec["chapter_count"] = 2
    spec["chapter_word_target"] = 3900
    title_artifacts = bookgen._build_title_artifacts(project_id=spec["project_id"], bookspec=spec, run_date="2026-03-04")
    constitution = bookgen._synthesize_constitution(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        series_title=title_artifacts["slate"]["series_title"],
        title_strategy=title_artifacts["strategy"],
    )
    installment_pack = bookgen._synthesize_installment_pack(
        project_id=spec["project_id"],
        run_date="2026-03-04",
        bookspec=spec,
        constitution=constitution,
        selected_title_block=title_artifacts["selected_title_block"],
    )
    outline = bookgen._build_outline(
        project_id=spec["project_id"],
        bookspec=spec,
        constitution=constitution,
        installment_pack=installment_pack,
    )
    ledgers = bookgen._build_initial_ledgers(spec["project_id"], installment_pack, constitution)
    chapter_pack = bookgen._build_chapter_pack(
        project_id=spec["project_id"],
        constitution=constitution,
        installment_pack=installment_pack,
        outline_chapter=outline["chapters"][0],
        ledgers=ledgers,
    )

    assert installment_pack["output_targets"]["chapter_word_target"] == 3900
    assert chapter_pack["scene_constraints"]["word_count_target"] == 3900
    assert chapter_pack["draft_contract"]["word_count"]["target"] == 3900


def test_inspect_chapter_exposes_scene_artifacts(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    monkeypatch.chdir(root)

    backing: dict[str, bytes] = {}
    monkeypatch.setattr(bookgen, "ObjectStore", lambda: MemoryStore(backing))
    monkeypatch.setattr(bookgen, "_commit_stage_checkpoint", lambda **kwargs: None)
    monkeypatch.setattr(bookgen, "_log_mlflow_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        bookgen,
        "get_settings",
        lambda: SimpleNamespace(
            bookgen_generation_preset="smoke",
            bookgen_use_llm=False,
            bookgen_llm_chapter_limit=0,
            bookgen_eval_use_llm=False,
            bookgen_eval_llm_chapter_limit=0,
            bookgen_rewrite_use_llm=False,
            bookgen_rewrite_llm_chapter_limit=0,
            bookgen_structural_retry_limit=0,
            bookgen_title_critic_use_llm=False,
            bookgen_title_critic_shortlist_size=5,
            bookgen_allow_lock_override=False,
            llm_endpoint=None,
            mlflow_tracking_uri="http://127.0.0.1:15000",
            mlflow_local_tracking_uri="http://127.0.0.1:15000",
        ),
    )

    project_id = "demo-thriller-scenes-001"
    bookspec = json.loads((root / "docs/bookgen/bookspec.sample.json").read_text(encoding="utf-8"))
    bookspec["chapter_count"] = 1
    backing[f"inputs/{project_id}/bookspec.json"] = json.dumps(bookspec).encode("utf-8")
    backing["prompt-packs/thriller/v1/manifest.json"] = json.dumps(
        {"genre": "thriller", "version": "v1", "structure": {"acts": 3}}
    ).encode("utf-8")
    backing["rubrics/thriller/v1/rubric.json"] = json.dumps(
        {
            "genre": "thriller",
            "version": "v1",
            "chapter_min_words": 120,
            "pass_overall": 7.0,
            "hard_fail": [
                {"category": "world_rule_compliance", "min_score": 7.0},
                {"category": "character_consistency", "min_score": 7.0},
            ],
        }
    ).encode("utf-8")

    intake = bookgen.run_intake(project_id=project_id, run_date="2026-03-04")
    resolved = bookgen.run_prompt_pack_resolve(intake=intake)
    bookgen.run_bible_outline(intake=intake, resolved=resolved)
    bookgen.run_chapter_drafting(project_id=project_id)
    inspected = bookgen.inspect_chapter(project_id=project_id, chapter_index=1)

    assert inspected["scene_cards"]["chapter_id"] == "ch-01"
    assert len(inspected["scene_cards"]["scenes"]) >= 3
    assert inspected["scene_beats"]["chapter_id"] == "ch-01"
    assert len(inspected["scene_beats"]["beats"]) >= 9
    assert inspected["scene_qc"]["chapter_id"] == "ch-01"
