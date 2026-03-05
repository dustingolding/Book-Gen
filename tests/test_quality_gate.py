from app.services import quality_gate


class DummyStore:
    def __init__(self, verification: dict):
        self._verification = verification

    def get_json(self, key: str) -> dict:
        return self._verification


def test_quality_gate_passes(monkeypatch):
    verification = {
        "citation_coverage": 0.95,
        "issues": [],
        "item_count": 10,
        "items_with_citation": 10,
    }
    monkeypatch.setattr(quality_gate, "ObjectStore", lambda: DummyStore(verification))

    result = quality_gate.run("2026-01-01")
    assert result["status"] == "passed"


def test_quality_gate_fails_coverage(monkeypatch):
    verification = {
        "citation_coverage": 0.85,
        "issues": [],
    }
    monkeypatch.setattr(quality_gate, "ObjectStore", lambda: DummyStore(verification))

    try:
        quality_gate.run("2026-01-01")
        assert False, "expected runtime error"
    except RuntimeError as exc:
        assert "citation coverage" in str(exc)
