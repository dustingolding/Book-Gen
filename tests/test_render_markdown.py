from app.services import render_markdown


class DummyStore:
    def __init__(self, factpack: dict, verification: dict):
        self._factpack = factpack
        self._verification = verification
        self.saved = {}

    def get_json(self, key: str) -> dict:
        if key.endswith("/factpack.json"):
            return self._factpack
        if key.endswith("/verification.json"):
            return self._verification
        raise KeyError(key)

    def put_text(self, key: str, text: str, content_type: str = "text/markdown") -> None:
        self.saved[key] = {"text": text, "content_type": content_type}


def test_render_markdown_writes_document(monkeypatch):
    factpack = {
        "meta": {
            "runtime_target_minutes": 40,
            "yesterday_window_et": {
                "start": "2026-01-01T00:00:00-05:00",
                "end": "2026-01-02T00:00:00-05:00",
            },
        },
        "show_identity": {
            "name": "SideLine Wire DailyCast",
            "hosts": {"primary": ["Evan Cole", "Marcus Reed"]},
        },
        "coverage": {"allocations": [{"sport": "nfl", "minutes": 12.5, "phase": "REGULAR_SEASON"}]},
        "yesterday_results": [{"title": "A at B"}],
        "today_matchups": [{"title": "C at D"}],
        "major_news": [{"title": "News"}],
    }
    verification = {
        "citation_coverage": 1.0,
        "approved": True,
        "item_count": 3,
        "items_with_citation": 3,
    }
    store = DummyStore(factpack, verification)
    monkeypatch.setattr(render_markdown, "ObjectStore", lambda: store)

    result = render_markdown.run("2026-01-01")
    assert result["key"] == "publish/2026-01-01/dailycast.md"
    assert "# SideLine Wire DailyCast - Fact Pack" in store.saved[result["key"]]["text"]
