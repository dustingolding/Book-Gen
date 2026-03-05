from __future__ import annotations

from types import SimpleNamespace

from app.clients import llm


class _StubResponse:
    def __init__(self) -> None:
        self.status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {"choices": [{"message": {"content": "ok"}}]}


def test_openai_endpoint_uses_max_completion_tokens(monkeypatch):
    captured: dict = {}

    class _StubClient:
        def __init__(self, timeout: float) -> None:
            del timeout

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            return False

        def post(self, endpoint: str, headers: dict, json: dict):
            del headers
            captured["endpoint"] = endpoint
            captured["payload"] = json
            return _StubResponse()

    monkeypatch.setattr(
        llm,
        "get_settings",
        lambda: SimpleNamespace(
            llm_endpoint="https://api.openai.com/v1/chat/completions",
            llm_api_key="test",
            llm_model="gpt-5",
            llm_provider_profile="gpt5",
            llm_reasoning_effort=None,
            llm_timeout_seconds=10,
            llm_max_retries=1,
            llm_strict_mode=True,
        ),
    )
    monkeypatch.setattr(llm.httpx, "Client", _StubClient)

    client = llm.LLMClient()
    out = client.complete("s", "u", max_completion_tokens=123, temperature=0.2)
    assert out == "ok"
    assert captured["payload"]["model"] == "gpt-5"
    assert "max_completion_tokens" in captured["payload"]
    assert captured["payload"]["max_completion_tokens"] == 256
    assert "max_tokens" not in captured["payload"]
    assert captured["payload"]["reasoning_effort"] == "low"


def test_non_openai_endpoint_uses_max_tokens(monkeypatch):
    captured: dict = {}

    class _StubClient:
        def __init__(self, timeout: float) -> None:
            del timeout

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            return False

        def post(self, endpoint: str, headers: dict, json: dict):
            del headers
            captured["endpoint"] = endpoint
            captured["payload"] = json
            return _StubResponse()

    monkeypatch.setattr(
        llm,
        "get_settings",
        lambda: SimpleNamespace(
            llm_endpoint="https://api.groq.com/openai/v1/chat/completions",
            llm_api_key="test",
            llm_model="llama-3.3-70b-versatile",
            llm_provider_profile="default",
            llm_reasoning_effort=None,
            llm_timeout_seconds=10,
            llm_max_retries=1,
            llm_strict_mode=True,
        ),
    )
    monkeypatch.setattr(llm.httpx, "Client", _StubClient)

    client = llm.LLMClient()
    out = client.complete("s", "u", max_completion_tokens=321, temperature=0.2)
    assert out == "ok"
    assert "max_tokens" in captured["payload"]
    assert captured["payload"]["max_tokens"] == 321
    assert "max_completion_tokens" not in captured["payload"]


def test_gpt5_forces_default_temperature(monkeypatch):
    captured: dict = {}

    class _StubClient:
        def __init__(self, timeout: float) -> None:
            del timeout

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            return False

        def post(self, endpoint: str, headers: dict, json: dict):
            del endpoint, headers
            captured["payload"] = json
            return _StubResponse()

    monkeypatch.setattr(
        llm,
        "get_settings",
        lambda: SimpleNamespace(
            llm_endpoint="https://api.openai.com/v1/chat/completions",
            llm_api_key="test",
            llm_model="gpt-5",
            llm_provider_profile="gpt5",
            llm_reasoning_effort="minimal",
            llm_timeout_seconds=10,
            llm_max_retries=1,
            llm_strict_mode=True,
        ),
    )
    monkeypatch.setattr(llm.httpx, "Client", _StubClient)

    client = llm.LLMClient()
    out = client.complete("s", "u", max_completion_tokens=64, temperature=0.2)
    assert out == "ok"
    assert captured["payload"]["temperature"] == 1.0
    assert captured["payload"]["reasoning_effort"] == "minimal"


def test_default_profile_keeps_openai_max_tokens(monkeypatch):
    captured: dict = {}

    class _StubClient:
        def __init__(self, timeout: float) -> None:
            del timeout

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            return False

        def post(self, endpoint: str, headers: dict, json: dict):
            del endpoint, headers
            captured["payload"] = json
            return _StubResponse()

    monkeypatch.setattr(
        llm,
        "get_settings",
        lambda: SimpleNamespace(
            llm_endpoint="https://api.openai.com/v1/chat/completions",
            llm_api_key="test",
            llm_model="gpt-4o",
            llm_provider_profile="default",
            llm_reasoning_effort=None,
            llm_timeout_seconds=10,
            llm_max_retries=1,
            llm_strict_mode=True,
        ),
    )
    monkeypatch.setattr(llm.httpx, "Client", _StubClient)

    client = llm.LLMClient()
    out = client.complete("s", "u", max_completion_tokens=80, temperature=0.2)
    assert out == "ok"
    assert captured["payload"]["temperature"] == 0.2
    assert "max_tokens" in captured["payload"]
    assert "max_completion_tokens" not in captured["payload"]
    assert "reasoning_effort" not in captured["payload"]


def test_gpt5_retries_with_higher_budget_after_empty_content(monkeypatch):
    payloads: list[dict] = []

    class _RetryResponse:
        def __init__(self, body: dict) -> None:
            self.status_code = 200
            self._body = body

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return self._body

    class _StubClient:
        def __init__(self, timeout: float) -> None:
            del timeout
            self.calls = 0

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            return False

        def post(self, endpoint: str, headers: dict, json: dict):
            del endpoint, headers
            payloads.append(dict(json))
            self.calls += 1
            if self.calls == 1:
                return _RetryResponse({"choices": [{"finish_reason": "length", "message": {"content": ""}}]})
            return _RetryResponse({"choices": [{"finish_reason": "stop", "message": {"content": "usable output"}}]})

    monkeypatch.setattr(
        llm,
        "get_settings",
        lambda: SimpleNamespace(
            llm_endpoint="https://api.openai.com/v1/chat/completions",
            llm_api_key="test",
            llm_model="gpt-5",
            llm_provider_profile="gpt5",
            llm_reasoning_effort="minimal",
            llm_timeout_seconds=10,
            llm_max_retries=2,
            llm_strict_mode=True,
        ),
    )
    monkeypatch.setattr(llm.httpx, "Client", _StubClient)

    client = llm.LLMClient()
    out = client.complete("s", "u", max_completion_tokens=300, temperature=0.2)
    assert out == "usable output"
    assert len(payloads) == 2
    assert payloads[0]["max_completion_tokens"] == 300
    assert payloads[1]["max_completion_tokens"] > payloads[0]["max_completion_tokens"]
    assert payloads[1]["reasoning_effort"] == "low"
