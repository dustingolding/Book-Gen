import json
import logging
import random
import time

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)


class LLMClient:
    def __init__(self) -> None:
        cfg = get_settings()
        self.endpoint = cfg.llm_endpoint
        self.api_key = cfg.llm_api_key
        self.model = cfg.llm_model
        self.provider_profile = (cfg.llm_provider_profile or "default").strip().lower()
        self.reasoning_effort = (cfg.llm_reasoning_effort or "").strip().lower() or None
        self.timeout_seconds = cfg.llm_timeout_seconds
        self.max_retries = cfg.llm_max_retries
        self.strict_mode = cfg.llm_strict_mode
        self.fallback_count = 0
        self.fallback_reasons: list[str] = []

    @staticmethod
    def _fallback_output() -> str:
        return json.dumps(
            {
                "segments": [
                    {
                        "segment": "Opening",
                        "text": "Welcome to Side Line Wire Dailycast. Here is your sports briefing.",
                        "citations": [],
                    }
                ]
            }
        )

    def _normalized_endpoint(self) -> str:
        endpoint = (self.endpoint or "").strip()
        if not endpoint:
            return ""
        if endpoint.endswith("/chat/completions"):
            return endpoint
        if endpoint.endswith("/openai/v1"):
            return f"{endpoint}/chat/completions"
        if endpoint == "https://api.groq.com":
            return "https://api.groq.com/openai/v1/chat/completions"
        return endpoint

    def _resolved_model(self, endpoint: str) -> str:
        if self.model:
            return self.model
        # Groq does not host OpenAI model IDs like gpt-4o-mini.
        if "api.groq.com" in endpoint:
            return "llama-3.3-70b-versatile"
        return "gpt-4o-mini"

    def _use_gpt5_contract(self, endpoint: str, model: str) -> bool:
        return self.provider_profile == "gpt5" and "api.openai.com" in endpoint and model.startswith("gpt-5")

    def _token_budget_key(self, endpoint: str, model: str) -> str:
        if self._use_gpt5_contract(endpoint, model):
            return "max_completion_tokens"
        return "max_tokens"

    @staticmethod
    def _boost_token_budget(current: int) -> int:
        # Increase budget aggressively enough to recover from reasoning-length truncation.
        return min(12000, max(512, int(current * 1.6)))

    def complete(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        max_completion_tokens: int | None = None,
        temperature: float = 0.2,
    ) -> str:
        endpoint = self._normalized_endpoint()
        if not endpoint:
            if self.strict_mode:
                raise RuntimeError("LLM endpoint is not configured")
            self.fallback_count += 1
            self.fallback_reasons.append("no_endpoint")
            return self._fallback_output()

        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        model = self._resolved_model(endpoint)
        use_gpt5_contract = self._use_gpt5_contract(endpoint, model)
        # gpt-5 chat-completions currently supports only default temperature=1.
        effective_temperature = 1.0 if use_gpt5_contract else temperature
        payload = {
            "model": model,
            "temperature": effective_temperature,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        token_budget_key = self._token_budget_key(endpoint, model)
        if max_completion_tokens:
            token_budget = int(max_completion_tokens)
            if use_gpt5_contract and token_budget < 256:
                token_budget = 256
            payload[token_budget_key] = token_budget
        if use_gpt5_contract:
            payload["reasoning_effort"] = self.reasoning_effort or "low"

        with httpx.Client(timeout=float(self.timeout_seconds)) as client:
            for attempt in range(1, self.max_retries + 1):
                try:
                    started = time.time()
                    resp = client.post(endpoint, headers=headers, json=payload)
                    resp.raise_for_status()
                    data = resp.json()
                    elapsed = time.time() - started
                    logger.info(
                        "llm_request_ok endpoint=%s model=%s attempt=%s elapsed=%.2fs",
                        endpoint,
                        payload["model"],
                        attempt,
                        elapsed,
                    )
                    choice = data["choices"][0]
                    finish_reason = str(choice.get("finish_reason", "") or "").strip().lower()
                    content = str(choice["message"].get("content", ""))
                    if not content.strip():
                        if use_gpt5_contract and attempt < self.max_retries and token_budget_key in payload:
                            payload[token_budget_key] = self._boost_token_budget(int(payload[token_budget_key]))
                            payload["reasoning_effort"] = "low"
                        raise RuntimeError("LLM response contained empty content")
                    if use_gpt5_contract and finish_reason == "length" and len(content.strip()) < 400:
                        if attempt < self.max_retries and token_budget_key in payload:
                            payload[token_budget_key] = self._boost_token_budget(int(payload[token_budget_key]))
                            payload["reasoning_effort"] = "low"
                            raise RuntimeError("LLM response was truncated before usable output")
                    return content
                except httpx.HTTPStatusError as exc:
                    status = exc.response.status_code
                    if status in (429, 500, 502, 503, 504) and attempt < self.max_retries:
                        retry_after = exc.response.headers.get("Retry-After")
                        if retry_after and retry_after.isdigit():
                            sleep_seconds = min(45, max(1, int(retry_after)))
                        else:
                            # Exponential backoff with small jitter.
                            sleep_seconds = min(45, (2 ** attempt) + random.uniform(0.1, 0.9))
                        logger.warning(
                            "llm_request_retry endpoint=%s status=%s attempt=%s sleep=%.1fs",
                            endpoint,
                            status,
                            attempt,
                            sleep_seconds,
                        )
                        time.sleep(sleep_seconds)
                        continue
                    logger.warning(
                        "llm_request_failed_fallback endpoint=%s status=%s error=%s body=%s",
                        endpoint,
                        status,
                        str(exc),
                        exc.response.text[:500],
                    )
                    if self.strict_mode:
                        raise RuntimeError(f"LLM request failed (status={status})") from exc
                    self.fallback_count += 1
                    self.fallback_reasons.append(f"http_status:{status}")
                    return self._fallback_output()
                except Exception as exc:
                    logger.warning(
                        "llm_request_attempt_failed endpoint=%s attempt=%s/%s error=%s",
                        endpoint,
                        attempt,
                        self.max_retries,
                        f"{type(exc).__name__}: {exc}",
                    )
                    if attempt < self.max_retries:
                        time.sleep(1.0 * attempt)
                        continue
                    logger.warning(
                        "llm_request_failed_fallback endpoint=%s error=%s",
                        endpoint,
                        str(exc),
                    )
                    if self.strict_mode:
                        if isinstance(exc, RuntimeError) and str(exc).startswith("LLM response"):
                            raise
                        raise RuntimeError(f"LLM request failed ({type(exc).__name__})") from exc
                    self.fallback_count += 1
                    self.fallback_reasons.append(f"http_error:{type(exc).__name__}")
                    return self._fallback_output()
