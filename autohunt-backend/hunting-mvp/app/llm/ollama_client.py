import os
import json
import logging
from typing import Any, Dict, Optional

import aiohttp
import requests

log = logging.getLogger(__name__)


class OllamaClient:
    """
    Клиент для Ollama с авто-выбором endpoint.

    Поддерживаем env как у тебя:
      - OLLAMA_HOST (например http://localhost:11434)
      - LLM_MODEL   (например llama3:8b)

    Пытаемся по очереди:
      1) POST /api/chat
      2) POST /api/generate
      3) POST /v1/chat/completions (OpenAI-compatible)
    """

    def __init__(
        self,
        host: Optional[str] = None,
        model: Optional[str] = None,
        timeout_s: int = 120,
        base_url: Optional[str] = None,
        embed_model: Optional[str] = None,
    ) -> None:
        # backward-compatible: legacy code passes base_url/embed_model kwargs
        self.host = (host or base_url or os.getenv("OLLAMA_HOST") or "http://localhost:11434").rstrip("/")
        self.model = model or os.getenv("LLM_MODEL") or "llama3:8b"
        self.embed_model = embed_model or os.getenv("EMBED_MODEL") or "nomic-embed-text"
        self.timeout_s = timeout_s

    def _post_json_sync(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.host}{path}"
        r = requests.post(url, json=payload, timeout=self.timeout_s)
        body = r.text
        if r.status_code >= 400:
            raise RuntimeError(f"{path} HTTP {r.status_code}: {body[:500]}")
        try:
            return json.loads(body)
        except Exception:
            return {"raw": body}

    async def _post_json(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.host}{path}"
        timeout = aiohttp.ClientTimeout(total=self.timeout_s)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=payload) as r:
                text = await r.text()
                if r.status >= 400:
                    raise aiohttp.ClientResponseError(
                        request_info=r.request_info,
                        history=r.history,
                        status=r.status,
                        message=text[:500],
                        headers=r.headers,
                    )
                try:
                    return json.loads(text)
                except Exception:
                    return {"raw": text}

    async def preflight(self) -> Dict[str, Any]:
        """
        Пробуем определить, что вообще отвечает на host.
        В нормальной Ollama /api/tags существует (GET), но мы делаем POST как fallback.
        """
        results: Dict[str, Any] = {"host": self.host, "model": self.model, "checks": {}}

        # try POST /api/tags (некоторые прокси могут не поддержать GET)
        try:
            data = await self._post_json("/api/tags", {})
            results["checks"]["/api/tags(POST)"] = "OK"
            # не логируем полный json, он может быть большой
        except Exception as e:
            results["checks"]["/api/tags(POST)"] = f"FAIL: {type(e).__name__}"

        # try /api/chat (пустой запрос — ожидаемо может упасть по модели, но важно 404/не 404)
        try:
            _ = await self._post_json(
                "/api/chat",
                {
                    "model": self.model,
                    "messages": [{"role": "user", "content": "ping"}],
                    "stream": False,
                },
            )
            results["checks"]["/api/chat"] = "OK"
        except aiohttp.ClientResponseError as e:
            results["checks"]["/api/chat"] = f"HTTP {e.status}"
        except Exception as e:
            results["checks"]["/api/chat"] = f"FAIL: {type(e).__name__}"

        # try /api/generate
        try:
            _ = await self._post_json(
                "/api/generate",
                {"model": self.model, "prompt": "ping", "stream": False},
            )
            results["checks"]["/api/generate"] = "OK"
        except aiohttp.ClientResponseError as e:
            results["checks"]["/api/generate"] = f"HTTP {e.status}"
        except Exception as e:
            results["checks"]["/api/generate"] = f"FAIL: {type(e).__name__}"

        # try /v1/chat/completions
        try:
            _ = await self._post_json(
                "/v1/chat/completions",
                {
                    "model": self.model,
                    "messages": [{"role": "user", "content": "ping"}],
                    "temperature": 0.0,
                    "max_tokens": 8,
                },
            )
            results["checks"]["/v1/chat/completions"] = "OK"
        except aiohttp.ClientResponseError as e:
            results["checks"]["/v1/chat/completions"] = f"HTTP {e.status}"
        except Exception as e:
            results["checks"]["/v1/chat/completions"] = f"FAIL: {type(e).__name__}"

        return results

    async def chat(
        self,
        system_prompt: str,
        user_text: str,
        temperature: float = 0.0,
        num_predict: int = 1024,
        json_mode: bool = False,
    ) -> str:
        # 1) /api/chat
        try:
            payload = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_text},
                ],
                "stream": False,
                "options": {"temperature": temperature, "num_predict": num_predict},
            }
            if json_mode:
                payload["format"] = "json"

            data = await self._post_json("/api/chat", payload)
            msg = data.get("message") or {}
            if isinstance(msg.get("content"), str):
                return msg["content"].strip()
            if isinstance(data.get("response"), str):
                return data["response"].strip()
            return json.dumps(data, ensure_ascii=False).strip()

        except aiohttp.ClientResponseError as e:
            if e.status != 404:
                raise
            log.warning("/api/chat not found (404) on %s, trying /api/generate", self.host)

        # 2) /api/generate
        try:
            prompt = f"{system_prompt}\n\nUSER_MESSAGE:\n{user_text}"
            payload = {
                "model": self.model,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": temperature, "num_predict": num_predict},
            }
            if json_mode:
                payload["format"] = "json"

            data = await self._post_json("/api/generate", payload)
            if isinstance(data.get("response"), str):
                return data["response"].strip()
            if isinstance(data.get("raw"), str):
                return data["raw"].strip()
            return json.dumps(data, ensure_ascii=False).strip()

        except aiohttp.ClientResponseError as e:
            if e.status != 404:
                raise
            log.warning("/api/generate not found (404) on %s, trying /v1/chat/completions", self.host)

        # 3) OpenAI-compatible /v1/chat/completions
        data = await self._post_json(
            "/v1/chat/completions",
            {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_text},
                ],
                "temperature": temperature,
                "max_tokens": num_predict,
            },
        )
        try:
            return data["choices"][0]["message"]["content"].strip()
        except Exception:
            return json.dumps(data, ensure_ascii=False).strip()

    def generate(
        self,
        system: str,
        prompt: str,
        temperature: float = 0.0,
        max_tokens: int = 1024,
    ) -> str:
        """
        Legacy sync API used by scripts/handlers.
        """
        # 1) /api/chat
        try:
            data = self._post_json_sync(
                "/api/chat",
                {
                    "model": self.model,
                    "messages": [
                        {"role": "system", "content": system or ""},
                        {"role": "user", "content": prompt or ""},
                    ],
                    "stream": False,
                    "options": {"temperature": float(temperature), "num_predict": int(max_tokens)},
                },
            )
            msg = data.get("message") or {}
            if isinstance(msg.get("content"), str):
                return msg["content"].strip()
            if isinstance(data.get("response"), str):
                return data["response"].strip()
            return json.dumps(data, ensure_ascii=False).strip()
        except Exception:
            pass

        # 2) /api/generate
        prompt_full = f"{system or ''}\n\nUSER_MESSAGE:\n{prompt or ''}"
        data = self._post_json_sync(
            "/api/generate",
            {
                "model": self.model,
                "prompt": prompt_full,
                "stream": False,
                "options": {"temperature": float(temperature), "num_predict": int(max_tokens)},
            },
        )
        if isinstance(data.get("response"), str):
            return data["response"].strip()
        if isinstance(data.get("raw"), str):
            return data["raw"].strip()
        return json.dumps(data, ensure_ascii=False).strip()

    def embed(self, text_in: str) -> list[float]:
        """
        Legacy sync embeddings API used by scripts/handlers.
        Returns [] when embedding endpoint is unavailable.
        """
        if not self.embed_model:
            return []

        # 1) /api/embeddings
        try:
            data = self._post_json_sync(
                "/api/embeddings",
                {"model": self.embed_model, "prompt": text_in},
            )
            emb = data.get("embedding")
            if isinstance(emb, list):
                return emb
        except Exception:
            pass

        # 2) /api/embed
        try:
            data = self._post_json_sync(
                "/api/embed",
                {"model": self.embed_model, "input": text_in},
            )
            embs = data.get("embeddings")
            if isinstance(embs, list) and embs and isinstance(embs[0], list):
                return embs[0]
        except Exception:
            pass

        return []


ollama = OllamaClient()
