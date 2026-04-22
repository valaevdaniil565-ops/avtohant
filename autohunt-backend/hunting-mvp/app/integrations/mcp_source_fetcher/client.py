from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from typing import Any

from .schemas import NormalizedItem, SourceFetchResult


class MCPSourceFetcherClient:
    """
    Minimal local MCP client over STDIO for MVP.
    """

    def __init__(self, command: str | None = None) -> None:
        self.command = command or os.getenv(
            "MCP_SOURCE_FETCHER_COMMAND",
            "python -m app.integrations.mcp_source_fetcher.server",
        )

    def fetch_url(self, url: str) -> SourceFetchResult:
        req = {"id": 1, "method": "fetch_url", "params": {"url": url}}
        out = self._call(req)
        if not out.get("ok"):
            return SourceFetchResult(ok=False, source_type="unknown", source_url=url, error=str(out.get("error") or "MCP error"))
        res = out.get("result") or {}
        items = [NormalizedItem(**it) for it in (res.get("items") or [])]
        return SourceFetchResult(
            ok=bool(res.get("ok")),
            source_type=str(res.get("source_type") or "unknown"),
            source_url=str(res.get("source_url") or url),
            items=items,
            metadata=res.get("metadata") or {},
            error=res.get("error"),
        )

    def _call(self, req: dict[str, Any]) -> dict[str, Any]:
        cmd = shlex.split(self.command)
        if cmd and cmd[0] in {"python", "python3"}:
            cmd[0] = sys.executable
        payload = json.dumps(req, ensure_ascii=False) + "\n"
        proc = subprocess.run(
            cmd,
            input=payload,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=float(os.getenv("SOURCE_FETCHER_TIMEOUT_S", "20")) + 5.0,
        )
        if proc.returncode != 0:
            err = (proc.stderr or proc.stdout or "").strip()
            return {"ok": False, "error": f"MCP process failed: {err[:300]}"}
        line = (proc.stdout or "").strip().splitlines()
        if not line:
            return {"ok": False, "error": "MCP process returned no output"}
        try:
            return json.loads(line[-1])
        except Exception:
            return {"ok": False, "error": "Invalid MCP JSON response"}
