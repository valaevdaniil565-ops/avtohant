from app.integrations.mcp_source_fetcher import server


class _Resp:
    def __init__(self, status_code: int):
        self.status_code = status_code
        self.headers = {"content-type": "text/plain"}

    def iter_content(self, chunk_size: int = 65536):
        yield b""


def test_private_link_returns_human_error(monkeypatch):
    monkeypatch.setattr(server, "_allowed_domains", lambda: set())
    monkeypatch.setattr(server.requests, "get", lambda *a, **k: _Resp(403))
    res = server.fetch_url("https://example.com/private.txt")
    assert not res.ok
    assert "приват" in (res.error or "").lower() or "403" in (res.error or "")
