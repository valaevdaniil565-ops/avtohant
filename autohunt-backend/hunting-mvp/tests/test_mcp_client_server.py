import os

from app.integrations.mcp_source_fetcher.client import MCPSourceFetcherClient


def test_mcp_client_server_stdio_roundtrip_blocked_url():
    old = os.environ.get("MCP_SOURCE_FETCHER_COMMAND")
    os.environ["MCP_SOURCE_FETCHER_COMMAND"] = "python -m app.integrations.mcp_source_fetcher.server"
    try:
        c = MCPSourceFetcherClient()
        res = c.fetch_url("http://localhost:9999/x.csv")
        assert not res.ok
        assert "blocked" in (res.error or "").lower()
    finally:
        if old is None:
            os.environ.pop("MCP_SOURCE_FETCHER_COMMAND", None)
        else:
            os.environ["MCP_SOURCE_FETCHER_COMMAND"] = old
