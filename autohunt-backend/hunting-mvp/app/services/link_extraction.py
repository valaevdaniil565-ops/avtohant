from __future__ import annotations

from app.integrations.mcp_source_fetcher.url_router import extract_urls


def extract_external_urls(text: str) -> list[str]:
    urls = extract_urls(text or "")
    # Telegram links are managed by existing delete/source flow; don't treat as external fetch targets.
    out = []
    for u in urls:
        if "t.me/" in u:
            continue
        out.append(u)
    return out
