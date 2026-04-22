from __future__ import annotations

import json
import logging
import os
import re
import sys
from dataclasses import asdict
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

import requests

from .normalizers import (
    csv_bytes_to_items,
    csv_bytes_to_items_with_summary,
    docx_bytes_to_text,
    html_to_text,
    pdf_bytes_to_text,
    xlsx_bytes_to_items,
    xlsx_bytes_to_items_with_summary,
)
from .schemas import NormalizedItem, SourceFetchResult
from .url_router import classify_url, is_host_allowed

log = logging.getLogger("mcp_source_fetcher.server")
_SESSION = requests.Session()
_SESSION.trust_env = False


class FetchError(Exception):
    pass


def _allowed_domains() -> set[str]:
    raw = os.getenv("SOURCE_FETCHER_ALLOWED_DOMAINS", "").strip()
    if not raw:
        return set()
    return {x.strip().lower() for x in raw.split(",") if x.strip()}


def _timeout_s() -> int:
    return int(os.getenv("SOURCE_FETCHER_TIMEOUT_S", "1200"))


def _max_bytes() -> int:
    return int(os.getenv("SOURCE_FETCHER_MAX_BYTES", "25000000"))


def _max_items() -> int:
    return int(os.getenv("SOURCE_FETCHER_MAX_ITEMS", "200"))


def _get_bytes(url: str) -> tuple[bytes, str]:
    resp = _SESSION.get(url, timeout=_timeout_s(), stream=True, allow_redirects=True)
    if resp.status_code in (401, 403):
        raise FetchError("Источник недоступен (403/401). Возможно, файл приватный.")
    if resp.status_code == 404:
        raise FetchError("Источник не найден (404).")
    if resp.status_code >= 400:
        raise FetchError(f"HTTP {resp.status_code} for URL")

    ctype = (resp.headers.get("content-type") or "").lower()
    max_b = _max_bytes()
    chunks: list[bytes] = []
    total = 0
    for ch in resp.iter_content(chunk_size=65536):
        if not ch:
            continue
        total += len(ch)
        if total > max_b:
            raise FetchError(f"Слишком большой файл: > {max_b} bytes")
        chunks.append(ch)
    return b"".join(chunks), ctype


def _sheet_export_url(url: str, *, fmt: str = "xlsx") -> str:
    # docs.google.com/spreadsheets/d/<id>/...
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    if not m:
        raise FetchError("Не удалось извлечь spreadsheet id")
    sid = m.group(1)
    p = urlparse(url)
    qs = parse_qs(p.query)
    gid = qs.get("gid", [None])[0]
    base = f"https://docs.google.com/spreadsheets/d/{sid}/export?format={fmt}"
    if gid and fmt.lower() == "csv":
        return f"{base}&gid={gid}"
    return base


def _doc_export_url(url: str) -> str:
    m = re.search(r"/document/d/([a-zA-Z0-9-_]+)", url)
    if not m:
        raise FetchError("Не удалось извлечь document id")
    did = m.group(1)
    return f"https://docs.google.com/document/d/{did}/export?format=txt"


def _drive_download_url(url: str) -> str:
    # drive.google.com/file/d/<id>/view
    m = re.search(r"/file/d/([a-zA-Z0-9-_]+)", url)
    if not m:
        raise FetchError("Не удалось извлечь drive file id")
    fid = m.group(1)
    return f"https://drive.google.com/uc?export=download&id={fid}"


def _yandex_public_download_url(url: str) -> str:
    resp = _SESSION.get(
        "https://cloud-api.yandex.net/v1/disk/public/resources/download",
        params={"public_key": url},
        timeout=_timeout_s(),
    )
    if resp.status_code in (401, 403):
        raise FetchError("Публичная ссылка Яндекс.Диска недоступна (403/401).")
    if resp.status_code == 404:
        raise FetchError("Публичная ссылка Яндекс.Диска не найдена (404).")
    if resp.status_code >= 400:
        raise FetchError(f"Yandex Disk API HTTP {resp.status_code}")
    data = resp.json()
    href = str(data.get("href") or "").strip()
    if not href:
        raise FetchError("Yandex Disk API did not return download href")
    return href


def fetch_url(url: str) -> SourceFetchResult:
    allowed = _allowed_domains()
    if not is_host_allowed(url, allowed):
        return SourceFetchResult(ok=False, source_type="blocked", source_url=url, error="URL blocked by allowlist/security")

    source_type = classify_url(url)
    try:
        if source_type == "google_sheet":
            try:
                data, ctype = _get_bytes(_sheet_export_url(url, fmt="xlsx"))
                items, import_summary = xlsx_bytes_to_items_with_summary(data, url, max_items=_max_items())
                return SourceFetchResult(
                    ok=True,
                    source_type=source_type,
                    source_url=url,
                    items=items,
                    metadata={"content_type": ctype, "items_count": len(items), "import_summary": import_summary},
                )
            except Exception:
                log.exception("google_sheet xlsx export failed, falling back to csv: %s", url)
                data, ctype = _get_bytes(_sheet_export_url(url, fmt="csv"))
                items, import_summary = csv_bytes_to_items_with_summary(data, url, max_items=_max_items())
                return SourceFetchResult(
                    ok=True,
                    source_type=source_type,
                    source_url=url,
                    items=items,
                    metadata={"content_type": ctype, "items_count": len(items), "import_summary": import_summary},
                )

        if source_type == "google_doc":
            data, ctype = _get_bytes(_doc_export_url(url))
            text = data.decode("utf-8", errors="replace").strip()
            item = NormalizedItem(text=text, metadata={"source_url": url}) if text else None
            return SourceFetchResult(
                ok=True,
                source_type=source_type,
                source_url=url,
                items=[item] if item else [],
                metadata={"content_type": ctype, "items_count": 1 if item else 0},
            )

        if source_type == "google_drive_file":
            data, ctype = _get_bytes(_drive_download_url(url))
            return _normalize_direct(data, url, ctype, source_type=source_type)

        if source_type == "yandex_disk_public":
            data, ctype = _get_bytes(_yandex_public_download_url(url))
            return _normalize_direct(data, url, ctype, source_type=source_type)

        data, ctype = _get_bytes(url)
        return _normalize_direct(data, url, ctype, source_type=source_type)

    except FetchError as e:
        return SourceFetchResult(ok=False, source_type=source_type, source_url=url, error=str(e))
    except requests.Timeout:
        return SourceFetchResult(ok=False, source_type=source_type, source_url=url, error="Timeout while fetching URL")
    except Exception as e:
        log.exception("fetch_url failed: %s", url)
        return SourceFetchResult(ok=False, source_type=source_type, source_url=url, error=f"Unhandled fetch error: {type(e).__name__}")


def _normalize_direct(data: bytes, source_url: str, content_type: str, *, source_type: str) -> SourceFetchResult:
    ct = content_type.lower()
    p = urlparse(source_url)
    path = (p.path or "").lower()
    items: list[NormalizedItem] = []
    kind = source_type

    if "text/csv" in ct or path.endswith(".csv"):
        items, import_summary = csv_bytes_to_items_with_summary(data, source_url, max_items=_max_items())
        kind = "direct_csv"
    elif "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" in ct or path.endswith(".xlsx"):
        items, import_summary = xlsx_bytes_to_items_with_summary(data, source_url, max_items=_max_items())
        kind = "direct_xlsx"
    elif "application/vnd.openxmlformats-officedocument.wordprocessingml.document" in ct or path.endswith(".docx"):
        txt = docx_bytes_to_text(data)
        if txt:
            items = [NormalizedItem(text=txt, metadata={"source_url": source_url})]
        kind = "direct_docx"
        import_summary = None
    elif "application/pdf" in ct or path.endswith(".pdf"):
        txt = pdf_bytes_to_text(data)
        if txt:
            items = [NormalizedItem(text=txt, metadata={"source_url": source_url})]
        kind = "direct_pdf"
        import_summary = None
    elif "text/plain" in ct or path.endswith(".txt"):
        txt = data.decode("utf-8", errors="replace").strip()
        if txt:
            items = [NormalizedItem(text=txt, metadata={"source_url": source_url})]
        kind = "direct_txt"
        import_summary = None
    elif "text/html" in ct or path.endswith(".html") or path.endswith(".htm"):
        txt = html_to_text(data.decode("utf-8", errors="replace"))
        if txt:
            items = [NormalizedItem(text=txt, metadata={"source_url": source_url})]
        kind = "direct_html"
        import_summary = None
    else:
        # For unsupported binary content return an explicit error.
        return SourceFetchResult(
            ok=False,
            source_type=source_type,
            source_url=source_url,
            error=f"Unsupported content type for MVP: {content_type or 'unknown'}",
        )

    return SourceFetchResult(
        ok=True,
        source_type=kind,
        source_url=source_url,
        items=items,
        metadata={
            "content_type": content_type,
            "items_count": len(items),
            "bytes": len(data),
            "import_summary": import_summary,
        },
    )


def _handle_request(req: dict[str, Any]) -> dict[str, Any]:
    rid = req.get("id")
    method = req.get("method")
    params = req.get("params") or {}
    if method == "fetch_url":
        url = str(params.get("url") or "").strip()
        if not url:
            return {"id": rid, "ok": False, "error": "url is required"}
        res = fetch_url(url)
        return {"id": rid, "ok": True, "result": _asdict_fetch_result(res)}
    return {"id": rid, "ok": False, "error": f"Unknown method: {method}"}


def _asdict_fetch_result(res: SourceFetchResult) -> dict[str, Any]:
    payload = asdict(res)
    return payload


def main() -> None:
    # Keep logs away from stdout to avoid protocol corruption.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), stream=sys.stderr)

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
            out = _handle_request(req)
        except Exception as e:
            out = {"id": None, "ok": False, "error": f"Invalid request: {type(e).__name__}"}
        sys.stdout.write(json.dumps(out, ensure_ascii=False) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
