from __future__ import annotations

import ipaddress
import re
import socket
from urllib.parse import urlparse

from .schemas import UrlMatch

_URL_RE = re.compile(r"https?://[^\s<>()]+", re.IGNORECASE)
_GOOGLE_URL_RE = re.compile(
    r"https?://docs\.google\.com/"
    r"(?:spreadsheets/d|document/d|forms/d|file/d)/"
    r"[A-Za-z0-9_\-\s]+"
    r"(?:/[^\s<>()]*)?",
    re.IGNORECASE,
)


def extract_urls(text: str) -> list[str]:
    source_text = _repair_wrapped_google_urls_in_text(text or "")
    urls: list[str] = []
    for m in _GOOGLE_URL_RE.findall(source_text):
        urls.append(_normalize_extracted_url(m))
    for m in _URL_RE.findall(source_text):
        u = _normalize_extracted_url(m)
        urls.append(u)
    # preserve order, deduplicate
    seen = set()
    out = []
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _normalize_extracted_url(url: str) -> str:
    u = re.sub(r"\s+", "", (url or "").strip())
    u = u.rstrip(").,;]>")
    u = re.sub(r"/edit7gid=", "/edit?gid=", u, flags=re.IGNORECASE)
    u = re.sub(r"/editgid=", "/edit?gid=", u, flags=re.IGNORECASE)
    u = re.sub(r"/edit&gid=", "/edit?gid=", u, flags=re.IGNORECASE)
    return u


def _repair_wrapped_google_urls_in_text(text: str) -> str:
    return re.sub(
        r"(https?://docs\.google\.com/spreadsheets/d/[A-Za-z0-9_-]+)\s+([A-Za-z0-9_-]+/edit(?:7gid=|gid=|[?#]gid=)[^\s<>()]+)",
        lambda m: m.group(1) + m.group(2).replace("edit7gid=", "edit?gid=").replace("editgid=", "edit?gid="),
        text or "",
        flags=re.IGNORECASE,
    )


def classify_url(url: str) -> str:
    p = urlparse(url)
    host = (p.netloc or "").lower()
    path = (p.path or "").lower()

    if host == "docs.google.com" and path.startswith("/spreadsheets/"):
        return "google_sheet"
    if host == "docs.google.com" and path.startswith("/document/"):
        return "google_doc"
    if host == "drive.google.com" and "/file/" in path:
        return "google_drive_file"
    if host in {"disk.yandex.ru", "disk.360.yandex.ru", "yadi.sk", "disk.yandex.com"}:
        return "yandex_disk_public"

    for ext, name in [
        (".pdf", "direct_pdf"),
        (".docx", "direct_docx"),
        (".xlsx", "direct_xlsx"),
        (".csv", "direct_csv"),
        (".txt", "direct_txt"),
    ]:
        if path.endswith(ext):
            return name

    return "generic_url"


def is_host_allowed(url: str, allowed_domains: set[str]) -> bool:
    p = urlparse(url)
    if p.scheme not in ("http", "https"):
        return False
    host = (p.hostname or "").lower()
    if not host:
        return False
    if _is_local_or_private_host(host):
        return False

    if not allowed_domains:
        return True

    for d in allowed_domains:
        d = d.lower().strip()
        if not d:
            continue
        if host == d or host.endswith("." + d):
            return True
    return False


def route_urls(urls: list[str], allowed_domains: set[str]) -> tuple[list[UrlMatch], list[str]]:
    accepted: list[UrlMatch] = []
    errors: list[str] = []
    for url in urls:
        if not is_host_allowed(url, allowed_domains):
            errors.append(f"URL not allowed by policy: {url}")
            continue
        accepted.append(UrlMatch(url=url, source_type=classify_url(url)))
    return accepted, errors


def _is_local_or_private_host(host: str) -> bool:
    if host in ("localhost",):
        return True
    if host.endswith(".local"):
        return True

    try:
        ip = ipaddress.ip_address(host)
        return _is_private_ip(ip)
    except ValueError:
        pass

    try:
        infos = socket.getaddrinfo(host, None)
    except Exception:
        # unresolved host should be handled by requester later
        return False

    for i in infos:
        ip_s = i[4][0]
        try:
            ip = ipaddress.ip_address(ip_s)
        except ValueError:
            continue
        if _is_private_ip(ip):
            return True
    return False


def _is_private_ip(ip: ipaddress._BaseAddress) -> bool:
    return bool(
        ip.is_loopback
        or ip.is_link_local
        or ip.is_private
        or ip.is_multicast
        or ip.is_reserved
    )
