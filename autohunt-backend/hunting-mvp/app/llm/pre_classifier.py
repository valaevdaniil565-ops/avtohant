from __future__ import annotations

import re
from dataclasses import dataclass, field


_VACANCY_HINTS = (
    "нужен",
    "нужна",
    "нужно",
    "ищем",
    "требуется",
    "вакансия",
    "позиция",
    "проект",
    "отклик",
    "обязанности",
    "требования",
)

_GRADE_RE = re.compile(r"(?i)\b(junior|middle\+?|senior|lead|architect|staff|principal|head)\b")
_EXP_RE = re.compile(r"(?i)\b(\d+(?:[.,]\d+)?)\s*(?:лет?|years?)\s+опыт\w*\b")
_RATE_RE = re.compile(r"(?i)\b(?:ставк[аи]|rate)\s*[:\-]?\s*(\d{3,5}(?:[.,]\d+)?)\s*(?:rub|₽|р\b)?")
_CV_RE = re.compile(r"(?i)\b(?:cv[_\-][A-Za-z0-9]+|CV_CODE:\s*cv[_\-][A-Za-z0-9]+)\b")

_DOTNET_RE = re.compile(r"(?i)\.?\s*net\b")
_MIDDLE_PLUS_RE = re.compile(r"(?i)\bmiddle\s*\+\b")
_DECIMAL_COMMA_RE = re.compile(r"(\d),(\d)")
_CV_CODE_RE = re.compile(r"(?i)/(cv[_\-][A-Za-z0-9]+)\b")
_DASH_RE = re.compile(r"[–—−]")
_SPACE_RE = re.compile(r"\s{2,}")


@dataclass
class PreClassifyResult:
    kind: str
    confidence: float
    normalized_text: str
    is_confident: bool
    reason: str
    signals: dict[str, bool] = field(default_factory=dict)


@dataclass
class HybridDecision:
    kind: str
    source: str
    needs_llm: bool


def normalize_short_bench_line(text: str) -> str:
    t = (text or "").strip()
    t = _DASH_RE.sub("-", t)
    t = _DOTNET_RE.sub("DotNet", t)
    t = re.sub(r"\bC\s*#\b", "C#", t, flags=re.IGNORECASE)
    t = _MIDDLE_PLUS_RE.sub("Middle+", t)
    t = _DECIMAL_COMMA_RE.sub(r"\1.\2", t)
    t = t.replace("₽", " RUB")
    t = _CV_CODE_RE.sub(r"CV_CODE: \1", t)
    t = _SPACE_RE.sub(" ", t).strip()
    return t


def pre_classify_bench_line(text: str) -> PreClassifyResult:
    normalized = normalize_short_bench_line(text)
    low = normalized.lower()

    has_grade = bool(_GRADE_RE.search(normalized))
    has_exp = bool(_EXP_RE.search(normalized))
    has_rate = bool(_RATE_RE.search(normalized))
    has_cv = bool(_CV_RE.search(normalized))
    vacancy_hint = any(h in low for h in _VACANCY_HINTS)

    # Conservative scoring: require multiple card-like signals.
    score = 0.0
    if has_grade:
        score += 0.20
    if has_exp:
        score += 0.25
    if has_rate:
        score += 0.25
    if has_cv:
        score += 0.25
    if vacancy_hint:
        score -= 0.35

    # Role/name prefix is weak signal for short cards.
    prefix_words = len(normalized.split())
    if prefix_words >= 4 and not vacancy_hint:
        score += 0.10

    score = max(0.0, min(1.0, score))
    confident = score >= 0.70 and not vacancy_hint

    reason = "no_match"
    if confident:
        reason = "bench_card_rule_match"
    elif vacancy_hint:
        reason = "vacancy_hints_present"
    elif score >= 0.45:
        reason = "weak_bench_signal"

    return PreClassifyResult(
        kind="BENCH" if confident else "UNKNOWN",
        confidence=score,
        normalized_text=normalized,
        is_confident=confident,
        reason=reason,
        signals={
            "has_grade": has_grade,
            "has_exp": has_exp,
            "has_rate": has_rate,
            "has_cv": has_cv,
            "vacancy_hint": vacancy_hint,
        },
    )


def split_line_wise_bench_items(text: str) -> list[tuple[int, str]]:
    lines = []
    for i, ln in enumerate((text or "").splitlines(), start=1):
        v = ln.strip()
        if not v:
            continue
        v = re.sub(r"^[\-•\d\).\s]+", "", v).strip()
        if v:
            lines.append((i, v))

    if len(lines) < 2:
        return []

    hits: list[tuple[int, str]] = []
    for idx, ln in lines:
        r = pre_classify_bench_line(ln)
        if r.confidence >= 0.55 and not r.signals.get("vacancy_hint", False):
            hits.append((idx, r.normalized_text))

    # List-like only if majority is bench-like.
    if len(hits) >= 2 and len(hits) >= max(2, int(len(lines) * 0.6)):
        return hits
    return []


def decide_hybrid_classification(
    pre: PreClassifyResult,
    *,
    forced_type: str | None = None,
    llm_label: str | None = None,
    fallback_threshold: float = 0.55,
) -> HybridDecision:
    if forced_type:
        return HybridDecision(kind=forced_type, source="forced", needs_llm=False)

    if pre.is_confident:
        return HybridDecision(kind="BENCH", source="rule", needs_llm=False)

    if llm_label is None:
        return HybridDecision(kind="UNKNOWN", source="pending_llm", needs_llm=True)

    kind = llm_label
    source = "llm"
    if kind == "OTHER" and pre.confidence >= fallback_threshold and not pre.signals.get("vacancy_hint", False):
        kind = "BENCH"
        source = "fallback"
    return HybridDecision(kind=kind, source=source, needs_llm=False)
