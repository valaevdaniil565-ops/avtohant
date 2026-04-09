from __future__ import annotations

import re
from typing import Any, Optional

from sqlalchemy import text

MATCH_THRESHOLD = 0.50

_MATCH_PRIMARY_EXACT_ALIASES = {
    "reactnative": "reactnative",
    "flutter": "flutter",
    "ios": "ios",
    "android": "android",
    "swift": "swift",
    "kotlin": "kotlin",
    "dotnet": "dotnet",
    "csharp": "dotnet",
    "vbnet": "dotnet",
    "aspnet": "dotnet",
    "cpp": "cplusplus",
    "cplusplus": "cplusplus",
    "c": "c",
    "java": "java",
    "python": "python",
    "php": "php",
    "go": "go",
    "golang": "go",
    "ruby": "ruby",
    "scala": "scala",
    "javascript": "javascript",
    "js": "javascript",
    "typescript": "typescript",
    "ts": "typescript",
    "nodejs": "nodejs",
    "react": "react",
    "reactjs": "react",
    "angular": "angular",
    "angularjs": "angular",
    "vue": "vue",
    "vuejs": "vue",
    "qa": "qa",
    "aqa": "qa",
    "devops": "devops",
    "sre": "devops",
    "systemanalyst": "systemanalyst",
    "системныйаналитик": "systemanalyst",
    "sa": "systemanalyst",
    "ca": "systemanalyst",
    "businessanalyst": "businessanalyst",
    "бизнесаналитик": "businessanalyst",
    "ba": "businessanalyst",
    "dataanalyst": "dataanalyst",
    "designer": "designer",
    "uxui": "designer",
    "uidesigner": "designer",
    "uxdesigner": "designer",
    "projectmanager": "projectmanager",
    "pm": "projectmanager",
    "productowner": "productowner",
    "po": "productowner",
    "productmanager": "productmanager",
    "salesforce": "salesforce",
    "1c": "1c",
    "sap": "sap",
}
_MATCH_PRIMARY_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("reactnative", re.compile(r"(?i)\breact\s*native\b")),
    ("systemanalyst", re.compile(r"(?i)\b(system\s*analyst|системн\w*\s+аналит\w*)\b")),
    ("businessanalyst", re.compile(r"(?i)\b(business\s*analyst|бизнес\w*[- ]?аналит\w*)\b")),
    ("dataanalyst", re.compile(r"(?i)\b(data\s*analyst|аналит\w*\s+данн\w*)\b")),
    ("productowner", re.compile(r"(?i)\b(product\s*owner)\b")),
    ("productmanager", re.compile(r"(?i)\b(product\s*manager)\b")),
    ("projectmanager", re.compile(r"(?i)\b(project\s*manager|delivery\s*manager|scrum\s*master)\b")),
    ("designer", re.compile(r"(?i)\b(designer|ux/ui|ui/ux|ux\s*designer|ui\s*designer|дизайнер)\b")),
    ("devops", re.compile(r"(?i)\b(devops|platform\s*engineer)\b")),
    ("qa", re.compile(r"(?i)\b(aqa|qa|quality\s*assurance|tester|тестиров\w*)\b")),
    ("flutter", re.compile(r"(?i)\bflutter\b")),
    ("ios", re.compile(r"(?i)\b(ios|objective[- ]?c)\b")),
    ("android", re.compile(r"(?i)\bandroid\b")),
    ("swift", re.compile(r"(?i)\bswift\b")),
    ("kotlin", re.compile(r"(?i)\bkotlin\b")),
    ("dotnet", re.compile(r"(?i)(?:^|[^a-z0-9])(?:\.net|dotnet|c#|csharp|vb\.?net|asp\.?net)(?:[^a-z0-9]|$)")),
    ("cplusplus", re.compile(r"(?i)\b(c\+\+|cplusplus)\b")),
    ("c", re.compile(r"(?i)(?:^|[^a-z0-9])c(?:[^a-z0-9]|$)")),
    ("java", re.compile(r"(?i)\bjava\b(?!\s*script)")),
    ("python", re.compile(r"(?i)\bpython\b")),
    ("php", re.compile(r"(?i)\bphp\b")),
    ("go", re.compile(r"(?i)\bgo(lang)?\b")),
    ("ruby", re.compile(r"(?i)\bruby\b")),
    ("scala", re.compile(r"(?i)\bscala\b")),
    ("nodejs", re.compile(r"(?i)\bnode\.?\s*js\b")),
    ("react", re.compile(r"(?i)\breact(?:\.js)?\b")),
    ("angular", re.compile(r"(?i)\bangular(?:\.js)?\b")),
    ("vue", re.compile(r"(?i)\b(vue(?:\.js)?|nuxt(?:\.js)?)\b")),
    ("typescript", re.compile(r"(?i)\btypescript\b")),
    ("javascript", re.compile(r"(?i)\bjavascript\b")),
    ("salesforce", re.compile(r"(?i)\bsalesforce\b")),
    ("1c", re.compile(r"(?i)(?:^|[^a-z0-9])(1c|1с)(?:[^a-z0-9]|$)")),
    ("sap", re.compile(r"(?i)\bsap\b")),
)
_MATCH_TOOLING_EXACT_ALIASES = {
    "docker": "docker",
    "git": "git",
    "jira": "jira",
    "confluence": "confluence",
    "figma": "figma",
    "postman": "postman",
    "jenkins": "jenkins",
    "nginx": "nginx",
    "webpack": "webpack",
    "vite": "vite",
    "storybook": "storybook",
    "pvsstudio": "pvsstudio",
    "html": "html",
    "css": "css",
    "scss": "scss",
    "sass": "sass",
    "styledcomponents": "styledcomponents",
    "mui": "mui",
    "antd": "antd",
    "recharts": "recharts",
    "axios": "axios",
    "ajax": "ajax",
    "websocket": "websocket",
    "protobuf": "protobuf",
}
_MATCH_TOOLING_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("pvsstudio", re.compile(r"(?i)\bpvs[- ]?studio\b")),
    ("docker", re.compile(r"(?i)\bdocker\b")),
    ("git", re.compile(r"(?i)\bgit\b")),
    ("jira", re.compile(r"(?i)\bjira\b")),
    ("confluence", re.compile(r"(?i)\bconfluence\b")),
    ("figma", re.compile(r"(?i)\bfigma\b")),
    ("postman", re.compile(r"(?i)\bpostman\b")),
    ("jenkins", re.compile(r"(?i)\bjenkins\b")),
    ("nginx", re.compile(r"(?i)\bnginx\b")),
    ("webpack", re.compile(r"(?i)\bwebpack\b")),
    ("vite", re.compile(r"(?i)\bvite\b")),
    ("storybook", re.compile(r"(?i)\bstorybook\b")),
    ("styledcomponents", re.compile(r"(?i)\bstyled[- ]?components\b")),
)
_MATCH_SECONDARY_STOPWORDS = {"unknown", "developer", "engineer", "specialist", "candidate", "vacancy", "роль", "stack"}
_GRADE_LEVELS = {
    "intern": 0,
    "junior": 1,
    "middle-": 2,
    "middle": 3,
    "middle+": 4,
    "senior": 5,
    "lead": 6,
    "architect": 7,
    "head": 8,
    "staff": 9,
    "principal": 10,
}
_REMOTE_LOCATION_TOKENS = {"remote", "удаленка", "удаленно", "удалённо", "remotely"}
_RUSSIA_LOCATION_TOKENS = {"rf", "russia", "россия", "ru"}
_URLISH_RE = re.compile(r"(?i)(https?://|docs\.google\.com/|t\.me/)")


def _looks_like_urlish(value: str) -> bool:
    return bool(_URLISH_RE.search(str(value or "").strip()))


def _normalize_match_token(value: str) -> str:
    s = (value or "").strip().lower()
    s = s.replace("1с", "1c")
    s = s.replace(".net", " dotnet ")
    s = s.replace("c#", " csharp ")
    s = s.replace("c++", " cpp ")
    s = s.replace("react native", " reactnative ")
    s = s.replace("node.js", " nodejs ")
    s = s.replace("react.js", " reactjs ")
    s = s.replace("vue.js", " vuejs ")
    s = s.replace("angular.js", " angularjs ")
    s = re.sub(r"[^a-z0-9а-я]+", "", s)
    return s


def _unique_keep_order(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = _normalize_match_token(value)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _iter_match_texts(value: Any) -> list[str]:
    if not value:
        return []
    raw_values = value if isinstance(value, list) else [value]
    texts: list[str] = []
    seen: set[str] = set()
    for raw in raw_values:
        text = str(raw or "").strip()
        if not text or _looks_like_urlish(text):
            continue
        for candidate in [text, *re.split(r"[\n,;/|]|(?:\s{2,})", text)]:
            cleaned = str(candidate or "").strip()
            if not cleaned or _looks_like_urlish(cleaned):
                continue
            key = cleaned.lower()
            if key in seen:
                continue
            seen.add(key)
            texts.append(cleaned)
    return texts


def _primary_labels_for_text(text: str) -> list[str]:
    candidate = str(text or "").strip()
    if not candidate:
        return []
    norm = _normalize_match_token(candidate)
    exact = _MATCH_PRIMARY_EXACT_ALIASES.get(norm)
    if exact:
        return [exact]
    labels: list[str] = []
    for label, pattern in _MATCH_PRIMARY_PATTERNS:
        if pattern.search(candidate):
            labels.append(label)
    return _unique_keep_order(labels)


def _tooling_labels_for_text(text: str) -> list[str]:
    candidate = str(text or "").strip()
    if not candidate:
        return []
    norm = _normalize_match_token(candidate)
    exact = _MATCH_TOOLING_EXACT_ALIASES.get(norm)
    if exact:
        return [exact]
    labels: list[str] = []
    for label, pattern in _MATCH_TOOLING_PATTERNS:
        if pattern.search(candidate):
            labels.append(label)
    return _unique_keep_order(labels)


def _secondary_tokens_for_text(text: str) -> list[str]:
    candidate = str(text or "").strip()
    if not candidate or _looks_like_urlish(candidate):
        return []
    norm = _normalize_match_token(candidate)
    if not norm or norm.isdigit() or len(norm) < 2 or norm in _MATCH_SECONDARY_STOPWORDS:
        return []
    return [norm]


def _coerce_match_entity(entity_or_stack: Any) -> dict[str, Any]:
    if isinstance(entity_or_stack, dict):
        return entity_or_stack
    return {"stack": entity_or_stack}


def _build_stack_profile(entity_or_stack: Any, *, entity_kind: str = "GENERIC") -> dict[str, list[str]]:
    entity = _coerce_match_entity(entity_or_stack)
    role_text = str(entity.get("role") or "").strip()
    stack_texts = _iter_match_texts(entity.get("stack"))
    role_texts = _iter_match_texts(role_text)
    primary_from_stack: list[str] = []
    secondary: list[str] = []
    tooling: list[str] = []
    for text in stack_texts:
        primary = _primary_labels_for_text(text)
        if primary:
            primary_from_stack.extend(primary)
            continue
        tool = _tooling_labels_for_text(text)
        if tool:
            tooling.extend(tool)
            continue
        secondary.extend(_secondary_tokens_for_text(text))
    primary_from_role: list[str] = []
    for text in role_texts:
        primary_from_role.extend(_primary_labels_for_text(text))
    if entity_kind == "VACANCY":
        primary = _unique_keep_order(primary_from_stack)[:1] or _unique_keep_order(primary_from_role)[:1]
    else:
        primary = _unique_keep_order(primary_from_stack + primary_from_role)
    return {
        "primary": primary,
        "secondary": _unique_keep_order(secondary),
        "tooling": _unique_keep_order(tooling),
    }


def _stack_match_details(required_entity: Any, candidate_entity: Any, *, required_kind: str = "GENERIC", candidate_kind: str = "GENERIC") -> dict[str, Any]:
    required_profile = _build_stack_profile(required_entity, entity_kind=required_kind)
    candidate_profile = _build_stack_profile(candidate_entity, entity_kind=candidate_kind)
    required_primary = set(required_profile["primary"])
    candidate_primary = set(candidate_profile["primary"])
    overlap = required_primary & candidate_primary
    return {
        "passes": bool(required_primary and candidate_primary and overlap),
        "required_profile": required_profile,
        "candidate_profile": candidate_profile,
        "overlap": sorted(overlap),
    }


def _normalize_grade(value: Any) -> Optional[str]:
    candidate = str(value or "").strip().lower()
    if not candidate:
        return None
    if "middle+" in candidate or "middle +" in candidate or "mid+" in candidate or "мидл+" in candidate:
        return "middle+"
    if "middle-" in candidate or "middle -" in candidate or "mid-" in candidate or "мидл-" in candidate:
        return "middle-"
    if "middle" in candidate or "mid" in candidate or "мидл" in candidate:
        return "middle"
    if "senior" in candidate or "sen" in candidate or "сеньор" in candidate:
        return "senior"
    if "junior" in candidate or "jun" in candidate or "джун" in candidate:
        return "junior"
    if "lead" in candidate or "лид" in candidate:
        return "lead"
    if "architect" in candidate or "архитект" in candidate:
        return "architect"
    if "head" in candidate:
        return "head"
    if "staff" in candidate:
        return "staff"
    if "principal" in candidate:
        return "principal"
    if "intern" in candidate or "стаж" in candidate:
        return "intern"
    return None


def _grade_match_score(left_grade: Any, right_grade: Any) -> float:
    left = _normalize_grade(left_grade)
    right = _normalize_grade(right_grade)
    if not left or not right:
        return 0.0
    diff = abs(_GRADE_LEVELS[left] - _GRADE_LEVELS[right])
    if diff == 0:
        return 1.0
    if diff == 1:
        return 0.6
    if diff == 2:
        return 0.25
    return 0.0


def _rate_anchor(entity_or_hit: Any) -> Optional[float]:
    entity = _coerce_match_entity(entity_or_hit)
    for key in ("rate_max", "rate_min"):
        value = entity.get(key)
        if isinstance(value, (int, float)) and value > 0:
            return float(value)
        if isinstance(value, str):
            digits = re.sub(r"[^\d.]+", "", value)
            if digits:
                try:
                    parsed = float(digits)
                except ValueError:
                    continue
                if parsed > 0:
                    return parsed
    return None


def _budget_alignment_score(budget: Optional[float], ask: Optional[float]) -> float:
    if not budget or not ask:
        return 0.0
    if ask <= budget:
        return 1.0
    overflow = (ask - budget) / max(budget, 1.0)
    if overflow <= 0.10:
        return 0.6
    if overflow <= 0.25:
        return 0.25
    return 0.0


def _ascending_rate_tiebreak(value: Any) -> float:
    rate = _rate_anchor(value)
    return float("-inf") if not rate else -rate


def _location_tokens(value: Any) -> set[str]:
    text_value = str(value or "").strip().lower()
    if not text_value:
        return set()
    tokens: set[str] = set()
    for part in re.split(r"[\s,;/|]+", text_value):
        norm = _normalize_match_token(part)
        if norm:
            tokens.add(norm)
    return tokens


def _location_match_score(left_value: Any, right_value: Any) -> float:
    left_tokens = _location_tokens(left_value)
    right_tokens = _location_tokens(right_value)
    if not left_tokens or not right_tokens:
        return 0.0
    if left_tokens & right_tokens:
        return 1.0
    if (_REMOTE_LOCATION_TOKENS & left_tokens) and (_REMOTE_LOCATION_TOKENS & right_tokens):
        return 1.0
    if (_RUSSIA_LOCATION_TOKENS & left_tokens) and (_RUSSIA_LOCATION_TOKENS & right_tokens):
        return 0.5
    return 0.0


def _secondary_overlap_score(required_profile: dict[str, list[str]], candidate_profile: dict[str, list[str]]) -> float:
    required = set(required_profile.get("secondary") or [])
    candidate = set(candidate_profile.get("secondary") or [])
    if not required or not candidate:
        return 0.0
    overlap = required & candidate
    if not overlap:
        return 0.0
    return min(1.0, len(overlap) / max(1, min(len(required), 3)))


def _semantic_similarity(hit: dict[str, Any]) -> float:
    value = hit.get("semantic_sim", hit.get("sim"))
    try:
        sim = float(value or 0.0)
    except (TypeError, ValueError):
        sim = 0.0
    return max(0.0, min(1.0, sim))


def _final_business_score(*, secondary_score: float, grade_score: float, rate_score: float, location_score: float, semantic_score: float) -> float:
    score = 0.50
    score += 0.15 * max(0.0, min(1.0, secondary_score))
    score += 0.15 * max(0.0, min(1.0, grade_score))
    score += 0.10 * max(0.0, min(1.0, rate_score))
    score += 0.05 * max(0.0, min(1.0, location_score))
    score += 0.05 * max(0.0, min(1.0, semantic_score))
    return round(max(0.0, min(1.0, score)), 4)


def _merge_specialist_hits(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for group in groups:
        for hit in group or []:
            sid = str(hit.get("id") or "").strip()
            if not sid:
                continue
            existing = merged.get(sid)
            if existing is None:
                merged[sid] = dict(hit)
                continue
            candidate = dict(existing)
            existing_sim = float(existing.get("sim") or 0.0)
            hit_sim = float(hit.get("sim") or 0.0)
            if hit_sim > existing_sim:
                candidate.update(hit)
            candidate["sim"] = max(existing_sim, hit_sim)
            candidate["is_internal"] = bool(existing.get("is_internal")) or bool(hit.get("is_internal"))
            candidate["is_own_bench_source"] = bool(existing.get("is_own_bench_source")) or bool(hit.get("is_own_bench_source"))
            if not candidate.get("url"):
                candidate["url"] = hit.get("url") or existing.get("url")
            if not candidate.get("source_display"):
                candidate["source_display"] = hit.get("source_display") or existing.get("source_display")
            merged[sid] = candidate
    return list(merged.values())


def search_specialists(engine, query_emb: Optional[list[float]], query_text: str, limit: int, *, own_bench_url: Optional[str] = None, vector_dim: int = 768, vector_str_fn=None) -> list[dict[str, Any]]:
    own_url = (own_bench_url or "").strip()

    def _search_active_own_bench_specialists(local_limit: int) -> list[dict[str, Any]]:
        if not own_url:
            return []
        sql = text(
            """
            SELECT
              s.id, s.role, s.stack, s.grade, s.rate_min, s.rate_max, s.currency, s.location, s.is_internal,
              0.0 AS sim,
              (
                SELECT message_url FROM sources
                WHERE entity_type='specialist' AND entity_id=s.id AND message_url IS NOT NULL
                ORDER BY created_at DESC
                LIMIT 1
              ) AS url,
              (
                SELECT COALESCE(source_meta ->> 'source_display', message_url)
                FROM sources
                WHERE entity_type='specialist' AND entity_id=s.id
                ORDER BY created_at DESC
                LIMIT 1
              ) AS source_display,
              TRUE AS is_own_bench_source
            FROM specialists s
            WHERE s.status='active'
              AND (s.expires_at IS NULL OR s.expires_at > NOW())
              AND EXISTS(
                SELECT 1 FROM own_specialists_registry reg
                WHERE reg.specialist_id = s.id
                  AND reg.is_active = TRUE
                  AND COALESCE(reg.source_url, '') = :own_url
              )
            ORDER BY s.updated_at DESC
            LIMIT :limit
            """
        )
        with engine.begin() as connection:
            rows = connection.execute(sql, {"own_url": own_url, "limit": local_limit}).mappings().all()
        return [dict(row) for row in rows]

    if query_emb and len(query_emb) == vector_dim and vector_str_fn is not None:
        sql = text(
            """
            SELECT
              s.id, s.role, s.stack, s.grade, s.rate_min, s.rate_max, s.currency, s.location, s.is_internal,
              GREATEST(0.0, LEAST(1.0, 1 - (s.embedding <=> CAST(:q AS vector)))) AS sim,
              (
                SELECT message_url FROM sources
                WHERE entity_type='specialist' AND entity_id=s.id AND message_url IS NOT NULL
                ORDER BY created_at DESC
                LIMIT 1
              ) AS url,
              (
                SELECT COALESCE(source_meta ->> 'source_display', message_url)
                FROM sources
                WHERE entity_type='specialist' AND entity_id=s.id
                ORDER BY created_at DESC
                LIMIT 1
              ) AS source_display,
              CASE
                WHEN :own_url = '' THEN FALSE
                ELSE EXISTS(
                  SELECT 1 FROM own_specialists_registry reg
                  WHERE reg.specialist_id = s.id
                    AND reg.is_active = TRUE
                    AND COALESCE(reg.source_url, '') = :own_url
                )
              END AS is_own_bench_source
            FROM specialists s
            WHERE s.status='active'
              AND (s.expires_at IS NULL OR s.expires_at > NOW())
              AND s.embedding IS NOT NULL
            ORDER BY s.embedding <=> CAST(:q AS vector)
            LIMIT :limit
            """
        )
        with engine.begin() as connection:
            rows = connection.execute(sql, {"q": vector_str_fn(query_emb), "limit": limit, "own_url": own_url}).mappings().all()
        return _merge_specialist_hits([dict(row) for row in rows], _search_active_own_bench_specialists(max(limit, 100)))

    sql = text(
        """
        SELECT
          s.id, s.role, s.stack, s.grade, s.rate_min, s.rate_max, s.currency, s.location, s.is_internal,
          0.0 AS sim,
          (
            SELECT message_url FROM sources
            WHERE entity_type='specialist' AND entity_id=s.id AND message_url IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 1
          ) AS url,
          (
            SELECT COALESCE(source_meta ->> 'source_display', message_url)
            FROM sources
            WHERE entity_type='specialist' AND entity_id=s.id
            ORDER BY created_at DESC
            LIMIT 1
          ) AS source_display,
          CASE
            WHEN :own_url = '' THEN FALSE
            ELSE EXISTS(
              SELECT 1 FROM own_specialists_registry reg
              WHERE reg.specialist_id = s.id
                AND reg.is_active = TRUE
                AND COALESCE(reg.source_url, '') = :own_url
            )
          END AS is_own_bench_source
        FROM specialists s
        WHERE s.status='active'
          AND (s.expires_at IS NULL OR s.expires_at > NOW())
          AND (
            LOWER(s.role) LIKE LOWER(:q)
            OR LOWER(COALESCE(s.description,'')) LIKE LOWER(:q)
            OR LOWER(s.original_text) LIKE LOWER(:q)
          )
        ORDER BY s.updated_at DESC
        LIMIT :limit
        """
    )
    with engine.begin() as connection:
        rows = connection.execute(sql, {"q": f"%{query_text[:80]}%", "limit": limit, "own_url": own_url}).mappings().all()
    return _merge_specialist_hits([dict(row) for row in rows], _search_active_own_bench_specialists(max(limit, 100)))


def search_vacancies(engine, query_emb: Optional[list[float]], query_text: str, limit: int, *, vector_dim: int = 768, vector_str_fn=None) -> list[dict[str, Any]]:
    if query_emb and len(query_emb) == vector_dim and vector_str_fn is not None:
        sql = text(
            """
            SELECT
              v.id, v.role, v.stack, v.grade, v.rate_min, v.rate_max, v.currency, v.location,
              GREATEST(0.0, LEAST(1.0, 1 - (v.embedding <=> CAST(:q AS vector)))) AS sim,
              (
                SELECT message_url FROM sources
                WHERE entity_type='vacancy' AND entity_id=v.id AND message_url IS NOT NULL
                ORDER BY created_at DESC
                LIMIT 1
              ) AS url,
              (
                SELECT COALESCE(source_meta ->> 'source_display', message_url)
                FROM sources
                WHERE entity_type='vacancy' AND entity_id=v.id
                ORDER BY created_at DESC
                LIMIT 1
              ) AS source_display
            FROM vacancies v
            WHERE v.status='active'
              AND (v.expires_at IS NULL OR v.expires_at > NOW())
              AND v.embedding IS NOT NULL
            ORDER BY v.embedding <=> CAST(:q AS vector)
            LIMIT :limit
            """
        )
        with engine.begin() as connection:
            rows = connection.execute(sql, {"q": vector_str_fn(query_emb), "limit": limit}).mappings().all()
        return [dict(row) for row in rows]

    sql = text(
        """
        SELECT
          v.id, v.role, v.stack, v.grade, v.rate_min, v.rate_max, v.currency, v.location,
          0.0 AS sim,
          (
            SELECT message_url FROM sources
            WHERE entity_type='vacancy' AND entity_id=v.id AND message_url IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 1
          ) AS url,
          (
            SELECT COALESCE(source_meta ->> 'source_display', message_url)
            FROM sources
            WHERE entity_type='vacancy' AND entity_id=v.id
            ORDER BY created_at DESC
            LIMIT 1
          ) AS source_display
        FROM vacancies v
        WHERE v.status='active'
          AND (v.expires_at IS NULL OR v.expires_at > NOW())
          AND (
            LOWER(v.role) LIKE LOWER(:q)
            OR LOWER(COALESCE(v.description,'')) LIKE LOWER(:q)
            OR LOWER(v.original_text) LIKE LOWER(:q)
          )
        ORDER BY v.updated_at DESC
        LIMIT :limit
        """
    )
    with engine.begin() as connection:
        rows = connection.execute(sql, {"q": f"%{query_text[:80]}%", "limit": limit}).mappings().all()
    return [dict(row) for row in rows]


def rank_specialist_hit(vacancy: Any, hit: dict[str, Any]) -> Optional[dict[str, Any]]:
    details = _stack_match_details(vacancy, hit, required_kind="VACANCY", candidate_kind="BENCH")
    if not details["passes"]:
        return None
    semantic_score = _semantic_similarity(hit)
    secondary_score = _secondary_overlap_score(details["required_profile"], details["candidate_profile"])
    grade_score = _grade_match_score(_coerce_match_entity(vacancy).get("grade"), hit.get("grade"))
    rate_score = _budget_alignment_score(_rate_anchor(vacancy), _rate_anchor(hit))
    location_score = _location_match_score(_coerce_match_entity(vacancy).get("location"), hit.get("location"))
    final_score = _final_business_score(
        secondary_score=secondary_score,
        grade_score=grade_score,
        rate_score=rate_score,
        location_score=location_score,
        semantic_score=semantic_score,
    )
    ranked_hit = dict(hit)
    ranked_hit["semantic_sim"] = semantic_score
    ranked_hit["sim"] = final_score
    ranked_hit["score_components"] = {
        "semantic_score": semantic_score,
        "secondary_score": secondary_score,
        "grade_score": grade_score,
        "rate_score": rate_score,
        "location_score": location_score,
        "final_score": final_score,
        "stack_overlap": details["overlap"],
    }
    ranked_hit["_match_sort_key"] = (
        final_score,
        _ascending_rate_tiebreak(hit),
        grade_score,
        rate_score,
        location_score,
        secondary_score,
        semantic_score,
        1 if bool(hit.get("is_internal")) else 0,
    )
    return ranked_hit


def rank_specialist_hits(vacancy: Any, hits: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], bool]:
    eligible = [ranked for ranked in (rank_specialist_hit(vacancy, hit) for hit in hits) if ranked]
    out = sorted(eligible, key=lambda hit: tuple(hit.get("_match_sort_key") or (0, 0, 0, 0, 0, 0, 0)), reverse=True)
    for hit in out:
        hit.pop("_match_sort_key", None)
    return out, any(bool(hit.get("is_internal")) for hit in out)


def rank_vacancy_hit(bench: Any, hit: dict[str, Any]) -> Optional[dict[str, Any]]:
    details = _stack_match_details(bench, hit, required_kind="BENCH", candidate_kind="VACANCY")
    if not details["passes"]:
        return None
    semantic_score = _semantic_similarity(hit)
    secondary_score = _secondary_overlap_score(details["required_profile"], details["candidate_profile"])
    grade_score = _grade_match_score(_coerce_match_entity(bench).get("grade"), hit.get("grade"))
    rate_score = _budget_alignment_score(_rate_anchor(hit), _rate_anchor(bench))
    location_score = _location_match_score(_coerce_match_entity(bench).get("location"), hit.get("location"))
    final_score = _final_business_score(
        secondary_score=secondary_score,
        grade_score=grade_score,
        rate_score=rate_score,
        location_score=location_score,
        semantic_score=semantic_score,
    )
    ranked_hit = dict(hit)
    ranked_hit["semantic_sim"] = semantic_score
    ranked_hit["sim"] = final_score
    ranked_hit["score_components"] = {
        "semantic_score": semantic_score,
        "secondary_score": secondary_score,
        "grade_score": grade_score,
        "rate_score": rate_score,
        "location_score": location_score,
        "final_score": final_score,
        "stack_overlap": details["overlap"],
    }
    ranked_hit["_match_sort_key"] = (final_score, grade_score, rate_score, location_score, secondary_score, semantic_score)
    return ranked_hit


def rank_vacancy_hits(bench: Any, hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    eligible = [ranked for ranked in (rank_vacancy_hit(bench, hit) for hit in hits) if ranked]
    out = sorted(eligible, key=lambda hit: tuple(hit.get("_match_sort_key") or (0, 0, 0, 0, 0, 0)), reverse=True)
    for hit in out:
        hit.pop("_match_sort_key", None)
    return out


def extract_own_bench_hits(hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [hit for hit in hits if bool(hit.get("is_own_bench_source"))]


def upsert_matches(engine, vacancy_id: str, specialist_hits: list[dict[str, Any]]) -> None:
    sql = text(
        """
        INSERT INTO matches(vacancy_id, specialist_id, similarity_score, rank)
        VALUES (:vid, :sid, :score, :rank)
        ON CONFLICT(vacancy_id, specialist_id) DO UPDATE
          SET similarity_score=EXCLUDED.similarity_score,
              rank=EXCLUDED.rank,
              updated_at=NOW()
        """
    )
    with engine.begin() as connection:
        for rank, hit in enumerate(specialist_hits, start=1):
            connection.execute(sql, {"vid": vacancy_id, "sid": hit["id"], "score": float(hit.get("sim") or 0.0), "rank": rank})


def upsert_matches_reverse(engine, specialist_id: str, vacancy_hits: list[dict[str, Any]]) -> None:
    sql = text(
        """
        INSERT INTO matches(vacancy_id, specialist_id, similarity_score, rank)
        VALUES (:vid, :sid, :score, :rank)
        ON CONFLICT(vacancy_id, specialist_id) DO UPDATE
          SET similarity_score=EXCLUDED.similarity_score,
              rank=EXCLUDED.rank,
              updated_at=NOW()
        """
    )
    with engine.begin() as connection:
        for rank, hit in enumerate(vacancy_hits, start=1):
            connection.execute(sql, {"vid": hit["id"], "sid": specialist_id, "score": float(hit.get("sim") or 0.0), "rank": rank})


def list_recent_matches(engine, limit: int = 20) -> list[dict[str, Any]]:
    query = text(
        """
        SELECT
          m.vacancy_id,
          m.specialist_id,
          m.similarity_score,
          m.rank,
          m.created_at,
          v.role AS vacancy_role,
          s.role AS specialist_role
        FROM matches m
        LEFT JOIN vacancies v ON v.id = m.vacancy_id
        LEFT JOIN specialists s ON s.id = m.specialist_id
        ORDER BY m.created_at DESC, m.rank ASC
        LIMIT :limit
        """
    )
    with engine.begin() as connection:
        rows = connection.execute(query, {"limit": limit}).mappings().all()
    return [dict(row) for row in rows]
