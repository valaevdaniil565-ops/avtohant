from app.bots.manager_bot import (
    _compose_own_bench_empty_text,
    _extract_own_bench_hits,
    _merge_specialist_hits,
    _rank_specialist_hits,
    _rank_vacancy_hits,
    _stack_gate_passes,
    extract_archive_declared_type,
    extract_archive_payload_text,
    format_own_bench_block,
)
from app.services.availability import resolve_specialist_is_available


def test_stack_gate_requires_overlap():
    assert _stack_gate_passes(["Python", "PostgreSQL"], ["Python", "Redis"])
    assert not _stack_gate_passes(["Python"], ["Java"])
    assert not _stack_gate_passes([], ["Python"])
    assert not _stack_gate_passes(["React"], ["JavaScript", "TypeScript"])
    assert _stack_gate_passes(["C#"], [".NET", "ASP.NET"])


def test_rank_specialists_uses_match_score_for_global_order():
    hits = [
        {"id": "ext-high", "stack": ["Python"], "sim": 0.92, "is_internal": False, "is_own_bench_source": False},
        {"id": "own-mid", "stack": ["Python"], "sim": 0.65, "is_internal": True, "is_own_bench_source": True},
        {"id": "own-low", "stack": ["Python"], "sim": 0.49, "is_internal": True, "is_own_bench_source": True},
        {"id": "ext-mismatch", "stack": ["Java"], "sim": 0.99, "is_internal": False, "is_own_bench_source": False},
    ]

    ranked, has_own = _rank_specialist_hits({"stack": ["Python"]}, hits)

    assert has_own is True
    assert [h["id"] for h in ranked] == ["ext-high", "own-mid", "own-low"]
    assert ranked[0]["sim"] >= 0.50
    assert ranked[-1]["sim"] >= 0.50


def test_rank_vacancies_filters_by_stack():
    hits = [
        {"id": "vac-1", "stack": ["Python"], "sim": 0.10},
        {"id": "vac-2", "stack": ["Java"], "sim": 0.99},
    ]

    ranked = _rank_vacancy_hits({"stack": ["Python"]}, hits)

    assert [h["id"] for h in ranked] == ["vac-1"]
    assert ranked[0]["sim"] >= 0.50


def test_rank_specialists_requires_primary_stack_not_tooling():
    vacancy = {"role": "Middle", "stack": ["PVS-Studio"]}
    hits = [
        {"id": "cpp", "role": "C/C++", "stack": ["C++", "Qt", "Boost"], "sim": 0.95, "is_internal": False},
        {"id": "dotnet", "role": "C# / .Net", "stack": [".NET", "C#"], "sim": 0.90, "is_internal": False},
    ]

    ranked, has_own = _rank_specialist_hits(vacancy, hits)

    assert has_own is False
    assert ranked == []


def test_rank_specialists_prefers_business_fit_after_stack_match():
    vacancy = {"stack": ["Python"], "grade": "Senior", "rate_min": 2500, "location": "РФ"}
    hits = [
        {
            "id": "semantic-only",
            "stack": ["Python"],
            "grade": "Junior",
            "rate_min": 3800,
            "location": "EU",
            "sim": 0.98,
            "is_internal": False,
        },
        {
            "id": "business-fit",
            "stack": ["Python"],
            "grade": "Senior",
            "rate_min": 2300,
            "location": "РФ",
            "sim": 0.61,
            "is_internal": False,
        },
    ]

    ranked, _ = _rank_specialist_hits(vacancy, hits)

    assert [h["id"] for h in ranked] == ["business-fit", "semantic-only"]
    assert ranked[0]["sim"] > ranked[1]["sim"]


def test_rank_specialists_equal_score_prefers_lower_rate():
    vacancy = {"stack": ["Python"]}
    hits = [
        {"id": "higher-rate", "stack": ["Python"], "rate_min": 2500, "sim": 0.70, "is_internal": False},
        {"id": "lower-rate", "stack": ["Python"], "rate_min": 1800, "sim": 0.70, "is_internal": False},
        {"id": "no-rate", "stack": ["Python"], "sim": 0.70, "is_internal": False},
    ]

    ranked, _ = _rank_specialist_hits(vacancy, hits)

    assert [h["id"] for h in ranked] == ["lower-rate", "higher-rate", "no-rate"]
    assert ranked[0]["sim"] == ranked[1]["sim"] == ranked[2]["sim"]


def test_rank_vacancies_uses_budget_direction():
    bench = {"stack": ["Python"], "grade": "Senior", "rate_min": 2400}
    hits = [
        {"id": "budget-fit", "stack": ["Python"], "grade": "Senior", "rate_min": 2600, "sim": 0.40},
        {"id": "budget-low", "stack": ["Python"], "grade": "Senior", "rate_min": 1800, "sim": 0.95},
    ]

    ranked = _rank_vacancy_hits(bench, hits)

    assert [h["id"] for h in ranked] == ["budget-fit", "budget-low"]
    assert ranked[0]["sim"] > ranked[1]["sim"]


def test_own_bench_block_uses_source_marker():
    hits = [
        {"id": "own-1", "stack": ["Python"], "sim": 0.81, "is_internal": True, "is_own_bench_source": True, "role": "Python Dev", "grade": "Senior", "location": "Remote", "rate_min": 100, "rate_max": 120, "currency": "RUB", "source_display": "Менеджер: -; Ссылка на файл: https://disk.360.yandex.ru/i/Az1YZ4V0D1jx2g; Специалист: Иван Иванов"},
        {"id": "ext-1", "stack": ["Python"], "sim": 0.79, "is_internal": False, "is_own_bench_source": False, "role": "Python Dev", "grade": "Middle", "location": "Remote", "rate_min": 100, "rate_max": 120, "currency": "RUB"},
    ]

    own_hits = _extract_own_bench_hits(hits)
    block = format_own_bench_block(own_hits)

    assert [h["id"] for h in own_hits] == ["own-1"]
    assert "<b>НАШ БЕНЧ</b>" in block
    assert "Python Dev" in block


def test_own_bench_block_empty_text():
    assert format_own_bench_block([]) == "<b>НАШ БЕНЧ</b>\nНа нашем бенче нет подходящих специалистов."


def test_own_bench_empty_text_reports_sync_failure(monkeypatch):
    settings = {
        "own_bench_sync_last_success_at": "2026-03-23T00:15:00+00:00",
        "own_bench_sync_last_error": "RuntimeError: Слишком большой файл",
    }

    monkeypatch.setattr(
        "app.use_cases.own_bench.get_setting",
        lambda _engine, key: settings.get(key),
    )
    monkeypatch.setattr(
        "app.use_cases.own_bench.get_json_setting",
        lambda _engine, key: {},
    )

    text_out = _compose_own_bench_empty_text(object())

    assert "не синхронизирован" in text_out
    assert "Слишком большой файл" in text_out


def test_merge_specialist_hits_preserves_own_bench_marker():
    merged = _merge_specialist_hits(
        [
            {
                "id": "spec-1",
                "role": "UX/UI",
                "sim": 0.73,
                "is_internal": False,
                "is_own_bench_source": False,
            }
        ],
        [
            {
                "id": "spec-1",
                "role": "UX/UI",
                "sim": 0.0,
                "is_internal": True,
                "is_own_bench_source": True,
                "source_display": "Менеджер: -; Ссылка на файл: https://disk.360.yandex.ru/i/Az1YZ4V0D1jx2g; Специалист: Дарья И.",
            }
        ],
    )

    assert len(merged) == 1
    assert merged[0]["id"] == "spec-1"
    assert merged[0]["sim"] == 0.73
    assert merged[0]["is_internal"] is True
    assert merged[0]["is_own_bench_source"] is True


def test_archive_payload_helpers_support_new_header():
    text_in = (
        "Источник: chat / -100123\n"
        "Дата исходного сообщения: 2026-03-13 10:00:00 UTC\n"
        "Отправитель: @user / 42\n"
        "Тип: vacancy\n\n"
        "-- копия исходного сообщения --\n"
        "Ищем Python developer"
    )

    assert extract_archive_declared_type(text_in) == "VACANCY"
    assert extract_archive_payload_text(text_in) == "Ищем Python developer"


def test_explicit_busy_specialist_is_not_available():
    data = {"is_available": True, "availability_weeks": None}
    raw_text = "Senior Python developer, на проекте до конца месяца"

    assert resolve_specialist_is_available(data, raw_text) is False


def test_specialist_with_future_availability_is_not_available_now():
    data = {"is_available": True, "availability_weeks": 2}
    raw_text = "Senior QA, available in 2 weeks"

    assert resolve_specialist_is_available(data, raw_text) is False


def test_explicit_bench_keeps_available_status():
    data = {"is_available": True, "availability_weeks": 0}
    raw_text = "Middle Java developer, на бенче, available now"

    assert resolve_specialist_is_available(data, raw_text) is True
