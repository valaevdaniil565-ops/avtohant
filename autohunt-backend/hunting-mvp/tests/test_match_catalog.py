from app.services.match_catalog import detect_primary_catalog_profile
from app.use_cases.matching import _stack_match_details, build_manual_query_entity, rank_specialist_hit


def test_detect_primary_catalog_profile_for_performance_testing():
    profile = detect_primary_catalog_profile("РќР°РіСЂСѓР·РѕС‡РЅРѕРµ С‚РµСЃС‚РёСЂРѕРІР°РЅРёРµ, JMeter, Oracle, PostgreSQL")
    assert profile is not None
    assert profile["id"] == "profile_13"


def test_build_manual_query_entity_uses_legacy_fallback_for_designer():
    _, profile = build_manual_query_entity("РќСѓР¶РµРЅ РґРёР·Р°Р№РЅРµСЂ UX/UI, Figma", mode="vacancy")
    assert profile is not None
    assert profile["id"] == "designer"


def test_catalog_profiles_require_exact_same_direction_in_main_matching():
    details = _stack_match_details(
        {"role": "AQA Python", "stack": ["Python", "Pytest", "Selenium"]},
        {"role": "AQA Java", "stack": ["Java", "JUnit", "Selenium"]},
        required_kind="VACANCY",
        candidate_kind="BENCH",
    )
    assert details["required_catalog"] != details["candidate_catalog"]
    assert details["passes"] is False


def test_catalog_profiles_pass_for_same_exact_profile():
    details = _stack_match_details(
        {"role": "Python backend developer", "stack": ["Python", "FastAPI", "PostgreSQL"]},
        {"role": "Python Developer", "stack": ["Python", "Redis", "FastAPI"]},
        required_kind="VACANCY",
        candidate_kind="BENCH",
    )
    assert details["required_catalog"] == details["candidate_catalog"]
    assert details["passes"] is True


def test_role_title_has_priority_over_stack_only_catalog_guess():
    details = _stack_match_details(
        {"role": "AQA Python", "stack": ["Python", "Kafka", "Airflow", "Spark"]},
        {"role": "Data Engineer", "stack": ["Python", "Kafka", "Airflow", "Spark"]},
        required_kind="VACANCY",
        candidate_kind="BENCH",
    )
    assert details["required_catalog"] == "profile_11"
    assert details["candidate_catalog"] == "profile_17"
    assert details["passes"] is False


def test_same_catalog_without_real_stack_overlap_does_not_pass():
    details = _stack_match_details(
        {"role": "Data engineer", "stack": ["Python", "Kafka", "Airflow", "Spark"]},
        {"role": "Data engineer", "stack": ["Oracle", "Informatica", "PowerCenter"]},
        required_kind="VACANCY",
        candidate_kind="BENCH",
    )
    assert details["passes"] is False


def test_incompatible_work_format_rejects_match():
    ranked = rank_specialist_hit(
        {
            "role": "Python developer",
            "stack": ["Python", "FastAPI"],
            "grade": "Senior",
            "location": "Удаленно",
            "description": "Формат работы: удаленно",
        },
        {
            "id": "spec-1",
            "role": "Python developer",
            "stack": ["Python", "FastAPI"],
            "grade": "Senior",
            "location": "Москва",
            "description": "Только офис",
            "sim": 0.9,
        },
    )
    assert ranked is None
