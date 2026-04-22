from app.services.own_specialists import (
    _build_registry_identity_key,
    _build_registry_source_meta,
    _extract_registry_specialist_name,
    _resolve_registry_specialist_name,
)


def test_extract_registry_specialist_name_from_named_column():
    raw_text = (
        "Имя: Иван Иванов\n"
        "Стек: Python, FastAPI\n"
        "Ставка: 200000"
    )

    assert _extract_registry_specialist_name(raw_text) == "Иван Иванов"


def test_extract_registry_specialist_name_returns_dash_without_name_column():
    raw_text = (
        "Роль: Python Developer\n"
        "Стек: Python, FastAPI\n"
        "Ставка: 200000"
    )

    assert _extract_registry_specialist_name(raw_text) == "-"


def test_build_registry_source_meta_uses_specialist_name_instead_of_index():
    meta = _build_registry_source_meta(
        {"registry_key": "abc", "locator": "row:17"},
        source_url="https://disk.360.yandex.ru/i/Az1YZ4V0D1jx2g",
        locator="row:17",
        specialist_name="Иван Иванов",
    )

    assert meta["manager_name"] == "-"
    assert meta["source_index"] == "17"
    assert meta["source_person_name"] == "Иван Иванов"
    assert meta["source_display"] == (
        "Менеджер: -; "
        "Ссылка на файл: https://disk.360.yandex.ru/i/Az1YZ4V0D1jx2g; "
        "Специалист: Иван Иванов"
    )


def test_resolve_registry_specialist_name_uses_payload_name_when_text_has_no_name():
    raw_text = (
        "Роль: Python Developer\n"
        "Стек: Python, FastAPI\n"
        "Ставка: 200000"
    )

    assert _resolve_registry_specialist_name(
        raw_text,
        {"name": "Иван Иванов", "role": "Python Developer"},
    ) == "Иван Иванов"


def test_resolve_registry_specialist_name_ignores_payload_role_like_name():
    raw_text = (
        "Роль: Python Developer\n"
        "Стек: Python, FastAPI\n"
        "Ставка: 200000"
    )

    assert _resolve_registry_specialist_name(
        raw_text,
        {"name": "Python Developer", "role": "Python Developer"},
    ) == "-"


def test_registry_identity_key_is_stable_when_locator_changes():
    raw_text = (
        "Имя: Иван Иванов\n"
        "Роль: Дизайнер\n"
        "Стек: Figma, Sketch\n"
        "Портфолио: https://example.com/portfolio"
    )
    payload = {
        "name": "Иван Иванов",
        "role": "Дизайнер",
        "stack": ["Figma", "Sketch"],
    }

    key_a = _build_registry_identity_key(
        "https://disk.360.yandex.ru/i/Az1YZ4V0D1jx2g",
        raw_text,
        payload,
        "Лист1:18",
    )
    key_b = _build_registry_identity_key(
        "https://disk.360.yandex.ru/i/Az1YZ4V0D1jx2g",
        raw_text,
        payload,
        "Лист1:42",
    )

    assert key_a == key_b


def test_registry_identity_key_changes_for_different_specialists():
    source_url = "https://disk.360.yandex.ru/i/Az1YZ4V0D1jx2g"

    key_a = _build_registry_identity_key(
        source_url,
        "Имя: Иван Иванов\nРоль: Дизайнер\nСтек: Figma",
        {"name": "Иван Иванов", "role": "Дизайнер", "stack": ["Figma"]},
        "Лист1:18",
    )
    key_b = _build_registry_identity_key(
        source_url,
        "Имя: Мария Петрова\nРоль: Дизайнер\nСтек: Figma",
        {"name": "Мария Петрова", "role": "Дизайнер", "stack": ["Figma"]},
        "Лист1:19",
    )

    assert key_a != key_b
