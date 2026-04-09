from datetime import datetime, timezone

from app.bots.manager_bot import (
    ARCHIVE_REFERENCE_ONLY_MARKER,
    _build_reference_archive_text,
    _build_source_meta,
    _compose_source_display,
    _should_create_manual_reference_post,
)


def test_compose_source_display_for_file_source():
    text_out = _compose_source_display(
        manager_name="@hr_manager",
        canonical_url="https://disk.yandex.ru/file",
        external_url="https://disk.yandex.ru/file",
        external_locator="row:17",
        source_kind="file",
    )

    assert text_out == "Менеджер: @hr_manager; Ссылка на файл: https://disk.yandex.ru/file; Индекс: 17"


def test_compose_source_display_for_manager_text_only():
    text_out = _compose_source_display(
        manager_name="@hr_manager",
        canonical_url=None,
        external_url=None,
        external_locator=None,
        source_kind="manager_text",
    )

    assert text_out == "Менеджер: @hr_manager"


def test_compose_source_display_for_file_source_with_sheet_trace():
    text_out = _compose_source_display(
        manager_name="@hr_manager",
        canonical_url="https://disk.yandex.ru/file",
        external_url="https://disk.yandex.ru/file",
        external_locator="row:17",
        source_kind="file",
        sheet_name="Analysts",
        table_index=2,
    )

    assert text_out == (
        "Менеджер: @hr_manager; "
        "Ссылка на файл: https://disk.yandex.ru/file; "
        "Лист: Analysts; Таблица: 2; Индекс: 17"
    )


def test_build_source_meta_for_archive_post():
    meta = _build_source_meta(
        base_meta={"foo": "bar"},
        manager_name="@hr_manager",
        canonical_url="https://t.me/c/123/456",
        external_url=None,
        external_locator=None,
        source_kind="archive_post",
        entity_index=2,
        source_sender_name="@candidate",
    )

    assert meta["foo"] == "bar"
    assert meta["source_index"] == "2"
    assert meta["source_sender_name"] == "@candidate"
    assert meta["source_display"] == "Менеджер: @hr_manager; Ссылка на архив-пост: https://t.me/c/123/456; Индекс: 2"


def test_build_reference_archive_text_contains_header_and_indices():
    text_out = _build_reference_archive_text(
        original_text="Middle Python developer, available now",
        original_date=datetime(2026, 3, 19, 12, 0, tzinfo=timezone.utc),
        source_name="@candidate",
        manager_name="@hr_manager",
        items=["Python Developer | Middle | Python, FastAPI", "QA Engineer | Middle | QA, API"],
    )

    assert "<b>Reference-only источник</b>" in text_out
    assert "Дата: 2026-03-19 12:00:00 UTC" in text_out
    assert "Источник: @candidate" in text_out
    assert "Менеджер: @hr_manager" in text_out
    assert "reference-only" in text_out
    assert "1. Python Developer · Middle · Python, FastAPI" in text_out
    assert "2. QA Engineer · Middle · QA, API" in text_out


def test_manual_plain_text_requires_reference_archive_post():
    assert _should_create_manual_reference_post(
        manual_input=True,
        archive_ingest_mode=False,
        forward_kind=None,
        original_text="Senior Python developer, 200k, remote",
        external_urls=[],
    )


def test_manual_message_with_links_or_forward_does_not_require_reference_archive_post():
    assert not _should_create_manual_reference_post(
        manual_input=True,
        archive_ingest_mode=False,
        forward_kind="chat",
        original_text="Forwarded candidate card",
        external_urls=[],
    )
    assert not _should_create_manual_reference_post(
        manual_input=True,
        archive_ingest_mode=False,
        forward_kind=None,
        original_text="Смотри файл https://disk.yandex.ru/file",
        external_urls=["https://disk.yandex.ru/file"],
    )
    assert not _should_create_manual_reference_post(
        manual_input=True,
        archive_ingest_mode=False,
        forward_kind=None,
        original_text="Источник: https://t.me/test/123",
        external_urls=[],
    )
