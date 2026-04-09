from app.bots import views


def test_render_import_summary_includes_processed_and_skipped_sheets():
    html_out = views.render_import_summary(
        {
            "source_url": "https://docs.google.com/spreadsheets/d/x",
            "source_type": "google_sheet",
            "items_count": 12,
            "sheets_total": 3,
            "sheets_processed": 2,
            "sheets_skipped": 1,
            "confidence": {"high": 8, "medium": 4, "low": 2},
            "processed_sheets": [
                {
                    "sheet_name": "Bench",
                    "sheet_entity_hint": "BENCH",
                    "tables_processed": 1,
                    "rows_imported": 7,
                    "skip_reasons": {"section_row": 2},
                },
                {
                    "sheet_name": "Vacancies",
                    "sheet_entity_hint": "VACANCY",
                    "tables_processed": 1,
                    "rows_imported": 5,
                    "skip_reasons": {},
                },
            ],
            "skipped_sheets": [
                {
                    "sheet_name": "Readme",
                    "skip_reason": "service_sheet_name",
                }
            ],
        }
    )

    assert "Импорт источника" in html_out
    assert "Bench" in html_out
    assert "Vacancies" in html_out
    assert "Readme" in html_out
    assert "служебный лист" in html_out


def test_render_source_includes_sheet_table_and_index():
    text_out = views.render_source(
        "Менеджер: @gssgee; Ссылка на файл: https://disk.yandex.ru/file; Лист: Analysts; Таблица: 1; Индекс: 4",
        "https://disk.yandex.ru/file",
    )

    assert "лист Analysts" in text_out
    assert "таблица 1" in text_out
    assert "индекс 4" in text_out
