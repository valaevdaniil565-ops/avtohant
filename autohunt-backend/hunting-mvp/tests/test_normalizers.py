from app.integrations.mcp_source_fetcher.normalizers import normalize_table_rows, normalize_table_rows_with_summary


def test_normalize_table_rows_row_wise():
    rows = [
        ["Role", "Stack", "Grade"],
        ["Backend Python", "Python, FastAPI", "Senior"],
        ["", "", ""],
        ["QA", "Playwright", "Middle"],
    ]
    items = normalize_table_rows(rows, "https://docs.google.com/spreadsheets/d/x")
    assert len(items) == 2
    assert "Role: Backend Python" in items[0].text
    assert items[0].row_index == 2


def test_normalize_table_rows_detects_header_below_banner_and_structures_bench_row():
    rows = [
        ["Свободные специалисты MyCo", "", "", ""],
        ["Имя", "Стек", "Грейд", "Ставка"],
        ["Роман Р.", "СА", "Senior", "3000"],
    ]
    items = normalize_table_rows(rows, "https://docs.google.com/spreadsheets/d/x")
    assert len(items) == 1
    assert items[0].row_index == 3
    assert items[0].metadata["entity_hint"] == "BENCH"
    assert items[0].metadata["structured_fields"]["name"] == "Роман Р."
    assert items[0].metadata["structured_fields"]["role"] == "Системный аналитик"
    assert items[0].metadata["structured_fields"]["stack"] == ["Системный аналитик"]
    assert "Роль: Системный аналитик" in items[0].text


def test_normalize_table_rows_skips_section_rows_and_keeps_multistack_values():
    rows = [
        ["Свободные Специалисты itWit", "", "", ""],
        ["Специалицация", "Имя", "Стэк технологий", "Рейт (руб)"],
        ["Fullltime", "", "", ""],
        ["Android Разработчик", "Антон С.", "Java, Kotlin\nStack: Compose, Retrofit", "2000"],
    ]
    items = normalize_table_rows(rows, "https://docs.google.com/spreadsheets/d/x")
    assert len(items) == 1
    stacks = items[0].metadata["structured_fields"]["stack"]
    assert "Java" in stacks
    assert "Kotlin" in stacks
    assert "Compose" in stacks


def test_normalize_table_rows_keeps_stack_phrase_without_explicit_delimiters():
    rows = [
        ["Роль", "Стек", "Грейд"],
        ["QA Engineer", "Авто QA Python", "Middle"],
    ]
    items = normalize_table_rows(rows, "https://docs.google.com/spreadsheets/d/x")

    assert len(items) == 1
    assert items[0].metadata["structured_fields"]["stack"] == ["Авто QA Python"]


def test_normalize_table_rows_keeps_slash_stack_phrase_as_single_value():
    rows = [
        ["Роль", "Стек", "Грейд"],
        ["QA Engineer", "Java AQA / SDET", "Senior"],
    ]
    items = normalize_table_rows(rows, "https://docs.google.com/spreadsheets/d/x")

    assert len(items) == 1
    assert items[0].metadata["structured_fields"]["stack"] == ["Java AQA / SDET"]


def test_normalize_table_rows_with_summary_splits_multiple_tables_on_same_sheet():
    rows = [
        ["Bench import", "", "", ""],
        ["Имя", "Роль", "Стек", "Грейд"],
        ["Роман Р.", "Системный аналитик", "SQL, BPMN", "Senior"],
        ["", "", "", ""],
        ["Роль", "Стек", "Требования", "Компания"],
        ["Python Developer", "Python, FastAPI", "Asyncio", "Acme"],
    ]
    items, summary = normalize_table_rows_with_summary(rows, "https://docs.google.com/spreadsheets/d/x", table_name="Mixed")

    assert len(items) == 2
    assert items[0].metadata["table_index"] == 1
    assert items[1].metadata["table_index"] == 2
    assert items[0].metadata["entity_hint"] == "BENCH"
    assert items[1].metadata["entity_hint"] == "VACANCY"
    assert summary["tables_processed"] == 2
    assert summary["rows_imported"] == 2


def test_normalize_table_rows_with_summary_uses_sheet_context_and_confidence():
    rows = [
        ["Имя", "Грейд", "Ставка"],
        ["Роман Р.", "Senior", "3000"],
    ]
    items, summary = normalize_table_rows_with_summary(
        rows,
        "https://docs.google.com/spreadsheets/d/x",
        table_name="Системные аналитики",
    )

    assert len(items) == 1
    assert items[0].metadata["structured_fields"]["role"] == "Системные аналитики"
    assert items[0].metadata["confidence"] == "high"
    assert summary["confidence"]["high"] == 1


def test_normalize_table_rows_with_summary_skips_service_sheet():
    rows = [["Инструкция"], ["Шаг 1"], ["Шаг 2"]]
    items, summary = normalize_table_rows_with_summary(rows, "https://docs.google.com/spreadsheets/d/x", table_name="Readme")

    assert items == []
    assert summary["is_skipped"] is True
    assert summary["skip_reason"] == "service_sheet_name"
