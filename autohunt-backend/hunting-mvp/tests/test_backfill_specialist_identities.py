from scripts.backfill_specialist_identities import _collect_structured_fields


def test_collect_structured_fields_prefers_row_map_for_sheet_rows():
    source_meta = {
        "row_map": {
            "Стек": "PHP",
            "Грейд": "Middle",
            "Локация": "Москва",
            "Имя": "Даниил П.",
            "Ссылка на резюме": "https://docs.google.com/document/d/example/edit",
            "Ставка": "1800 ₽",
        }
    }

    fields = _collect_structured_fields(source_meta, "")

    assert fields == {
        "stack": "PHP",
        "grade": "Middle",
        "location": "Москва",
        "name": "Даниил П.",
        "resume_url": "https://docs.google.com/document/d/example/edit",
        "rate_min": 1800,
    }


def test_collect_structured_fields_reads_archive_post_bullets():
    raw_text = "Авто QA Python\n· Грейд: Senior\n· Стек: Авто QA Python/Тестировщики\n· Ставка: 2 000 RUB\n· Локация: Москва"

    fields = _collect_structured_fields({"source_index": "1"}, raw_text)

    assert fields == {
        "role": "Авто QA Python",
        "grade": "Senior",
        "stack": "Авто QA Python/Тестировщики",
        "rate_min": 2000,
        "location": "Москва",
    }


def test_collect_structured_fields_uses_source_index_for_multi_bench_post():
    raw_text = (
        "✅ Катя Графический designer Junior\nЛокация РФ\nРейт 1000₽\n\n"
        "✅ Наиля UX/UI designer Middle\nЛокация РФ\nРейт 1000₽"
    )

    fields = _collect_structured_fields({"source_index": "2"}, raw_text)

    assert fields == {
        "name": "Наиля",
        "role": "UX/UI designer",
        "grade": "Middle",
        "rate_min": 1000,
        "location": "РФ",
    }
