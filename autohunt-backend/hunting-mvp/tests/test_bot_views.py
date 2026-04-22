from app.bots import views


def test_render_hit_uses_vertical_parameter_layout_for_internal_specialist():
    html = views.render_hit(
        {
            "sim": 0.67,
            "role": "Middle",
            "grade": "Middle",
            "stack": ["React"],
            "rate_min": None,
            "rate_max": None,
            "currency": None,
            "location": "РФ",
            "is_internal": True,
            "is_own_bench_source": True,
            "source_display": "Менеджер: -; Ссылка на файл: https://disk.360.yandex.ru/i/Az1YZ4V0D1jx2g; Специалист: Иван П.",
            "url": "https://disk.360.yandex.ru/i/Az1YZ4V0D1jx2g",
        },
        1,
    )

    assert "✅ <b>Наш специалист</b>" in html
    assert "<b>· Грейд:</b> Middle" in html
    assert "<b>· Стек:</b> React" in html
    assert "<b>· Ставка:</b> —" in html
    assert "<b>· Локация:</b> РФ" in html


def test_render_entity_summary_block_uses_vertical_parameter_layout():
    html = views.render_entity_summary_block("Кандидат", "Системный аналитик | Middle | BPMN, SQL")

    assert "<b>Кандидат</b>" in html
    assert "<b>· Роль:</b> Системный аналитик" in html
    assert "<b>· Грейд:</b> Middle" in html
    assert "<b>· Стек:</b> BPMN, SQL" in html


def test_render_digest_section_uses_vertical_parameter_layout():
    html = views.render_digest(
        window_start=__import__("datetime").datetime(2026, 3, 19, 10, 0, tzinfo=views.MSK),
        window_end=__import__("datetime").datetime(2026, 3, 20, 10, 0, tzinfo=views.MSK),
        new_vacancies=[
            {
                "role": "Frontend Developer",
                "grade": "Middle",
                "stack": ["React", "TypeScript"],
                "created_at": __import__("datetime").datetime(2026, 3, 19, 12, 0, tzinfo=views.MSK),
                "source_display": "Менеджер: @gssgee; Ссылка на файл: https://disk.yandex.ru/file; Индекс: 2",
                "url": "https://disk.yandex.ru/file",
            }
        ],
        updated_vacancies=[],
        new_specialists=[],
        updated_specialists=[],
    )

    assert "1. Frontend Developer" in html
    assert "<b>· Грейд:</b> Middle" in html
    assert "<b>· Стек:</b> React/TypeScript" in html
    assert "<b>· Обновлено:</b>" in html


def test_render_top_page_can_hide_hits_block_for_load_mode():
    html = views.render_top_page(
        title="",
        entity_label="Кандидат",
        summary="React Developer | Middle | React.js",
        hits=[],
        source_display="Менеджер: @gssgee; Ссылка на файл: https://disk.yandex.ru/file; Индекс: 24",
        source_url="https://disk.yandex.ru/file",
        page=1,
        total_pages=1,
        total_hits=0,
        entity_fields={"name": "Кирилл З.", "role": "React.js", "grade": "Middle", "stack": ["React.js"]},
        show_hits_block=False,
    )

    assert "TOP" not in html
    assert "Нет мэтчей." not in html
    assert "<b>· Имя:</b> Кирилл З." in html
