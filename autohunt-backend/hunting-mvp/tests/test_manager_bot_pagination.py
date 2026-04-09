from app.bots.manager_bot import _render_manual_top_page, format_hits_page


def _hit(i: int) -> dict:
    return {
        "id": f"id-{i}",
        "sim": 0.9 - (i * 0.001),
        "role": f"Role {i}",
        "grade": "Senior",
        "stack": ["Python", "PostgreSQL"],
        "rate_min": 1000 + i,
        "rate_max": 1200 + i,
        "currency": "RUB",
        "location": "Remote",
        "url": f"https://t.me/c/1/{i}",
    }


def test_format_hits_page_starts_from_custom_rank():
    text_out = format_hits_page([_hit(1), _hit(2)], start_rank=11)
    assert "<b>11.</b>" in text_out
    assert "<b>12.</b>" in text_out


def test_render_manual_top_page_builds_correct_page_slices():
    hits = [_hit(i) for i in range(1, 26)]
    state = {
        "token": "abc123def456",
        "title": "TOP",
        "header": "Header",
        "summary": "Summary",
        "intro_text": "НАШ БЕНЧ:\n01) 90% | Role 1",
        "source_url": "https://t.me/c/1/999",
        "hits": hits,
        "page_size": 10,
    }

    page1_text, page1_kb = _render_manual_top_page(state, 1)
    assert "Страница 1/3" in page1_text
    assert "НАШ БЕНЧ:" in page1_text
    assert "<b>01.</b>" in page1_text
    assert "<b>10.</b>" in page1_text
    assert page1_kb is not None

    page3_text, page3_kb = _render_manual_top_page(state, 3)
    assert "Страница 3/3" in page3_text
    assert "<b>21.</b>" in page3_text
    assert "<b>25.</b>" in page3_text
    assert page3_kb is not None
