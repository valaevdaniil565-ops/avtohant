from datetime import datetime

from app.bots import manager_bot


def test_next_digest_run_same_day_before_deadline():
    now = datetime(2026, 3, 18, 15, 30, tzinfo=manager_bot.MSK)

    result = manager_bot._next_digest_run_at(now)

    assert result == datetime(2026, 3, 18, 16, 0, tzinfo=manager_bot.MSK)


def test_next_digest_run_moves_to_next_day_after_deadline():
    now = datetime(2026, 3, 18, 16, 5, tzinfo=manager_bot.MSK)

    result = manager_bot._next_digest_run_at(now)

    assert result == datetime(2026, 3, 19, 16, 0, tzinfo=manager_bot.MSK)


def test_build_daily_digest_text_contains_sections_and_links(monkeypatch):
    now = datetime(2026, 3, 18, 16, 0, tzinfo=manager_bot.MSK)

    def fake_fetch(engine, table, ts_column, window_start, window_end):
        if table == "vacancies" and ts_column == "created_at":
            return [
                {
                    "role": "Senior Python Developer",
                    "grade": "Senior",
                    "stack": ["Python", "FastAPI"],
                    "created_at": now,
                    "updated_at": now,
                    "url": "https://t.me/python_jobs/123",
                }
            ]
        return []

    monkeypatch.setattr(manager_bot, "_fetch_digest_rows", fake_fetch)

    text_out = manager_bot.build_daily_digest_text(engine=object(), now=now)

    assert "Дайджест за последние 24 часа" in text_out
    assert "Новые вакансии" in text_out
    assert "Senior Python Developer" in text_out
    assert "https://t.me/python_jobs/123" in text_out


def test_build_daily_digest_text_empty_state(monkeypatch):
    monkeypatch.setattr(manager_bot, "_fetch_digest_rows", lambda *args, **kwargs: [])

    text_out = manager_bot.build_daily_digest_text(engine=object(), now=datetime(2026, 3, 18, 16, 0, tzinfo=manager_bot.MSK))

    assert "Новых или обновлённых вакансий и bench нет." in text_out
