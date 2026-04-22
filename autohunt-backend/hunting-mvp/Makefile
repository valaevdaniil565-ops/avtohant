PYTHON := ./.venv/bin/python
PIP := ./.venv/bin/pip
UVICORN := ./.venv/bin/python -m uvicorn
PYTEST := ./.venv/bin/python -m pytest

.PHONY: venv install db-up db-init ollama api worker scheduler bot collector verify imports test smoke

venv:
	python3 -m venv .venv

install:
	$(PIP) install -r requirements.txt

db-up:
	docker compose up -d db

db-init:
	psql postgresql://hunting:hunting@127.0.0.1:5432/hunting -f scripts/init_db.sql

ollama:
	ollama serve

api:
	PYTHONPATH=. $(UVICORN) backend.app.main:app --reload --host 127.0.0.1 --port 8000

worker:
	PYTHONPATH=. $(PYTHON) -m backend.app.worker

scheduler:
	PYTHONPATH=. $(PYTHON) -m backend.app.scheduler

bot:
	PYTHONPATH=. $(PYTHON) -m app.bots.manager_bot

collector:
	PYTHONPATH=. $(PYTHON) -m app.collectors.tg_collector

imports:
	PYTHONPATH=. $(PYTHON) -c "import backend.app.main, app.bots.manager_bot, app.collectors.tg_collector.collector; print('imports_ok')"

test:
	$(PYTEST) tests/test_manager_bot_digest.py tests/test_partner_companies.py tests/test_manager_bot_matching_rules.py -q

verify: imports test

smoke:
	curl http://127.0.0.1:8000/api/health
