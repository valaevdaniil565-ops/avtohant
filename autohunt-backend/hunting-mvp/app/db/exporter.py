# app/db/exporter.py
from __future__ import annotations

from io import BytesIO
from typing import Tuple, List, Any
import json
import uuid
import decimal
import datetime

from openpyxl import Workbook
from sqlalchemy import text
from sqlalchemy.engine import Engine


def _fetch(engine: Engine, sql: str, params: dict | None = None) -> tuple[list[str], list[tuple]]:
    with engine.begin() as c:
        res = c.execute(text(sql), params or {})
        cols = list(res.keys())
        rows = res.fetchall()
    return cols, rows


def _excelify(v: Any) -> Any:
    """
    Приводим значения из Postgres/SQLAlchemy к типам, которые принимает openpyxl:
    None, bool, int, float, str, datetime/date/time (naive), bytes.
    """
    if v is None:
        return None

    # UUID -> str
    if isinstance(v, uuid.UUID):
        return str(v)

    # Decimal -> float (или str, если боишься потери точности)
    if isinstance(v, decimal.Decimal):
        try:
            return float(v)
        except Exception:
            return str(v)

    # datetime/date/time
    if isinstance(v, datetime.datetime):
        # openpyxl не любит timezone-aware; делаем naive
        if v.tzinfo is not None:
            try:
                v = v.astimezone(datetime.timezone.utc).replace(tzinfo=None)
            except Exception:
                v = v.replace(tzinfo=None)
        return v

    if isinstance(v, (datetime.date, datetime.time)):
        return v

    # jsonb обычно приходит как dict/list
    if isinstance(v, (dict, list, tuple, set)):
        try:
            return json.dumps(v, ensure_ascii=False)
        except Exception:
            return str(v)

    # bytes/memoryview
    if isinstance(v, memoryview):
        return v.tobytes()
    if isinstance(v, (bytes, bytearray)):
        # можно оставить bytes; но удобнее строкой
        try:
            return v.decode("utf-8", errors="replace")
        except Exception:
            return v.hex()

    # pgvector иногда приходит как list[float] или как объект со str()
    # (если это не dict/list уже обработали выше)
    # Приводим "подозрительные" объекты к строке, если openpyxl не примет
    if isinstance(v, (bool, int, float, str)):
        return v

    # fallback: любые другие типы -> str
    return str(v)


def export_xlsx_bytes(engine: Engine, only_active: bool) -> bytes:
    """
    4 листа: specialists, vacancies, sources, matches
    only_active=True => фильтруем vacancies/specialists по status='active',
    sources/matches — только относящиеся к активным.
    """
    wb = Workbook()
    wb.remove(wb.active)

    def add_sheet(name: str, cols: list[str], rows: list[tuple]):
        ws = wb.create_sheet(title=name)
        ws.append(cols)
        for r in rows:
            ws.append([_excelify(x) for x in r])

    if only_active:
        cols, rows = _fetch(engine, "SELECT * FROM specialists WHERE status='active' ORDER BY created_at DESC")
        add_sheet("specialists", cols, rows)

        cols, rows = _fetch(engine, "SELECT * FROM vacancies WHERE status='active' ORDER BY created_at DESC")
        add_sheet("vacancies", cols, rows)

        cols, rows = _fetch(
            engine,
            """
            SELECT s.*
            FROM sources s
            LEFT JOIN vacancies v ON s.entity_type='vacancy' AND s.entity_id=v.id
            LEFT JOIN specialists sp ON s.entity_type='specialist' AND s.entity_id=sp.id
            WHERE (v.id IS NOT NULL AND v.status='active')
               OR (sp.id IS NOT NULL AND sp.status='active')
            ORDER BY s.created_at DESC
            """,
        )
        add_sheet("sources", cols, rows)

        cols, rows = _fetch(
            engine,
            """
            SELECT m.*
            FROM matches m
            JOIN vacancies v ON m.vacancy_id=v.id
            JOIN specialists s ON m.specialist_id=s.id
            WHERE v.status='active' AND s.status='active'
            ORDER BY m.created_at DESC
            """,
        )
        add_sheet("matches", cols, rows)
    else:
        cols, rows = _fetch(engine, "SELECT * FROM specialists ORDER BY created_at DESC")
        add_sheet("specialists", cols, rows)

        cols, rows = _fetch(engine, "SELECT * FROM vacancies ORDER BY created_at DESC")
        add_sheet("vacancies", cols, rows)

        cols, rows = _fetch(engine, "SELECT * FROM sources ORDER BY created_at DESC")
        add_sheet("sources", cols, rows)

        cols, rows = _fetch(engine, "SELECT * FROM matches ORDER BY created_at DESC")
        add_sheet("matches", cols, rows)

    bio = BytesIO()
    wb.save(bio)
    return bio.getvalue()
