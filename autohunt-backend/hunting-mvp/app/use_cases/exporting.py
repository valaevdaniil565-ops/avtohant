from __future__ import annotations

from app.db.exporter import export_xlsx_bytes


def export_database(engine, *, only_active: bool) -> bytes:
    return export_xlsx_bytes(engine, only_active)
