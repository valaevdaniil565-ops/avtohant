from __future__ import annotations

from fastapi import APIRouter, Query

from app.services.audit_log import list_audit_events
from backend.app.api.schemas import AuditEventItem, AuditListResponse
from backend.app.api.serializers import normalize_row
from backend.app.db.session import get_engine

router = APIRouter()


@router.get("", response_model=AuditListResponse)
def get_audit_log(
    event_type: str | None = Query(default=None),
    entity_type: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
) -> AuditListResponse:
    items = [AuditEventItem(**normalize_row(row)) for row in list_audit_events(get_engine(), event_type=event_type, entity_type=entity_type, limit=limit)]
    return AuditListResponse(items=items, total=len(items), limit=limit)
