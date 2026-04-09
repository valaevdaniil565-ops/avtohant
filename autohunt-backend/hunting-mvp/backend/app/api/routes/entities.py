from fastapi import APIRouter
from sqlalchemy import text

from app.services.audit_log import write_audit_event
from app.use_cases.source_trace import find_source_entities_by_message_url, soft_hide_entities
from backend.app.api.schemas import HideBySourceRequest, HideBySourceResponse
from backend.app.db.session import get_engine

router = APIRouter()


@router.post("/hide-by-source", response_model=HideBySourceResponse)
def hide_by_source(payload: HideBySourceRequest) -> HideBySourceResponse:
    engine = get_engine()
    source_ref = payload.source_ref.strip()
    matched = find_source_entities_by_message_url(engine, source_ref)
    if not matched:
        with engine.begin() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT id, entity_type, entity_id
                    FROM sources
                    WHERE external_url = :source_ref
                    ORDER BY created_at DESC
                    LIMIT 100
                    """
                ),
                {"source_ref": source_ref},
            ).mappings().all()
        matched = [dict(row) for row in rows]
    hidden = soft_hide_entities(engine, matched)
    write_audit_event(
        engine,
        event_type="entities.hidden_by_source",
        entity_type="source",
        entity_id=source_ref,
        payload={"matched_sources": len(matched), "hidden_entities": hidden},
    )
    return HideBySourceResponse(source_ref=source_ref, matched_sources=len(matched), hidden_entities=hidden)
