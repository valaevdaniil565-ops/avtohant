from fastapi import APIRouter

from app.use_cases.entities import list_sources_for_entity
from app.use_cases.source_trace import list_recent_sources
from backend.app.api.schemas import EntitySourceResponse, SourceTraceItem
from backend.app.api.serializers import normalize_row
from backend.app.db.session import get_engine

router = APIRouter()


@router.get("")
def list_sources() -> dict[str, object]:
    return {
        "status": "ok",
        "items": list_recent_sources(get_engine(), limit=20),
    }


@router.get("/{entity_type}/{entity_id}", response_model=EntitySourceResponse)
def list_entity_sources(entity_type: str, entity_id: str) -> EntitySourceResponse:
    normalized = entity_type.strip().lower()
    if normalized not in {"vacancy", "specialist"}:
        from fastapi import HTTPException

        raise HTTPException(status_code=400, detail="invalid_entity_type")
    items = [
        SourceTraceItem(**{**normalize_row(row), "source_meta": row.get("source_meta") or {}})
        for row in list_sources_for_entity(get_engine(), normalized, entity_id)
    ]
    return EntitySourceResponse(entity_type=normalized, entity_id=entity_id, items=items)
