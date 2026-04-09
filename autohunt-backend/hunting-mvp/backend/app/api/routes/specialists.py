from fastapi import APIRouter, HTTPException, Query

from app.use_cases.entities import get_specialist, get_specialist_matches, list_sources_for_entity, list_specialists
from backend.app.api.serializers import normalize_row
from backend.app.api.deps import get_ollama_client
from backend.app.api.schemas import MatchHit, MatchListResponse, MatchScoreComponents, SourceTraceItem, SpecialistDetailResponse, SpecialistItem, SpecialistListResponse
from backend.app.db.session import get_engine

router = APIRouter()


@router.get("", response_model=SpecialistListResponse)
def list_specialists_endpoint(limit: int = Query(default=50, ge=1, le=200), offset: int = Query(default=0, ge=0)) -> SpecialistListResponse:
    items = [SpecialistItem(**normalize_row(row)) for row in list_specialists(get_engine(), limit=limit, offset=offset)]
    return SpecialistListResponse(items=items, total=len(items), limit=limit, offset=offset)


@router.get("/{specialist_id}", response_model=SpecialistDetailResponse)
def get_specialist_endpoint(specialist_id: str) -> SpecialistDetailResponse:
    engine = get_engine()
    item = get_specialist(engine, specialist_id)
    if item is None:
        raise HTTPException(status_code=404, detail="specialist_not_found")
    sources = [SourceTraceItem(**{**normalize_row(row), "source_meta": row.get("source_meta") or {}}) for row in list_sources_for_entity(engine, "specialist", specialist_id)]
    return SpecialistDetailResponse(item=SpecialistItem(**normalize_row(item)), sources=sources)


@router.get("/{specialist_id}/matches", response_model=MatchListResponse)
def get_specialist_matches_endpoint(specialist_id: str, limit: int = Query(default=20, ge=1, le=100)) -> MatchListResponse:
    specialist, hits = get_specialist_matches(get_engine(), get_ollama_client(), specialist_id, limit=limit)
    if specialist is None:
        raise HTTPException(status_code=404, detail="specialist_not_found")
    items = [
        MatchHit(
            id=str(hit["id"]),
            role=str(hit.get("role") or "Unknown"),
            stack=list(hit.get("stack") or []),
            grade=hit.get("grade"),
            rate_min=hit.get("rate_min"),
            rate_max=hit.get("rate_max"),
            currency=hit.get("currency"),
            location=hit.get("location"),
            is_internal=hit.get("is_internal"),
            is_own_bench_source=hit.get("is_own_bench_source"),
            source_url=hit.get("url"),
            source_display=hit.get("source_display"),
            score=float(hit.get("sim") or 0.0),
            score_components=MatchScoreComponents(**(hit.get("score_components") or {"semantic_score": 0.0, "secondary_score": 0.0, "grade_score": 0.0, "rate_score": 0.0, "location_score": 0.0, "final_score": float(hit.get("sim") or 0.0), "stack_overlap": []})),
        )
        for hit in hits
    ]
    return MatchListResponse(entity_type="specialist", entity_id=specialist_id, items=items, total=len(items))
