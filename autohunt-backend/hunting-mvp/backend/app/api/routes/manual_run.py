from fastapi import APIRouter

from app.use_cases.entities import preview_manual_matches
from backend.app.api.schemas import ManualRunRequest, ManualRunResponse
from backend.app.db.session import get_engine

router = APIRouter()


@router.post("", response_model=ManualRunResponse)
def preview_manual_run(payload: ManualRunRequest) -> ManualRunResponse:
    result = preview_manual_matches(
        get_engine(),
        mode=payload.mode,
        text_value=payload.text,
        limit=payload.limit,
        rate=payload.rate,
    )
    return ManualRunResponse(**result)
