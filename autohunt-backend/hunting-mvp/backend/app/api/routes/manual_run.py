import traceback
from pathlib import Path

from fastapi import APIRouter

from app.use_cases.entities import preview_manual_matches
from backend.app.api.schemas import ManualRunRequest, ManualRunResponse
from backend.app.db.session import get_engine

router = APIRouter()


@router.post("", response_model=ManualRunResponse)
def preview_manual_run(payload: ManualRunRequest) -> ManualRunResponse:
    try:
        result = preview_manual_matches(
            get_engine(),
            mode=payload.mode,
            text_value=payload.text,
            limit=payload.limit,
            rate=payload.rate,
        )
        return ManualRunResponse(**result)
    except Exception:
        log_path = Path(".run") / "manual_run_error.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(traceback.format_exc(), encoding="utf-8")
        raise
