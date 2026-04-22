from fastapi import APIRouter

from app.use_cases.own_bench import get_sync_status
from backend.app.api.schemas import OwnBenchStatusPayload, OwnBenchStatusResponse
from backend.app.db.session import get_engine

router = APIRouter()


@router.get("", response_model=OwnBenchStatusResponse)
@router.get("/status", response_model=OwnBenchStatusResponse)
def own_bench_status() -> OwnBenchStatusResponse:
    return OwnBenchStatusResponse(status="ok", sync=OwnBenchStatusPayload(**get_sync_status(get_engine())))
