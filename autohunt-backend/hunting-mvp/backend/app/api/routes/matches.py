from fastapi import APIRouter

from app.use_cases.matching import list_recent_matches
from backend.app.db.session import get_engine

router = APIRouter()


@router.get("")
def list_matches() -> dict[str, object]:
    return {
        "status": "ok",
        "items": list_recent_matches(get_engine(), limit=20),
    }
