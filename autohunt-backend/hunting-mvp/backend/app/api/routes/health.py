from fastapi import APIRouter, HTTPException

from backend.app.core.config import get_backend_settings
from backend.app.db.session import ping_db

router = APIRouter()


@router.get("/health")
def health() -> dict[str, object]:
    db_ok = False
    try:
        db_ok = ping_db()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database_unavailable: {exc}") from exc

    return {
        "status": "ok" if db_ok else "degraded",
        "database": "ok" if db_ok else "down",
    }


@router.get("/version")
def version() -> dict[str, str]:
    settings = get_backend_settings()
    return {
        "name": settings.app_name,
        "version": settings.app_version,
        "api_prefix": settings.api_prefix,
    }
