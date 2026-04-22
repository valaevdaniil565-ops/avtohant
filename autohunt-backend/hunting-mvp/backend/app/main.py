from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.services.app_settings import ensure_app_settings_table
from app.services.audit_log import ensure_audit_log_table
from app.use_cases.jobs import ensure_jobs_table
from backend.app.api.router import api_router
from backend.app.core.config import get_backend_settings
from backend.app.db.session import bootstrap_db, get_engine


@asynccontextmanager
async def lifespan(_: FastAPI):
    bootstrap_db()
    engine = get_engine()
    ensure_jobs_table(engine)
    ensure_app_settings_table(engine)
    ensure_audit_log_table(engine)
    yield


def create_app() -> FastAPI:
    settings = get_backend_settings()

    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        debug=settings.debug,
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:5173",
            "http://127.0.0.1:5173",
            "http://localhost:4173",
            "http://127.0.0.1:4173",
            "http://localhost:4174",
            "http://127.0.0.1:4174",
            "http://localhost:4175",
            "http://127.0.0.1:4175",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(api_router, prefix=settings.api_prefix)
    return app


app = create_app()
