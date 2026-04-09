from fastapi import APIRouter

from backend.app.api.routes import admin, audit, digest, entities, health, imports, jobs, matches, own_bench, settings, sources, specialists, vacancies

api_router = APIRouter()
api_router.include_router(health.router, tags=["system"])
api_router.include_router(vacancies.router, prefix="/vacancies", tags=["vacancies"])
api_router.include_router(specialists.router, prefix="/specialists", tags=["specialists"])
api_router.include_router(matches.router, prefix="/matches", tags=["matches"])
api_router.include_router(imports.router, prefix="/imports", tags=["imports"])
api_router.include_router(jobs.router, prefix="/jobs", tags=["jobs"])
api_router.include_router(sources.router, prefix="/sources", tags=["sources"])
api_router.include_router(entities.router, prefix="/entities", tags=["entities"])
api_router.include_router(own_bench.router, prefix="/own-bench", tags=["own-bench"])
api_router.include_router(settings.router, prefix="/settings", tags=["settings"])
api_router.include_router(audit.router, prefix="/audit", tags=["audit"])
api_router.include_router(digest.router, prefix="/digest", tags=["digest"])
api_router.include_router(admin.router, prefix="/admin", tags=["admin"])
