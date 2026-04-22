from __future__ import annotations

from collections.abc import Generator
from functools import lru_cache

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.db.engine import make_engine
from app.db.repo import Repo
from backend.app.core.config import get_backend_settings


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    settings = get_backend_settings()
    return make_engine(settings.database_url)


@lru_cache(maxsize=1)
def get_session_factory() -> sessionmaker[Session]:
    return sessionmaker(bind=get_engine(), autoflush=False, autocommit=False, future=True)


def get_db_session() -> Generator[Session, None, None]:
    session = get_session_factory()()
    try:
        yield session
    finally:
        session.close()


@lru_cache(maxsize=1)
def get_repo() -> Repo:
    settings = get_backend_settings()
    return Repo(settings.database_url)


def bootstrap_db() -> None:
    with get_engine().begin() as connection:
        connection.execute(text("SELECT 1"))


def ping_db() -> bool:
    with get_engine().begin() as connection:
        return connection.execute(text("SELECT 1")).scalar_one() == 1
