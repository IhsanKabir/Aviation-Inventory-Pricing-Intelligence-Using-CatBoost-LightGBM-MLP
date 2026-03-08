from __future__ import annotations

import os
from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from core.runtime_config import get_database_url


def _database_configured() -> bool:
    return bool(
        os.getenv("AIRLINE_DB_URL", "").strip()
        or (
            os.getenv("DB_HOST", "").strip()
            and os.getenv("DB_PORT", "").strip()
            and os.getenv("DB_NAME", "").strip()
            and os.getenv("DB_USER", "").strip()
        )
    )


engine = create_engine(get_database_url(), pool_pre_ping=True, future=True) if _database_configured() else None
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True) if engine is not None else None


def get_db() -> Generator[Session, None, None]:
    if SessionLocal is None:
        raise RuntimeError("Database session requested but AIRLINE_DB_URL / DB_* is not configured")
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def get_optional_db() -> Generator[Session | None, None, None]:
    if SessionLocal is None:
        yield None
        return
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
