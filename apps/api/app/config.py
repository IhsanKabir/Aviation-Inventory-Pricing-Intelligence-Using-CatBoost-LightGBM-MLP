from __future__ import annotations

import os
from dataclasses import dataclass


def _split_csv(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return ()
    return tuple(part.strip() for part in raw.split(",") if part.strip())


@dataclass(frozen=True)
class Settings:
    api_title: str
    api_version: str
    cors_origins: tuple[str, ...]
    default_limit: int
    max_limit: int


def load_settings() -> Settings:
    default_limit = int(os.getenv("API_DEFAULT_LIMIT", "250"))
    max_limit = int(os.getenv("API_MAX_LIMIT", "5000"))
    if default_limit < 1:
        default_limit = 250
    if max_limit < default_limit:
        max_limit = default_limit

    return Settings(
        api_title=os.getenv("API_TITLE", "Aero Pulse Intelligence API").strip() or "Aero Pulse Intelligence API",
        api_version=os.getenv("API_VERSION", "0.1.0").strip() or "0.1.0",
        cors_origins=_split_csv(
            os.getenv(
                "API_CORS_ORIGINS",
                "http://localhost:3000,http://127.0.0.1:3000",
            )
        ),
        default_limit=default_limit,
        max_limit=max_limit,
    )


settings = load_settings()
