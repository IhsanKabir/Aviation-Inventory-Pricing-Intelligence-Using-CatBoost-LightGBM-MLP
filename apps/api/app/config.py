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
    gzip_enabled: bool
    gzip_minimum_size: int
    request_timing_log_enabled: bool
    bigquery_project_id: str | None
    bigquery_dataset: str | None
    forecasting_source: str


def load_settings() -> Settings:
    default_limit = int(os.getenv("API_DEFAULT_LIMIT", "250"))
    max_limit = int(os.getenv("API_MAX_LIMIT", "5000"))
    gzip_enabled = os.getenv("API_GZIP_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
    gzip_minimum_size = int(os.getenv("API_GZIP_MINIMUM_SIZE", "1200"))
    request_timing_log_enabled = os.getenv("API_REQUEST_TIMING_LOG_ENABLED", "1").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }
    if default_limit < 1:
        default_limit = 250
    if max_limit < default_limit:
        max_limit = default_limit
    if gzip_minimum_size < 256:
        gzip_minimum_size = 256

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
        gzip_enabled=gzip_enabled,
        gzip_minimum_size=gzip_minimum_size,
        request_timing_log_enabled=request_timing_log_enabled,
        bigquery_project_id=(os.getenv("BIGQUERY_PROJECT_ID", "").strip() or None),
        bigquery_dataset=(os.getenv("BIGQUERY_DATASET", "").strip() or None),
        forecasting_source=os.getenv("API_FORECASTING_SOURCE", "bigquery").strip().lower() or "bigquery",
    )


settings = load_settings()
