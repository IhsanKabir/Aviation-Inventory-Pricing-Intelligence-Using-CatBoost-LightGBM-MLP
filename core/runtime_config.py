from __future__ import annotations

import os
from dotenv import load_dotenv


def get_database_url(
    fallback: str = "postgresql+psycopg2://postgres@localhost:5432/Playwright_API_Calling",
) -> str:
    """
    Resolve DB URL from:
    1) AIRLINE_DB_URL
    2) DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD
    3) fallback
    """
    load_dotenv()
    url = os.getenv("AIRLINE_DB_URL", "").strip()
    if url:
        return url

    host = os.getenv("DB_HOST", "").strip()
    port = os.getenv("DB_PORT", "").strip()
    name = os.getenv("DB_NAME", "").strip()
    user = os.getenv("DB_USER", "").strip()
    pwd = os.getenv("DB_PASSWORD", "").strip()

    if host and port and name and user:
        if pwd:
            return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{name}"
        return f"postgresql+psycopg2://{user}@{host}:{port}/{name}"

    return fallback
