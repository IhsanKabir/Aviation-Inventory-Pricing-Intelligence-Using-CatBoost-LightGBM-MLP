"""Market Intelligence / Forecasting / GDS paused to save BigQuery costs (2026-10).

While settings.market_data_enabled is False (the default), their API paths answer 503
"paused" without touching BigQuery, /health checks only the database, and the
discount-report / auth / release paths the desktop app uses are untouched.
"""
import dataclasses
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).parent.parent))

from apps.api.app import main  # noqa: E402
from apps.api.app.db import get_optional_db  # noqa: E402


@pytest.fixture()
def client(monkeypatch):
    def no_bigquery(*_a, **_k):
        raise AssertionError("BigQuery must not be queried while market data is paused")

    for name in ("get_health", "get_latest_cycle", "get_cycle_health", "list_routes", "get_taxes"):
        monkeypatch.setattr(main.reporting, name, no_bigquery)
    main.app.dependency_overrides[get_optional_db] = lambda: None
    yield TestClient(main.app)
    main.app.dependency_overrides.clear()


@pytest.mark.parametrize("path", [
    "/api/v1/reporting/cycles/latest", "/api/v1/reporting/taxes", "/api/v1/reporting/forecasting/latest",
    "/api/v1/reporting/export.xlsx", "/api/v1/meta/routes", "/gds/fares",
])
def test_market_paths_answer_paused_without_querying(client, path):
    r = client.get(path)
    assert r.status_code == 503
    assert r.json()["paused"] is True and "running costs" in r.json()["detail"]
    assert r.headers["Retry-After"] == "86400"


def test_health_checks_only_the_database_while_paused(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"database_ok": False, "market_data": "paused"}


def test_discount_reports_api_is_not_paused(client):
    r = client.get("/api/v1/discount-reports/latest")
    assert r.status_code != 503 or not r.json().get("paused")


def test_switching_market_data_back_on_restores_the_endpoints(client, monkeypatch):
    monkeypatch.setattr(main, "settings", dataclasses.replace(main.settings, market_data_enabled=True))
    monkeypatch.setattr(main.reporting, "get_health", lambda db: {"database_ok": True, "latest_cycle_id": "c1"})
    assert client.get("/health").json() == {"database_ok": True, "latest_cycle_id": "c1"}


def test_paused_by_default_and_switched_on_by_env(monkeypatch):
    from apps.api.app import config
    monkeypatch.delenv("MARKET_DATA_ENABLED", raising=False)
    assert config.load_settings().market_data_enabled is False
    monkeypatch.setenv("MARKET_DATA_ENABLED", "true")
    assert config.load_settings().market_data_enabled is True
