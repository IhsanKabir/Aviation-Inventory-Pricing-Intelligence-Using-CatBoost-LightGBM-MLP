"""Router-level tests for /api/v1/discount-reports (no real DB — dependencies faked).

Locks in the gate order and the ingest guarantees: 503 without storage, 401 without a
session, 403 without an approved 'discount-comparison' request, 413 over the size cap,
422 on a bad report_date, server-side re-sanitize on ingest, and server-side coloring
(red diff from the stored previous report) on reads.
"""
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).parent.parent))

from apps.api.app import main  # noqa: E402
from apps.api.app.db import get_optional_db  # noqa: E402
from apps.api.app.routers import discount_reports as dr_router  # noqa: E402

USER = {"user_id": "u-1", "email": "agent@example.com"}


def _payload(report_date="02/07/2026", bs="12"):
    return {"report": {
        "report_date": report_date, "report_time": "1200",
        "generated_at": "2026-07-02T12:00:00", "normalized": True,
        "true_base": {"source": "ft_b2b_har", "airlines_covered": ["BS"], "sample_count": 3},
        "channel_status": {"USBA OTA B2B": "ok"},
        "sources": {"USBA OTA B2B": "FT-B2B HAR: secret-file.har  [true-base]"},
        "routes": ["DAC-CGP"],
        "grids": {"DOM": {"columns": ["BS"], "rows": [
            {"label": "USBA OTA B2B", "kind": "b2b", "cells": {"BS": bs}},
        ]}},
    }}


@pytest.fixture()
def client(monkeypatch):
    """TestClient with a fake DB + auth/access/storage seams; each test tweaks them."""
    fake_db = object()
    main.app.dependency_overrides[get_optional_db] = lambda: fake_db
    monkeypatch.setattr(dr_router.user_accounts, "get_session_user",
                        lambda db, token, touch=True: USER if token == "good" else None)
    monkeypatch.setattr(dr_router.access_requests, "find_approved_request_for_email",
                        lambda db, *, page_key, email: {"request_id": "r-1"}
                        if email == USER["email"] else None)
    stored: dict[str, dict] = {}

    def fake_upsert(db, *, report_date, report_data, submitted_by_user_id,
                    submitted_by_email, team_id="default"):
        stored[report_date.isoformat()] = report_data
        return {"report_id": "rep-1", "report_date": report_date.isoformat(),
                "replaced_existing": False}

    monkeypatch.setattr(dr_router.discount_reports, "upsert_report", fake_upsert)
    yield TestClient(main.app), stored
    main.app.dependency_overrides.pop(get_optional_db, None)


def test_no_db_is_503():
    main.app.dependency_overrides[get_optional_db] = lambda: None
    try:
        r = TestClient(main.app).post("/api/v1/discount-reports", json=_payload())
        assert r.status_code == 503
    finally:
        main.app.dependency_overrides.pop(get_optional_db, None)


def test_no_session_is_401(client):
    c, _ = client
    assert c.post("/api/v1/discount-reports", json=_payload()).status_code == 401
    assert c.post("/api/v1/discount-reports", json=_payload(),
                  headers={"X-User-Session": "bad"}).status_code == 401


def test_unapproved_user_is_403(client, monkeypatch):
    c, _ = client
    monkeypatch.setattr(dr_router.access_requests, "find_approved_request_for_email",
                        lambda db, *, page_key, email: None)
    monkeypatch.setattr(dr_router.access_requests, "find_latest_request_for_email",
                        lambda db, *, page_key, email, statuses=("approved",),
                        enforce_window=False: None)
    r = c.post("/api/v1/discount-reports", json=_payload(),
               headers={"X-User-Session": "good"})
    assert r.status_code == 403
    assert "discount-comparison" in r.json()["detail"]


def test_tier_403_messages(client, monkeypatch):
    """payment_required and expired-window approvals get specific 403 details."""
    c, _ = client
    monkeypatch.setattr(dr_router.access_requests, "find_approved_request_for_email",
                        lambda db, *, page_key, email: None)

    def payment_lookup(db, *, page_key, email, statuses=("approved",),
                       enforce_window=False):
        if "payment_required" in statuses:
            return {"request_id": "r-p",
                    "decision_note": "Weekly plan expired - pay to renew."}
        return None

    monkeypatch.setattr(dr_router.access_requests, "find_latest_request_for_email",
                        payment_lookup)
    r = c.get("/api/v1/discount-reports/latest", headers={"X-User-Session": "good"})
    assert r.status_code == 403 and "pay to renew" in r.json()["detail"]

    def expired_lookup(db, *, page_key, email, statuses=("approved",),
                       enforce_window=False):
        if "approved" in statuses and not enforce_window:
            return {"request_id": "r-e", "requested_end_date": "2026-06-30"}
        return None

    monkeypatch.setattr(dr_router.access_requests, "find_latest_request_for_email",
                        expired_lookup)
    r = c.get("/api/v1/discount-reports/latest", headers={"X-User-Session": "good"})
    assert r.status_code == 403 and "expired" in r.json()["detail"]
    assert "2026-06-30" in r.json()["detail"]


def test_oversized_payload_is_413(client):
    c, _ = client
    big = _payload()
    big["report"]["padding"] = "x" * (600 * 1024)
    r = c.post("/api/v1/discount-reports", json=big, headers={"X-User-Session": "good"})
    assert r.status_code == 413


def test_bad_report_date_is_422(client):
    c, _ = client
    r = c.post("/api/v1/discount-reports", json=_payload(report_date="2026-07-02"),
               headers={"X-User-Session": "good"})
    assert r.status_code == 422
    r = c.post("/api/v1/discount-reports", json=_payload(report_date="02/07/1999"),
               headers={"X-User-Session": "good"})
    assert r.status_code == 422


def test_ingest_resanitizes_server_side(client):
    c, stored = client
    r = c.post("/api/v1/discount-reports", json=_payload(),
               headers={"X-User-Session": "good"})
    assert r.status_code == 200, r.text
    saved = stored["2026-07-02"]
    assert "routes" not in saved                          # route intel stripped
    assert "secret-file.har" not in str(saved)            # filename stripped
    assert saved["sources"]["USBA OTA B2B"]["kinds"] == ["har"]
    assert r.json()["report_date"] == "2026-07-02"


def test_read_colors_against_stored_previous(client, monkeypatch):
    c, _ = client
    current = {"report_date": "2026-07-02",
               "report": _payload(bs="12")["report"], "report_id": "rep-2",
               "generated_at": None, "submitted_by_email": "a@b.c", "updated_at_utc": None}
    previous = {"report_date": "2026-07-01",
                "report": _payload(report_date="01/07/2026", bs="11")["report"]}
    monkeypatch.setattr(dr_router.discount_reports, "get_report",
                        lambda db, report_date=None, team_id="default": dict(current))
    monkeypatch.setattr(dr_router.discount_reports, "get_previous_report",
                        lambda db, *, before_date, team_id="default": previous)
    r = c.get("/api/v1/discount-reports/latest", headers={"X-User-Session": "good"})
    assert r.status_code == 200, r.text
    body = r.json()
    row = body["report"]["grids"]["DOM"]["rows"][0]
    assert row["highlights"]["BS"] == "changed"           # 11 -> 12: server-computed red
    assert body["prev_report_date"] == "2026-07-01"
    assert body["report"]["grids"]["DOM"]["best"]["BS"]["display"] == "12% · USBA"


if __name__ == "__main__":
    import subprocess
    raise SystemExit(subprocess.call([sys.executable, "-m", "pytest", __file__, "-q"]))
