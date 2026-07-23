"""Tests for the desktop bridge (no pywebview/keyring/network needed).

Locks in the sign-in wall (no session -> no run/export/archive; explicit denial
blocks; short offline stretches honor the grace window) plus: login stores the
token + flushes the outbox; sync posts the SANITIZED payload; network failures
queue to the outbox (auth failures do NOT); run() shares the engine and colors
against the backend previous report.
"""
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

import desktop.backend as backend_mod  # noqa: E402
from desktop.outbox import Outbox  # noqa: E402


def _mktmp() -> Path:
    # pytest's tmp_path fixtures hit a PermissionError on this machine's temp
    # policy — plain mkdtemp works everywhere.
    return Path(tempfile.mkdtemp(prefix="dsk_test_"))


@pytest.fixture()
def api(monkeypatch):
    tmp = _mktmp()
    monkeypatch.setattr(backend_mod, "config_dir", lambda: tmp)
    monkeypatch.setattr(backend_mod, "_HAS_KEYRING", False)   # deterministic file store
    return backend_mod.DesktopApi()


class FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}
        self.reason = "reason"
        self.headers = {"content-type": "application/json"}

    def json(self):
        return self._payload


def _report(bs="12"):
    return {"report_date": "02/07/2026", "report_time": "1200",
            "generated_at": "x", "normalized": True,
            "true_base": {"source": "ft_b2b_har", "airlines_covered": ["BS"], "sample_count": 1},
            "channel_status": {}, "sources": {}, "routes": ["DAC-CGP"],
            "grids": {"DOM": {"columns": ["BS"], "rows": [
                {"label": "USBA OTA B2B", "kind": "b2b", "cells": {"BS": bs}}]}}}


def test_outbox_roundtrip():
    box = Outbox(_mktmp() / "ob")
    box.enqueue("2026-07-02", {"a": 1})
    box.enqueue("2026-07-01", {"b": 2})
    assert box.count() == 2
    assert [d for d, _ in box.pending()] == ["2026-07-01", "2026-07-02"]  # oldest first
    box.mark_done("2026-07-01")
    assert box.count() == 1


def test_login_stores_token_and_flushes_outbox(api, monkeypatch):
    api._outbox.enqueue("2026-07-01", {"report_date": "01/07/2026"})
    posts = []

    def fake_post(url, **kwargs):
        posts.append(url)
        if url.endswith("/user-auth/login"):
            return FakeResponse(200, {"session_token": "tok-1", "user": {}})
        return FakeResponse(200, {})

    monkeypatch.setattr(backend_mod.requests, "post", fake_post)
    result = api.login("Agent@Example.com", "pw")
    assert result["ok"] and result["email"] == "agent@example.com"
    assert api._token() == "tok-1"
    assert result["outbox_flushed"] == 1 and api._outbox.count() == 0


def test_sync_posts_sanitized_payload(api, monkeypatch):
    api._store_token("tok-1")
    api._report = _report()
    posts = []   # sync_now also fires an async usage ping — capture per-call

    def fake_post(url, json=None, headers=None, timeout=None):
        posts.append({"url": url, "json": json, "headers": headers})
        return FakeResponse(200, {})

    monkeypatch.setattr(backend_mod.requests, "post", fake_post)
    result = api.sync_now()
    assert result["ok"] and result["synced"] == "2026-07-02"
    sync_post = next(p for p in posts if "/discount-reports" in p["url"])
    assert sync_post["headers"]["X-User-Session"] == "tok-1"
    assert "routes" not in sync_post["json"]["report"]         # sanitized before send
    assert sync_post["json"]["report"]["grids"]["DOM"]["rows"][0]["cells"]["BS"] == "12"


def test_sync_network_failure_queues_but_auth_failure_does_not(api, monkeypatch):
    api._store_token("tok-1")
    api._report = _report()

    def unreachable(url, **kwargs):
        raise backend_mod.requests.ConnectionError("offline")

    monkeypatch.setattr(backend_mod.requests, "post", unreachable)
    result = api.sync_now()
    assert result["queued"] and api._outbox.count() == 1

    api._outbox.mark_done("2026-07-02")
    monkeypatch.setattr(backend_mod.requests, "post",
                        lambda url, **kw: FakeResponse(401, {"detail": "expired"}))
    result = api.sync_now()
    assert not result.get("queued") and result["needs_login"]
    assert api._outbox.count() == 0                            # auth errors never queue


def _sign_in(api, monkeypatch, status="approved", allowed=True):
    api._store_token("tok-1")
    monkeypatch.setattr(api, "check_access",
                        lambda: {"status": status, "allowed": allowed, "detail": ""})


def test_run_requires_sign_in(api):
    tmp_path = _mktmp()
    api._config["har_dir"] = str(tmp_path)
    result = api.run()
    assert not result["ok"] and result.get("auth_required")


def test_offline_grace_window(api, monkeypatch):
    from datetime import datetime, timedelta, timezone
    api._store_token("tok-1")
    monkeypatch.setattr(api, "check_access",
                        lambda: {"status": "unknown", "allowed": False, "detail": ""})
    # Verified recently -> offline run allowed.
    api._config["last_access_ok_utc"] = datetime.now(timezone.utc).isoformat()
    assert api._require_access() is None
    # Verified too long ago -> blocked until online again.
    stale = datetime.now(timezone.utc) - timedelta(hours=api.OFFLINE_GRACE_HOURS + 1)
    api._config["last_access_ok_utc"] = stale.isoformat()
    assert api._require_access().get("auth_required")


def test_pending_user_runs_locally_but_denial_blocks(api, monkeypatch):
    _sign_in(api, monkeypatch, status="pending", allowed=False)
    assert api._require_access() is None                       # identified + tracked
    _sign_in(api, monkeypatch, status="rejected", allowed=False)
    assert api._require_access().get("access_blocked")


def test_export_requires_sign_in(api):
    api._report = _report()
    result = api.export_xlsx()
    assert not result["ok"] and result.get("auth_required")


def test_run_shares_engine_and_colors_against_prev(api, monkeypatch):
    tmp_path = _mktmp()
    (tmp_path / "x.har").write_text("{}", encoding="utf-8")
    api._config["har_dir"] = str(tmp_path)
    api._config["routes"] = "DAC-CGP"
    _sign_in(api, monkeypatch)
    monkeypatch.setattr(backend_mod, "auto_detect_hars",
                        lambda d: {"firsttrip_b2b": [str(tmp_path / "x.har")]})
    monkeypatch.setattr(backend_mod, "build_report",
                        lambda *a, **kw: _report(bs="12"))
    # change diff is now vs THIS machine's previous local run, not the backend
    monkeypatch.setattr(api, "_load_local_prev", lambda: _report(bs="11"))
    monkeypatch.setattr(api, "_save_local_prev", lambda report: None)
    result = api.run()
    assert result["ok"] and result["prev_available"]
    row = result["report"]["grids"]["DOM"]["rows"][0]
    assert row["highlights"]["BS"] == "changed"                # 11 -> 12 vs local prev
    assert result["report"]["grids"]["DOM"]["best"]["BS"]["display"] == "12% net · USBA"


def test_run_skip_paths_filter(api, monkeypatch):
    tmp_path = _mktmp()
    keep, skip = str(tmp_path / "a.har"), str(tmp_path / "b.har")
    for p in (keep, skip):
        Path(p).write_text("{}", encoding="utf-8")
    api._config["har_dir"] = str(tmp_path)
    api._config["routes"] = ""
    _sign_in(api, monkeypatch)
    monkeypatch.setattr(backend_mod, "auto_detect_hars",
                        lambda d: {"bdfare": [keep, skip]})
    seen = {}

    def fake_build(date, routes, **kwargs):
        seen.update(kwargs)
        return _report()

    monkeypatch.setattr(backend_mod, "build_report", fake_build)
    result = api.run(skip_paths=[skip])
    assert result["ok"]
    assert seen["bdfare_hars"] == [keep]                       # skipped file excluded


if __name__ == "__main__":
    import subprocess
    raise SystemExit(subprocess.call([sys.executable, "-m", "pytest", __file__, "-q"]))
