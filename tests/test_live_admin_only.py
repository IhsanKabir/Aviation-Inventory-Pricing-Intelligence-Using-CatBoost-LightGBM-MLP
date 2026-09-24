"""Live searches are for the administrator only; everyone else works from HARs."""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from test_desktop_backend import _report, _sign_in  # noqa: E402

from desktop import backend as backend_mod  # noqa: E402


@pytest.fixture()
def api(monkeypatch):
    tmp = Path(tempfile.mkdtemp())
    monkeypatch.setattr(backend_mod, "config_dir", lambda: tmp)
    monkeypatch.setattr(backend_mod, "_HAS_KEYRING", False)
    return backend_mod.DesktopApi()


def _setup_run(api, monkeypatch, *, admin: bool):
    folder = Path(tempfile.mkdtemp())
    api._config.update(har_dir=str(folder), routes="DAC-CGP", travel_date="",
                       live_routes={"sharetrip": "DAC-DXB"}, is_admin=admin)
    calls = {"plugin": 0}

    def write_har(routes, date, out, on_route=None):
        calls["plugin"] += 1
        return 0
    api._live_plugins = {"sharetrip": {"label": "ShareTrip", "write_har": write_har}}
    _sign_in(api, monkeypatch)
    monkeypatch.setattr(backend_mod, "auto_detect_hars", lambda d: {})
    monkeypatch.setattr(api, "_save_local_prev", lambda r: None)

    def fake_build(date, routes, **kw):
        calls["routes"] = routes
        return _report()
    monkeypatch.setattr(backend_mod, "build_report", fake_build)
    return calls


def test_a_non_admin_run_makes_no_live_search_whatever_the_boxes_say(api, monkeypatch):
    calls = _setup_run(api, monkeypatch, admin=False)
    result = api.run()
    assert result["ok"]
    assert calls["plugin"] == 0 and calls["routes"] == []      # no ShareTrip, no FirstTrip live
    assert any("administrator only" in w for w in result["warnings"])
    assert api.get_state()["live_allowed"] is False


def test_the_admin_run_still_searches_live(api, monkeypatch):
    calls = _setup_run(api, monkeypatch, admin=True)
    result = api.run()
    assert result["ok"]
    assert calls["plugin"] == 1 and calls["routes"] == [("DAC", "CGP", None)]
    assert not any("administrator only" in w for w in result["warnings"])


def test_admin_flag_comes_from_the_server_and_ends_at_sign_out(api, monkeypatch):
    api._store_token("tok")

    class R:
        status_code = 200
        def json(self): return {"status": "approved", "allowed": True, "is_admin": True}
    monkeypatch.setattr(backend_mod.requests, "get", lambda *a, **k: R())
    api.check_access()
    assert api.get_state()["live_allowed"] is True
    monkeypatch.setattr(backend_mod.requests, "post", lambda *a, **k: None)
    api.logout()
    assert api.get_state()["live_allowed"] is False


def test_an_older_server_without_the_flag_means_not_admin(api, monkeypatch):
    api._store_token("tok")

    class R:
        status_code = 200
        def json(self): return {"status": "approved", "allowed": True}
    monkeypatch.setattr(backend_mod.requests, "get", lambda *a, **k: R())
    api.check_access()
    assert api.get_state()["live_allowed"] is False


def test_schedule_and_fare_refuse_live_sources_for_non_admins(api):
    api._store_token("tok")
    api._config["is_admin"] = False
    plan, err = api._market_plan("fare", "DAC-DXB", "2026-10-01", "2026-10-02",
                                 ["firsttrip"], "Economy")
    assert plan is None and "administrator only" in err["error"]
    plan, err = api._market_plan("fare", "DAC-DXB", "2026-10-01", "2026-10-02",
                                 ["har"], "Economy")
    assert err is None and plan.use_har and plan.source_keys == []
    live = [s for s in api.market_state()["sources"] if s["key"] != "har"]
    assert live and all(not s["available"] and "Administrator only" in s["note"] for s in live)
    api._config["is_admin"] = True
    plan, err = api._market_plan("fare", "DAC-DXB", "2026-10-01", "2026-10-02",
                                 ["firsttrip"], "Economy")
    assert err is None and plan.source_keys == ["firsttrip"]
