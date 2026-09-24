"""Progress bars: the tracker, the discount run's ticks, and silent live failures."""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from test_desktop_backend import _report, _sign_in  # noqa: E402

from desktop import backend as backend_mod  # noqa: E402
from desktop.progress import ProgressTracker  # noqa: E402


@pytest.fixture()
def api(monkeypatch):
    tmp = Path(tempfile.mkdtemp())
    monkeypatch.setattr(backend_mod, "config_dir", lambda: tmp)
    monkeypatch.setattr(backend_mod, "_HAS_KEYRING", False)
    return backend_mod.DesktopApi()


def test_tracker_never_shows_100_before_the_end():
    t = ProgressTracker()
    assert t.snapshot()["state"] == "idle"
    t.start("discount", total=2)
    t.tick("a")
    t.tick("b")
    t.tick("extra step nobody counted")
    s = t.snapshot()
    assert s["state"] == "running" and s["percent"] == 99 and s["label"] == "extra step nobody counted"
    t.finish("complete", "Complete")
    assert t.snapshot()["percent"] == 100
    t.tick("late tick")                                   # ignored once finished
    assert t.snapshot()["label"] == "Complete"


def test_tracker_unknown_total_is_indeterminate_and_task_scoped():
    t = ProgressTracker()
    t.start("schedule")
    assert t.snapshot()["percent"] is None
    t.step("DAC-DXB 2026-10-01", 3, 12)
    assert t.snapshot("schedule")["percent"] == 25
    assert t.snapshot("fare")["state"] == "idle"         # another tab's bar stays hidden


def test_an_emptied_firsttrip_box_stays_empty(api):
    assert api.get_state()["routes"] == "DAC-CGP,DAC-DXB,DAC-SIN"     # brand-new install
    api.set_config(routes="")
    assert api.get_state()["routes"] == ""                            # no hidden searches


def _live_plugin(writes: bool):
    def write_har(routes, date, out, on_route=None):
        for r in routes:
            if on_route:
                on_route(r, 5 if writes else 0, None if writes else "mint_failed")
        if writes:
            Path(out).write_text("{}", encoding="utf-8")
            return 5
        return 0
    return {"sharetrip": {"label": "ShareTrip", "write_har": write_har}}


@pytest.mark.parametrize("writes", [True, False])
def test_run_ticks_every_unit_and_flags_a_silent_live_channel(api, monkeypatch, writes):
    folder = Path(tempfile.mkdtemp())
    (folder / "a.har").write_text("{}", encoding="utf-8")
    api._config.update(har_dir=str(folder), routes="DAC-CGP,DAC-CXB",
                       live_routes={"sharetrip": "DAC-DXB"})
    api._live_plugins = _live_plugin(writes)
    _sign_in(api, monkeypatch)
    monkeypatch.setattr(backend_mod, "auto_detect_hars", lambda d: {"bdfare": [str(folder / "a.har")]})
    seen = []

    def fake_build(date, routes, progress=None, **kw):
        for o, d, _ in routes:
            progress(f"FirstTrip B2C live: {o}-{d}")
        progress("Reading a.har")
        progress("Building the per-route view")
        seen.append(api.get_progress("discount"))
        return _report()

    report = {**_report(), "channel_status": {"USBA OTA B2B": "ok"}}
    monkeypatch.setattr(backend_mod, "build_report",
                        lambda *a, **kw: (fake_build(*a, **kw), report)[1])
    monkeypatch.setattr(api, "_save_local_prev", lambda r: None)
    result = api.run()
    assert result["ok"]
    mid = seen[0]
    # 1 live route + 2 FirstTrip routes + 1 HAR + 1 build = 5 units, all ticked
    assert (mid["done"], mid["total"], mid["percent"]) == (5, 5, 99)
    final = api.get_progress("discount")
    assert final["percent"] == 100
    # green only when everything asked for came back; a silent source turns it amber
    assert final["state"] == ("complete" if writes else "warning")
    silent = [w for w in result["warnings"] if "ShareTrip live returned NO data" in w]
    assert bool(silent) is (not writes)
    assert ("ShareTrip live returned nothing" in final["label"]) is (not writes)


def test_a_run_with_no_data_at_all_is_never_green(api, monkeypatch):
    folder = Path(tempfile.mkdtemp())
    api._config.update(har_dir=str(folder), routes="")
    _sign_in(api, monkeypatch)
    monkeypatch.setattr(backend_mod, "auto_detect_hars", lambda d: {})
    monkeypatch.setattr(backend_mod, "build_report", lambda *a, **k: _report())  # no "ok" channel
    monkeypatch.setattr(api, "_save_local_prev", lambda r: None)
    assert api.run()["ok"]
    p = api.get_progress("discount")
    assert p["state"] == "warning" and "no OTA had data" in p["label"]


def test_a_failed_run_ends_red_not_stuck_running(api, monkeypatch):
    folder = Path(tempfile.mkdtemp())
    api._config.update(har_dir=str(folder), routes="")
    _sign_in(api, monkeypatch)
    monkeypatch.setattr(backend_mod, "auto_detect_hars", lambda d: {})
    monkeypatch.setattr(backend_mod, "build_report", lambda *a, **k: 1 / 0)
    assert not api.run()["ok"]
    p = api.get_progress("discount")
    assert p["state"] == "failed" and p["label"].startswith("Failed")
