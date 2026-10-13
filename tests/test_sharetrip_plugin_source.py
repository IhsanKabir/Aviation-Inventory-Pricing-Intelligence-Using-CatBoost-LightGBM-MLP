"""Schedule/Fare tabs find the private ShareTrip plugin the way the Discounts tab does.

modules.sharetrip_live is gitignored, so the released exe never bundles it; the admin's
copy lives in the app's plugins folder. Field report 2026-10-12: ShareTrip could not be
ticked on the Schedule/Fare tabs because they only tried the bundled import.
"""
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from market_engine import sources as S  # noqa: E402

PLUGIN = '''
CALLS = []

def fetch_live(origin, dest, day, cabin="Economy", token=None):
    CALLS.append((origin, dest, day, token))
    return {"rows": [], "reason": "plugin answered", "token": "tok-1"}
'''


@pytest.fixture
def no_bundled_module(monkeypatch):
    """As in the exe: `from modules import sharetrip_live` fails; plugin cache cleared."""
    import modules
    monkeypatch.setitem(sys.modules, "modules.sharetrip_live", None)
    # an earlier test may have imported it, binding it on the package: hide that too
    monkeypatch.delattr(modules, "sharetrip_live", raising=False)
    monkeypatch.setattr(S, "_SHARETRIP_PLUGIN", None)
    empty = tempfile.mkdtemp()
    monkeypatch.setenv("APPDATA", empty)              # no real plugins folder
    monkeypatch.delenv("OTA_PLUGINS_DIR", raising=False)


def test_sharetrip_is_offered_from_the_plugins_folder(no_bundled_module, monkeypatch):
    d = Path(tempfile.mkdtemp())
    (d / "sharetrip_live.py").write_text(PLUGIN, encoding="utf-8")
    monkeypatch.setenv("OTA_PLUGINS_DIR", str(d))
    assert S.live_available()["sharetrip"] is True
    rows, reason = S.LiveFetcher("sharetrip")("DAC", "CXB", "2026-10-20")
    assert reason == "plugin answered"
    mod = S.sharetrip_live_module()
    assert mod.CALLS == [("DAC", "CXB", "2026-10-20", None)]
    S.LiveFetcher("sharetrip")  # a fresh fetcher reuses the same loaded plugin
    assert S.sharetrip_live_module() is mod


def test_without_the_plugin_sharetrip_stays_unavailable(no_bundled_module):
    assert S.live_available()["sharetrip"] is False
    rows, reason = S.LiveFetcher("sharetrip")("DAC", "CXB", "2026-10-20")
    assert rows == [] and "not installed" in reason


def test_a_plugin_without_fetch_live_is_ignored(no_bundled_module, monkeypatch):
    d = Path(tempfile.mkdtemp())
    (d / "sharetrip_live.py").write_text("x = 1\n", encoding="utf-8")
    monkeypatch.setenv("OTA_PLUGINS_DIR", str(d))
    assert S.live_available()["sharetrip"] is False


def test_only_the_admin_sees_live_sharetrip_even_with_the_plugin_installed(no_bundled_module, monkeypatch):
    """Live search is the admin's alone: a non-admin never sees ShareTrip (or any live
    source) on the Schedule/Fare tabs, plugin or not; they work from HAR captures."""
    from desktop.market_api import MarketApiMixin
    d = Path(tempfile.mkdtemp())
    (d / "sharetrip_live.py").write_text(PLUGIN, encoding="utf-8")
    monkeypatch.setenv("OTA_PLUGINS_DIR", str(d))

    class _Host(MarketApiMixin):
        def __init__(self, admin):
            self._config = {"har_dir": ""}
            self._admin = admin

        def _live_allowed(self):
            return self._admin

    assert [s["key"] for s in _Host(admin=False).market_state()["sources"]] == ["har"]
    admin_sources = {s["key"]: s["available"] for s in _Host(admin=True).market_state()["sources"]}
    assert admin_sources["sharetrip"] is True
