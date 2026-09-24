"""Route catalogue + saved preferred routes (desktop) and preferred ordering (engine)."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from desktop import backend as backend_mod
from desktop.routes_api import MAX_PREFERRED, normalize_routes
from discount_engine import by_route, grid


@pytest.fixture()
def api(monkeypatch):
    tmp = Path(tempfile.mkdtemp())
    monkeypatch.setattr(backend_mod, "config_dir", lambda: tmp)
    monkeypatch.setattr(backend_mod, "_HAS_KEYRING", False)
    return backend_mod.DesktopApi()


def test_routes_are_cleaned_deduped_and_bad_ones_reported():
    valid, rejected = normalize_routes(" dac-dxb, DAC-CXB\nDAC-DXB; DAC-DAC  DXB  dac-jed ")
    assert valid == ["DAC-DXB", "DAC-CXB", "DAC-JED"]
    assert rejected == ["DAC-DAC", "DXB"]
    assert normalize_routes(["cgp-jed", "", None]) == (["CGP-JED"], [])


def test_bundled_catalog_covers_every_region(api):
    cat = api.route_catalog()
    assert {"Domestic", "Saudi (KSA)", "Gulf", "Asia", "Europe & long haul"} <= set(cat)
    routes = [r for rs in cat.values() for r in rs]
    assert len(routes) == len(set(routes)) >= 60           # no route listed twice
    assert all(normalize_routes([r])[0] == [r] for r in routes)


def test_operator_catalog_copy_wins(api):
    (backend_mod.config_dir() / "route_catalog.json").write_text(
        json.dumps({"_comment": "x", "Mine": ["DAC-HKG"]}), encoding="utf-8")
    assert api.route_catalog() == {"Mine": ["DAC-HKG"]}


def test_preferred_routes_are_saved_and_survive_a_restart(api):
    r = api.save_preferred_routes(["dac-cxb", "DAC-DXB", "bad"])
    assert r["ok"] and r["preferred"] == ["DAC-CXB", "DAC-DXB"] and r["rejected"] == ["BAD"]
    assert not r["used_for_live"]
    reopened = backend_mod.DesktopApi()                   # same config dir = app restart
    assert reopened.get_state()["preferred_routes"] == ["DAC-CXB", "DAC-DXB"]
    assert api.save_preferred_routes([])["preferred"] == []   # clearing is allowed


def test_use_for_live_fills_every_live_box_only_when_asked(api):
    api._config["routes"] = "DAC-SIN"
    api._live_plugins = {"sharetrip": {"label": "ShareTrip", "write_har": lambda *a: 0}}
    api.save_preferred_routes(["DAC-CXB"])
    assert api.get_state()["routes"] == "DAC-SIN"          # untouched without the flag
    api.save_preferred_routes(["DAC-CXB", "DAC-DXB"], use_for_live=True)
    state = api.get_state()
    assert state["routes"] == "DAC-CXB,DAC-DXB"
    assert state["live_routes"] == {"sharetrip": "DAC-CXB,DAC-DXB"}


def test_too_many_routes_is_refused(api):
    many = [f"DAC-{chr(65 + i // 26)}{chr(65 + i % 26)}Z" for i in range(MAX_PREFERRED + 1)]
    assert not api.save_preferred_routes(many)["ok"]


def _report():
    blocks = by_route.route_blocks({("DAC", "CXB"): {"Amy": {"BS": "6"}},
                                    ("DAC", "DXB"): {"BDFare": {"EK": "5"}}})
    return {"report_date": "20/09/2026", "report_time": "1800", "grids": {},
            "by_route": by_route.highlighted(blocks)}


def test_preferred_routes_come_first_in_the_users_order():
    out = by_route.with_preferred(_report(), ["dac-dxb", "DAC-JED"])
    assert [b["route"] for b in out["by_route"]] == ["DAC-DXB", "DAC-CXB"]
    assert out["by_route"][0]["preferred"] is True and "preferred" not in out["by_route"][1]
    assert out["preferred_missing"] == ["DAC-JED"]
    assert [b["route"] for b in _report()["by_route"]] == ["DAC-CXB", "DAC-DXB"]  # input untouched


def test_excel_names_preferred_routes_nobody_captured():
    from openpyxl import load_workbook
    report = by_route.with_preferred(_report(), ["DAC-DXB", "DAC-JED"])
    wb = load_workbook(grid.write_single_sheet_xlsx(report, None, Path(tempfile.mkdtemp()) / "p.xlsx"))
    detail = [c.value for c in wb["20 September route grids"]["A"] if c.value]
    assert detail.index("★ Your preferred routes") < detail.index("DAC-DXB · International  ★") \
        < detail.index("Other routes found in this run") < detail.index("DAC-CXB · Domestic")
    assert "Not captured yet: DAC-JED" in detail
    overview = [c.value for c in wb["20 September routes"]["A"] if c.value]
    assert "DAC-JED ★" in overview
