"""Tests for discount_engine.build_report orchestration.

Locks in the Phase 0 guarantees:
  1. Each FT B2B HAR is parsed ONCE and each FT B2C route fetched ONCE — the rows are
     shared by the true-base oracle AND the channel collectors (no double work).
  2. Oracle health is reported: source (offline ft_b2b_har / live_b2c / both / none /
     disabled) + normalized flag; a degraded oracle never silently normalizes.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from discount_engine import grid  # noqa: E402


def _ft_b2b_row(airline="BS", gross=5549, base=4424, pct=12.0):
    return {"airline": airline, "origin": "DAC", "destination": "CGP",
            "gross_total_bdt": gross, "base_fare_bdt": base,
            "net_total_bdt": gross - round(base * pct / 100), "commission_pct": pct}


def _b2c_row(airline="BS", gross=5549, base=4424, rate=16.0):
    return {"airline": airline, "origin": "DAC", "destination": "CGP",
            "gross_total_bdt": gross, "base_fare_bdt": base,
            "headline_rate": rate, "dynamic_rate": None, "coupon_code": "FT16",
            "realized_pct": rate, "coupon_cap_bdt": None}


def test_ft_b2b_har_parsed_exactly_once(monkeypatch):
    calls: list[str] = []

    def fake_parse(path):
        calls.append(str(path))
        return [_ft_b2b_row()]

    monkeypatch.setattr(grid.firsttrip, "parse_b2b_commissions", fake_parse)
    report = grid.build_report(None, [], firsttrip_b2b_hars=["x.har"], use_true_base=True)

    assert calls == ["x.har"]                      # oracle + collector shared ONE parse
    assert report["normalized"] is True
    assert report["true_base"]["source"] == "ft_b2b_har"
    assert report["true_base"]["airlines_covered"] == ["BS"]
    dom = {r["label"]: r["cells"] for r in report["grids"]["DOM"]["rows"]}
    assert dom["USBA OTA B2B"]["BS"] == "12"       # collector still fed from shared rows


def test_ft_b2c_route_fetched_exactly_once(monkeypatch):
    calls: list[tuple] = []

    def fake_fetch(origin, dest, date):
        calls.append((origin, dest, date))
        return [_b2c_row()]

    monkeypatch.setattr(grid.firsttrip, "fetch_b2c_discounts", fake_fetch)
    monkeypatch.setattr(grid.firsttrip, "summarize_b2c_discounts",
                        lambda rows: {"BS": {"rate": 16.0, "code": "FT16",
                                             "source": "coupon", "realized_pct": 16.0,
                                             "cap_bdt": None}})
    report = grid.build_report("2026-07-30", [("DAC", "CGP", None)], use_true_base=True)

    assert calls == [("DAC", "CGP", "2026-07-30")]   # oracle + collector shared ONE fetch
    assert report["true_base"]["source"] == "live_b2c"
    dom = {r["label"]: r["cells"] for r in report["grids"]["DOM"]["rows"]}
    assert dom["Firsttrip-B2C"]["BS"] == "16"


def test_degraded_oracle_is_flagged_not_silent(monkeypatch):
    def failing_fetch(origin, dest, date):
        raise RuntimeError("blocked / offline")

    monkeypatch.setattr(grid.firsttrip, "fetch_b2c_discounts", failing_fetch)
    report = grid.build_report("2026-07-30", [("DAC", "CGP", None)], use_true_base=True)

    assert report["true_base"]["source"] == "none"
    assert report["normalized"] is False


def test_true_base_disabled_is_reported(monkeypatch):
    monkeypatch.setattr(grid.firsttrip, "parse_b2b_commissions", lambda p: [_ft_b2b_row()])
    report = grid.build_report(None, [], firsttrip_b2b_hars=["x.har"], use_true_base=False)

    assert report["true_base"]["source"] == "disabled"
    assert report["normalized"] is False
    dom = {r["label"]: r["cells"] for r in report["grids"]["DOM"]["rows"]}
    assert dom["USBA OTA B2B"]["BS"] == "12"       # channel itself still populated


if __name__ == "__main__":
    import subprocess
    raise SystemExit(subprocess.call([sys.executable, "-m", "pytest", __file__, "-q"]))
