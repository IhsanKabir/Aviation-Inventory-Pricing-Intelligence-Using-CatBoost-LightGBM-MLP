"""Offline tests: what a GoZayaan capture session still needs to click."""
from __future__ import annotations

import json

from modules.gozayaan_coverage import coverage_for_hars, coverage_from_entries


def _entry(path: str, body: dict, resp: dict) -> dict:
    return {"request": {"method": "POST",
                        "url": f"https://production.gozayaan.com/api{path}",
                        "postData": {"text": json.dumps(body)}},
            "response": {"content": {"text": json.dumps(resp)}}}


def _search(sid: str, origin: str, dest: str) -> dict:
    return _entry("/flight/v4.0/search/",
                  {"trips": [{"origin": origin, "destination": dest}]},
                  {"result": {"search_id": sid}})


def _legs(sid: str, *carriers: str) -> dict:
    fares = [{"hash_str": f"{c}|X", "total_fare_amount": 1000 + i}
             for i, c in enumerate(carriers)]
    return _entry("/flight/v4.0/search/legs/", {"search_id": sid},
                  {"result": {"fares": fares}})


def _coupons(sid: str, carrier: str, market: str) -> dict:
    return _entry("/business_rules/get_discount_list/",
                  {"search_id": sid, "plating_carrier": carrier, "flight_type": market,
                   "product_price": 5000}, {"result": []})


SESSION = [
    _search("d1", "DAC", "CXB"), _legs("d1", "BS", "2A", "BG", "VQ"),
    _coupons("d1", "BS", "DOM"),
    _search("d2", "DAC", "CXB"), _legs("d2", "BS", "2A", "BG", "VQ"),
    _coupons("d2", "BG", "DOM"), _coupons("d2", "BG", "DOM"),          # a wasted repeat
    _search("i1", "DAC", "DXB"), _legs("i1", "BS", "BG", "EK", "RX"),
    _coupons("i1", "EK", "OUTBOUND"),
]


def test_missing_is_offered_minus_captured_per_market():
    cov = coverage_from_entries(SESSION)
    assert sorted(cov["DOM"].offered) == ["2A", "BG", "BS", "VQ"]
    assert cov["DOM"].missing == ["2A", "VQ"]
    assert cov["OUTBOUND"].missing == ["BG", "BS", "RX"]
    assert cov["DOM"].routes == {"DAC-CXB"} and cov["OUTBOUND"].routes == {"DAC-DXB"}


def test_repeat_clicks_are_flagged():
    assert coverage_from_entries(SESSION)["DOM"].repeats == {"BG": 2}


def test_missing_can_be_limited_to_tracked_airlines():
    intl = coverage_from_entries(SESSION)["OUTBOUND"]
    assert intl.missing_among(["BS", "BG", "EK"]) == ["BG", "BS"]      # RX not tracked
    assert intl.missing_among(["bs"]) == ["BS"]                        # case-insensitive


def test_market_comes_from_the_route_not_a_guess():
    cov = coverage_from_entries([_search("z", "CGP", "ZYL"), _legs("z", "BS")])
    assert set(cov) == {"DOM"} and cov["DOM"].missing == ["BS"]


def test_junk_entries_are_ignored():
    junk = [{"request": {"url": "https://production.gozayaan.com/api/date/"}},
            {"request": {"url": "x", "postData": {"text": "not json"}}},
            _entry("/flight/v4.0/search/", {"trips": []}, {"result": None}), {}]
    assert coverage_from_entries(junk) == {}


def test_a_session_split_across_files_is_merged(tmp_path):
    a, b = tmp_path / "gozayaan_a.har", tmp_path / "gozayaan_b.har"
    a.write_text(json.dumps({"log": {"entries": SESSION[:3]}}), encoding="utf-8")
    b.write_text(json.dumps({"log": {"entries": [_coupons("d1", "VQ", "DOM")]}}),
                 encoding="utf-8")
    (tmp_path / "broken.har").write_text("{", encoding="utf-8")
    cov = coverage_for_hars([a, b, tmp_path / "broken.har"])
    assert cov["DOM"].missing == ["2A", "BG"]
