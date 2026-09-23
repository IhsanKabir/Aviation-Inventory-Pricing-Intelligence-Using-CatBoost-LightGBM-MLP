"""Offline tests for the live GoZayaan discount pull's pure core + HAR bridge.

Nothing here touches the network or mints a token: modules.gozayaan is never
imported. The live collector composes these pieces with real requests, so a
break here would silently corrupt every live pull.
"""
from __future__ import annotations

import json
from pathlib import Path

from discount_engine.grid import detect_channel
from modules import gozayaan_har
from modules.gozayaan_discounts_core import (
    build_live_har, carrier_prices_from_legs, discount_body, error_summary,
    flight_type_for, write_live_har)


def _fare(carrier: str, total: float) -> dict:
    return {"hash_str": f"{carrier}|DAC-CXB-2026-10-15", "total_fare_amount": total}


def _coupon(code: str, pct: float, cap: float) -> dict:
    return {"discount_promo_code": code, "discount_name": code,
            "discount_markup": {"markup_type": "PERCENTAGE", "markup_amount": pct,
                                "markup_max_amount": cap, "apply_on": "BOOKING"}}


def test_flight_type_dom_only_when_both_ends_domestic():
    assert flight_type_for("DAC", "CXB") == "DOM"
    assert flight_type_for("dac", "zyl") == "DOM"
    assert flight_type_for("DAC", "DXB") == "OUTBOUND"
    assert flight_type_for("JED", "DAC") == "OUTBOUND"


def test_carrier_prices_keep_cheapest_per_carrier():
    legs = {"result": {"fares": [_fare("BS", 5200), _fare("BS", 4800),
                                 _fare("BG", 6100), _fare("2A", 0),   # unpriced: skipped
                                 {"hash_str": "", "total_fare_amount": 900}, "junk"]}}
    assert carrier_prices_from_legs(legs) == {"BS": 4800, "BG": 6100}


def test_carrier_codes_are_sent_back_exactly_as_gozayaan_spells_them():
    # 3L must NOT be aliased before the request: plating_carrier has to be
    # GoZayaan's own code, and aliasing once sent Air Arabia AD's price as 2A's.
    legs = {"result": {"fares": [_fare("3L", 30000), _fare("2A", 4100)]}}
    assert carrier_prices_from_legs(legs) == {"3L": 30000, "2A": 4100}


def test_carrier_prices_tolerate_malformed_responses():
    for bad in (None, [], {}, {"result": None}, {"result": {"fares": "x"}}):
        assert carrier_prices_from_legs(bad) == {}


def test_discount_body_matches_the_web_app_request():
    assert discount_body("sid-1", "BS", "DOM", 4800.4) == {
        "type": "FLIGHT", "region": "BD", "currency": "BDT",
        "platform_type": "GZ_WEB", "search_id": "sid-1",
        "plating_carrier": "BS", "flight_type": "DOM", "product_price": 4800,
    }


def test_error_summary_shows_gozayaans_own_words():
    body = {"error": {"code": "429", "message": "Rate limit exceeded"}}
    assert error_summary(body) == "code=429 message=Rate limit exceeded"
    assert error_summary({"error": "blocked"}) == "error=blocked"
    assert error_summary({"result": {}}) == "keys=['result']"
    assert error_summary("<html>").startswith("non-JSON body")


def test_har_bridge_round_trips_through_the_unchanged_parser():
    calls = [
        {"request_body": discount_body("sid", "BS", "DOM", 5000),
         "response": {"result": [_coupon("DOM10", 10, 300), _coupon("CARD5", 5, 0)]}},
        {"request_body": discount_body("sid", "3L", "OUTBOUND", 30000),
         "response": {"result": [_coupon("INTL7", 7, 1000)]}},
    ]
    har = build_live_har(calls)
    via_har = gozayaan_har.parse_discounts("", har=har)
    direct = []
    for c in calls:
        b = c["request_body"]
        direct += gozayaan_har.rows_from_discount_list(
            plating_carrier=b["plating_carrier"], flight_type=b["flight_type"],
            product_price=b["product_price"], data=c["response"])
    assert via_har == direct
    by_code = {r["coupon_code"]: r for r in via_har}
    assert by_code["DOM10"]["realized_discount_bdt"] == 300      # 10% of 5000 capped at 300
    assert by_code["CARD5"]["realized_discount_bdt"] == 250      # uncapped
    assert by_code["INTL7"]["airline"] == "G9"                    # display alias at parse time


def test_har_url_is_the_real_endpoint(tmp_path: Path):
    url = build_live_har([{"request_body": {}, "response": {}}])["log"]["entries"][0][
        "request"]["url"]
    assert url == "https://production.gozayaan.com/api/business_rules/get_discount_list/"


def test_written_har_is_auto_detected_as_gozayaan(tmp_path: Path):
    out = tmp_path / "gozayaan_live.har"
    n = write_live_har([{"request_body": discount_body("s", "BS", "DOM", 5000),
                         "response": {"result": [_coupon("X", 5, 0)]}}], str(out))
    assert n == 1 and json.loads(out.read_text(encoding="utf-8"))["log"]["entries"]
    assert detect_channel(out) == "gozayaan"
    # and by content alone, if someone renames the file
    renamed = out.rename(tmp_path / "capture.har")
    assert detect_channel(renamed) == "gozayaan"
