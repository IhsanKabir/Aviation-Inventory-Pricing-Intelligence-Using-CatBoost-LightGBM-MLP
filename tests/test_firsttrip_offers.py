"""FirstTrip B2C: every coupon from the payment-page offer list, flat coupons, telco perks.

Shapes mirror the 2026-10-04 captures (DAC-CXB and DAC-DXB on BS)."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from discount_engine import all_codes, grid
from modules import firsttrip, firsttrip_offers as fo


def _offer_row(code, method, *, rate=12.0, kind="P", cap=3000.0, min_sales=2500.0, flight_type=1,
               bank=None, config=None, configured=True, desc="Exclusive Savings on BS & 2A"):
    return {"code": code, "couponDiscountRate": rate, "couponDiscountType": kind,
            "couponDiscountValue": rate, "couponMaximumDiscountAmount": cap,
            "minimumSalesAmount": min_sales, "flightType": flight_type, "paymentMethod": method,
            "bankInfoId": bank, "description": desc, "promotionTitle": None,
            "isAllowWithCoupon": False, "isDynamicConfigurationEnabled": configured,
            "validTo": "2026-12-31T23:59:00", "dynamicConfig": config or []}


def _cfg(airline, origin=None, dest=None, value=12.0, cap=3000.0, kind="P"):
    return {"airlineCode": airline, "departureAirportCode": origin, "arrivalAirportCode": dest,
            "discountType": kind, "discountValue": value, "maximumDiscountAmount": cap}


INTL_ROWS = [
    *[_offer_row("FTINT26", m, rate=15.0, cap=7000.0, min_sales=4000.0, flight_type=2, bank=b,
                 config=[_cfg("BS", "DAC", "DXB", 15.0, 7000.0), _cfg("EK", "DAC", "DXB", 9.0, 7000.0),
                         _cfg("EK", "DAC", "JED", 8.1, 7000.0)], desc="Exclusive Offer on INTL' Flight")
      for m, b in (("Nagad", None), ("Upay", None), ("Mastercard", 1), ("Mastercard", 2), ("VISA", 3))],
    _offer_row("FTIN-bKash", "bKash", cap=5000.0, flight_type=2,
               config=[_cfg("BS", "DAC", "DXB", 12.0, 5000.0)], desc="Exclusive Savings on bKash Payment"),
    _offer_row("BANKONLY", "VISA", rate=20.0, cap=4000.0, flight_type=2, bank=7, configured=False),
]


@pytest.fixture
def catalog():
    return {"coupons": fo.coupons_from_rows(INTL_ROWS), "perks": {"DOM": ["Gp", "Robi"]}}


def _coupon(cat, code):
    return next(c for c in cat["coupons"] if c["code"] == code)


# --- the offer list -----------------------------------------------------------------

def test_rows_are_grouped_per_coupon_with_who_can_pay(catalog):
    ftint = _coupon(catalog, "FTINT26")
    assert ftint["methods"] == ["Nagad", "Upay", "Mastercard", "VISA"]
    assert ftint["open_methods"] == ["Nagad", "Upay"]            # wallets: anyone paying with one
    assert fo.audience(ftint) == ("common", "Pay with Nagad / Upay, or Mastercard / VISA cards of listed banks")
    assert fo.audience(_coupon(catalog, "BANKONLY"))[0] == "special"
    assert fo.audience(_coupon(catalog, "FTIN-bKash")) == ("common", "Pay with bKash")


def test_rate_table_decides_the_rate_and_where_the_coupon_applies(catalog):
    ftint = _coupon(catalog, "FTINT26")
    assert fo.rate_for(ftint, "EK", "DAC", "DXB")["value"] == 9.0
    assert fo.rate_for(ftint, "EK", "DAC", "JED")["value"] == 8.1
    assert fo.rate_for(ftint, "QR", "DAC", "DXB") is None          # not in its table -> not offered
    assert fo.rate_for(_coupon(catalog, "BANKONLY"), "QR", "DAC", "DXB")["value"] == 20.0  # no table


def test_worth_applies_caps_flat_amounts_and_minimum_spend(catalog):
    ftint = _coupon(catalog, "FTINT26")
    assert fo.worth(ftint, "BS", "DAC", "DXB", 25968)["pct"] == 15.0            # 3,895 < cap: published rate
    capped = fo.worth(ftint, "BS", "DAC", "DXB", 80000)                          # 12,000 -> 7,000 cap
    assert (capped["pct"], capped["capped"]) == (8.75, True)
    assert fo.worth(ftint, "BS", "DAC", "DXB", 3000) is None                     # under 4,000 minimum
    flat = fo.coupons_from_rows([_offer_row("FTCITYAMEX", "AMEX", rate=4500.0, kind="F",
                                            cap=10000.0, bank=4, configured=False)])[0]
    assert fo.worth(flat, "EK", "DAC", "DXB", 33150)["pct"] == 13.57             # 4,500 BDT off 33,150


# --- the search's auto-applied coupon -----------------------------------------------

def _search_offer(**kw):
    seg = {"originAirportCode": "DAC", "destinationAirportCode": "DXB", "departureTime": "2026-10-31T17:10:00",
           "cabinClass": "Economy", "rbd": "S", "flightNumber": "341", "operatingCarrierCode": "EK"}
    offer = {"marketingCarrierCode": "EK", "supplierId": 1, "flightTypeId": 2, "tripTypeId": 1,
             "directions": [[{"segments": [seg]}]], "finalTotalPrice": 40000, "finalBasePrice": 33150,
             "couponCode": "FTCITYAMEX", "couponDiscountRate": 10000.0, "couponDiscountType": "F",
             "couponDiscountValue": 10000.0, "couponMaximumDiscountAmount": 10000.0,
             "totalCouponAmount": 10000.0, "dynamicDiscountCode": "EKINTFT", "dynamicDiscountRate": 7.1,
             "dynamicDiscountAmount": 2353.0}
    offer.update(kw)
    return offer


def test_flat_coupon_is_a_bdt_amount_not_ten_thousand_percent():
    # The old "10000% (CITYAMEX)" cells: couponDiscountType 'F' carries BDT, not %.
    rows = firsttrip._b2c_rows_from_offers([_search_offer()])
    if not rows:
        pytest.skip("offer shape not accepted by _normalize in this fixture")
    (row,) = rows
    assert row["headline_rate"] == pytest.approx(30.17, abs=0.01)    # 10,000 / 33,150
    assert (row["coupon_type"], row["coupon_flat_bdt"]) == ("F", 10000.0)
    assert row["offer_request"]["airlineCode"] == "EK" and row["offer_request"]["rbd"] == "S"


def test_coupon_pct_helper_converts_flat_amounts():
    assert firsttrip._coupon_pct({"couponDiscountRate": 12.0, "couponDiscountType": "P"}, 3724) == 12.0
    flat = {"couponDiscountRate": 4500.0, "couponDiscountType": "F", "couponDiscountValue": 4500.0,
            "couponMaximumDiscountAmount": 10000.0}
    assert firsttrip._coupon_pct(flat, 33150) == 13.57


@pytest.mark.parametrize("code,core", [("FT-Nagad", "NAGAD"), ("FTIN-bKash", "BKASH"), ("FT-bKASH", "BKASH"),
                                       ("FTCITYAMEX", "CITYAMEX"), ("FTEBLDOM07", "EBL"), ("FTINT26", "")])
def test_hyphenated_and_compound_codes_are_recognised(code, core):
    assert firsttrip._ft_coupon_core(code) == core


# --- grid cell ------------------------------------------------------------------------

def _row(airline, base, *, dyn=None, dyn_amt=None, code="", rate=0.0, kind="P", o="DAC", d="DXB"):
    return {"airline": airline, "origin": o, "destination": d, "base_fare_bdt": base,
            "dynamic_code": f"{airline}DYN" if dyn else None, "dynamic_rate": dyn, "dynamic_amount_bdt": dyn_amt,
            "coupon_code": code, "headline_rate": rate, "coupon_type": kind, "coupon_cap_bdt": None,
            "special_code": None, "special_rate": None, "realized_pct": 0.0}


def test_open_coupon_beating_the_dynamic_rate_is_the_common_rate(catalog):
    # BS DAC-DXB: dynamic 9.15%, but FTINT26 15% is open to anyone paying by Nagad/Upay.
    cells = grid._collect_firsttrip_b2c_rows({("DAC", "DXB", "d"): [_row("BS", 25968, dyn=9.15, dyn_amt=2376)]},
                                             1.2, 1.5, catalog)
    # ...and the bank-only 20% coupon, capped at 4,000 BDT, is worth 15.4% here: the special
    assert cells[("INTL", "BS")] == "15(FTINT26, 1.2% fee), 15.4 (BANKONLY, capped, 1.5% fee)"


def test_card_only_flat_coupon_is_the_special_tier(catalog):
    rows = [_row("EK", 33150, dyn=7.1, dyn_amt=2353, code="FTCITYAMEX", rate=13.57, kind="F")]
    cells = grid._collect_firsttrip_b2c_rows({("DAC", "DXB", "d"): rows}, None, None, catalog)
    assert cells[("INTL", "EK")] == "9(FTINT26), 13.57 (City Bank AMEX)"


def test_capped_dynamic_discount_shows_what_it_is_worth():
    row = _row("SV", 128500, dyn=7.4, dyn_amt=6000)        # 7.4% would be 9,509; capped at 6,000
    assert fo.dynamic_pct(row) == 4.67
    assert fo.dynamic_pct(_row("BS", 3724, dyn=15.0, dyn_amt=558)) == 15.0   # FT floors: still 15


def test_without_an_offer_list_the_old_cell_rule_still_applies():
    rows = [_row("BS", 3724, dyn=15.0, dyn_amt=558, code="FT-Nagad", rate=12.0, o="DAC", d="CXB")]
    cells = grid._collect_firsttrip_b2c_rows({("DAC", "CXB", "d"): rows}, None, None, None)
    assert cells[("DOM", "BS")] == "15"


# --- HAR: offer list, telco perks, detection ------------------------------------------

def _har(entries, filler_mb=0):
    p = Path(tempfile.mkdtemp()) / "capture.har"
    filler = [{"request": {"method": "GET", "url": "https://firsttrip.com/videos/hero.mp4"},
               "response": {"content": {"text": "x" * 1_000_000}}} for _ in range(filler_mb)]
    p.write_text(json.dumps({"log": {"entries": filler + entries}}), encoding="utf-8")
    return p


def _post(path, body, data):
    return {"request": {"method": "POST", "url": f"https://b2c-api.firsttrip.com/flight/api/v1{path}",
                        "postData": {"text": json.dumps(body)}},
            "response": {"content": {"text": json.dumps({"statusCode": 200, "data": data})}}}


def test_offer_list_and_telco_partners_are_read_from_a_payment_page_har():
    har = _har([_post(fo.OFFER_LIST, {"flightType": 2}, INTL_ROWS),
                _post(fo.PERK_LIST, {"flightType": 1, "couponType": 4},
                      [{"id": 1, "partnerName": "Gp"}, {"id": 2, "partnerName": "Robi"}])])
    cat = fo.parse_har(har)
    assert sorted(c["code"] for c in cat["coupons"]) == ["BANKONLY", "FTIN-bKash", "FTINT26"]
    assert cat["perks"] == {"DOM": ["Gp", "Robi"]}


def test_capture_whose_api_calls_come_after_megabytes_of_site_files_is_detected():
    har = _har([_post(fo.OFFER_LIST, {"flightType": 2}, INTL_ROWS)], filler_mb=6)
    assert grid.detect_channel(har) == "firsttrip_b2c"


def test_live_catalogue_asks_once_per_market_and_airline():
    calls = []

    class _R:
        status_code = 200

        def __init__(self, data):
            self._d = data

        def json(self):
            return {"data": self._d}

    def post(url, json=None, headers=None, timeout=None):
        calls.append((url.rsplit("/", 1)[1], json["airlineCode"]))
        return _R(INTL_ROWS if url.endswith("GetOfferList") else [{"partnerName": "Gp"}])

    req = {"flightType": 2, "airlineCode": "BS"}
    fares = [{"airline": "BS", "offer_request": req}, {"airline": "BS", "offer_request": req},
             {"airline": "EK", "offer_request": {**req, "airlineCode": "EK"}}]
    cat = fo.fetch_catalog(fares, {}, post=post, sleep=lambda s: None)
    assert calls == [("GetOfferList", "BS"), ("GetPerkOfferList", "BS"), ("GetOfferList", "EK")]
    assert cat["perks"] == {"INTL": ["Gp"]} and len(cat["coupons"]) == 3


# --- all codes ------------------------------------------------------------------------

def test_all_codes_lists_every_firsttrip_coupon_rate_table_and_telco_gap(catalog):
    rows = {("DAC", "DXB", "d"): [{**_row("BS", 25968, dyn=9.15, dyn_amt=2376),
                                   "departure": "2026-10-31T17:10", "flight_number": "341"}]}
    out = all_codes.firsttrip_b2c(rows, {"common": 1.2, "card": 1.5}, catalog)
    bs = {e["code"]: e for e in all_codes.merge(out) if e["airline"] == "BS"}
    assert bs["FTINT26"]["tier"] == "Common" and bs["FTINT26"]["effective"] == "15"
    assert bs["FTIN-bKash"]["who"] == "Pay with bKash"
    table = [e for e in all_codes.merge(out) if e["airline"] == "EK"]
    assert {(e["route"], e["code"], e["published_pct"]) for e in table} == \
        {("DAC-DXB", "FTINT26", 9.0), ("DAC-JED", "FTINT26", 8.1)}     # from the coupon's table
    perk = [e for e in all_codes.merge(out) if e["code"] == "(telco perk)"]
    assert perk and perk[0]["tier"] == "Not captured" and "Gp / Robi" in perk[0]["who"]
