"""All codes / discounts: every promo code each OTA offered, with its real name."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from discount_engine import all_codes, grid
from discount_engine.sanitize import sanitize_report_for_sync
from modules import gozayaan_har, sharetrip_har


@pytest.fixture
def memo(monkeypatch):
    """A live parse memo, as inside build_report."""
    monkeypatch.setattr(grid, "_PARSE_MEMO", {})
    return grid


def _codes(entries, **match):
    return [e for e in entries if all(e[k] == v for k, v in match.items())]


# --- GoZayaan: every campaign and fare, its own code, banks named -------------------

def _gz_row(code, scope, who, pct, realized, cap=None, carrier="BS", price=40000,
            when="2026-10-29", banks=""):
    return {"channel": "gozayaan", "airline": carrier, "flight_type": "OUTBOUND",
            "coupon_code": code, "discount_pct": pct, "cap_bdt": cap, "realized_pct": realized,
            "eligibility_scope": scope, "eligibility": who, "bank_cards": banks,
            "name": f"{code} offer", "product_price": price,
            "origin": "DAC", "destination": "DXB", "departure_date": when}


def test_gozayaan_lists_every_campaign_not_only_the_best(memo):
    rows = [_gz_row("RFLYINT0926BSO", "common", "Any online payment", 8, 8),
            _gz_row("RVISA2026BRO4", "specific", "Brac Bank / EBL +3 banks", 4, 4,
                    banks="Brac Bank Visa/Master; EBL Visa"),
            _gz_row("RAMEX0126PBB13O", "specific", "City Bank AMEX", 13, 13, 30000),
            _gz_row("RAMEX0126GBB11O", "specific", "City Bank AMEX", 11, 11, 25000)]
    memo._remember("gozayaan_routed", "gz.har", ([], {"INTL": 2.1}))
    memo._remember("gozayaan_fares", "gz.har", rows)
    out = all_codes.collect(gozayaan_hars=["gz.har"])
    assert [e["code"] for e in out] == ["RFLYINT0926BSO", "RAMEX0126PBB13O",
                                        "RAMEX0126GBB11O", "RVISA2026BRO4"]  # common first, best first
    amex = _codes(out, code="RAMEX0126PBB13O")[0]
    assert (amex["route"], amex["market"], amex["cap_bdt"], amex["fee_pct"], amex["effective"],
            amex["basis"], amex["travel_dates"]) == \
        ("DAC-DXB", "INTL", 30000, 2.1, "13", "Booking total", "2026-10-29")
    # the bank-limited campaign names every bank card it needs
    assert _codes(out, code="RVISA2026BRO4")[0]["who"] == "Brac Bank Visa/Master; EBL Visa"


def test_gozayaan_campaign_limited_to_named_banks_is_special_not_common():
    def campaign(code, banks):
        return {"discount_promo_code": code, "discount_markup": {
                    "markup_type": "PERCENTAGE", "markup_amount": 8, "markup_max_amount": 0},
                "discount_validation": {"bank_type_details": [
                    {"bank_name": b, "card_type": t} for b, t in banks]}}
    rows = gozayaan_har.rows_from_discount_list(
        plating_carrier="BS", flight_type="OUTBOUND", product_price=40000,
        data={"result": [campaign("ANY", []),
                         campaign("BANKS", [("Brac Bank", "Visa"), ("Brac Bank", "Master"),
                                            ("EBL", "Visa"), ("Dhaka Bank", "Visa")])]})
    by = {r["coupon_code"]: r for r in rows}
    assert (by["ANY"]["eligibility_scope"], by["ANY"]["eligibility"]) == ("common", "Any online payment")
    assert (by["BANKS"]["eligibility_scope"], by["BANKS"]["eligibility"]) == \
        ("specific", "Brac Bank / Dhaka Bank +1 banks")
    assert by["BANKS"]["bank_cards"] == "Brac Bank Visa/Master; Dhaka Bank Visa; EBL Visa"


def test_gozayaan_campaign_open_to_any_banks_card_stays_common():
    # Real case: RVISA2026BRO4 lists 16 named banks AND "Others" (any bank's Visa).
    campaign = {"discount_promo_code": "RVISA", "discount_markup": {
                    "markup_type": "PERCENTAGE", "markup_amount": 4, "markup_max_amount": 0},
                "discount_validation": {"bank_type_details": [
                    {"bank_name": "Brac Bank", "card_type": "Visa"},
                    {"bank_name": "Others", "card_type": "Visa"}]}}
    (row,) = gozayaan_har.rows_from_discount_list(
        plating_carrier="BS", flight_type="OUTBOUND", product_price=40000, data={"result": [campaign]})
    assert (row["eligibility_scope"], row["eligibility"]) == ("common", "Any Visa card")
    assert row["bank_cards"] == "Any other bank Visa; Brac Bank Visa"


def test_gozayaan_grid_cell_with_only_bank_offers_shows_zero_common():
    rows = [_gz_row("BANKS", "specific", "Brac Bank / EBL", 8, 8)]
    for r in rows:
        r["flight_type"] = "OUTBOUND"
    cells = grid._gozayaan_cells(rows, {"INTL": 2.1})
    assert cells[("INTL", "BS")] == "0, 8 (Brac Bank / EBL, 2.1% fee)"


def test_gozayaan_keeps_every_fare_so_ranges_and_counts_are_real(memo):
    rows = [_gz_row("CAP", "common", "Any online payment", 10, eff, cap=4000, price=price)
            for price, eff in ((40000, 10.0), (80000, 5.0))]
    memo._remember("gozayaan_routed", "gz.har", ([], {}))
    memo._remember("gozayaan_fares", "gz.har", rows)
    (entry,) = all_codes.collect(gozayaan_hars=["gz.har"])
    assert (entry["effective"], entry["seen"]) == ("5-10", 2)


def test_gozayaan_per_fare_parse_keeps_each_price(tmp_path_factory=None):
    def entry(url, resp, body):
        return {"request": {"method": "POST", "url": url, "postData": {"text": json.dumps(body)}},
                "response": {"content": {"text": json.dumps(resp)}}}
    coupon = {"discount_promo_code": "X7", "discount_markup": {
        "markup_type": "PERCENTAGE", "markup_amount": 7, "markup_max_amount": 0}}
    har = {"log": {"entries": [
        entry("https://x/api/flight/v4.0/search/", {"result": {"search_id": "s1"}},
              {"trips": [{"origin": "DAC", "destination": "DXB", "preferred_time": "2026-10-29"}]}),
        *[entry("https://x/api/business_rules/get_discount_list/", {"result": [coupon]},
                {"search_id": "s1", "plating_carrier": "BS", "flight_type": "OUTBOUND",
                 "product_price": p}) for p in (40000, 52000)]]}}
    path = Path(tempfile.mkdtemp()) / "gz.har"
    path.write_text(json.dumps(har), encoding="utf-8")
    assert len(gozayaan_har.parse_discounts_routed(str(path))) == 1          # grid input unchanged
    fares = gozayaan_har.parse_discounts_routed(str(path), per_fare=True)
    assert [(r["product_price"], r["departure_date"]) for r in fares] == \
        [(40000, "2026-10-29"), (52000, "2026-10-29")]


# --- ShareTrip: each airline's OWN coupons, never another airline's ----------------

_TERMS = [
    {"couponCode": "FLIGHTINT", "title": "Preferred Fares", "discount": 0, "isDefault": True,
     "discountType": "Percentage", "withDiscount": "Yes"},
    {"couponCode": "STLRSIQ326", "title": "Up to BDT 6,000 Savings with Stellar Signature",
     "discount": 18, "discountType": "Percentage", "maximumDiscountAmount": 6000, "withDiscount": "No"},
    {"couponCode": "FLYGPSTAR", "title": "Exclusive for GPStar Customers!", "discount": 1,
     "discountType": "Percentage", "maximumDiscountAmount": 5000, "withDiscount": "Yes"},
    {"couponCode": "BKASHDOM", "title": "bKash payment", "discount": 2, "discountType": "Percentage",
     "maximumDiscountAmount": 0, "withDiscount": "Yes"},
    {"couponCode": "ZEROEMI", "title": "Enjoy 0% EMI", "discount": 0, "discountType": "Percentage",
     "withDiscount": "No"},
]


def _st_details(airline, origin, dest, auto, base, ftype, terms=_TERMS, when="2026-10-31"):
    row = {"channel": "sharetrip", "airline": airline, "flight_type": ftype, "coupon_terms": terms,
           "origin": origin, "destination": dest, "departure_date": when}
    row.update(sharetrip_har.judge_cell(auto, base, terms))
    return row


def _st_search(airline, origin, dest, auto, base, ftype="INTL", when="2026-10-31"):
    return {"airline": airline, "flight_type": ftype, "discount_pct": auto, "coupon_code": "FLIGHTINT",
            "base_fare_bdt": base, "origin": origin, "destination": dest, "departure_date": when}


def test_sharetrip_names_the_automatic_code_and_every_coupon(memo):
    details = [_st_details("BS", "DAC", "DXB", 8.5, 40000, "INTL")]
    search = [_st_search("BS", "DAC", "DXB", 8.5, 40000)]       # the SAME fare as the booking
    memo._remember("sharetrip_routed", "st.har", (details, search))
    out = all_codes.collect(sharetrip_hars=["st.har"])
    auto = _codes(out, code="FLIGHTINT")[0]
    assert (auto["tier"], auto["who"], auto["effective"], auto["seen"], auto["basis"]) == \
        ("Common", "Anyone (automatic)", "8.5", 1, "Base fare")  # one fare met twice counts once
    stellar = _codes(out, code="STLRSIQ326")[0]
    assert (stellar["tier"], stellar["who"], stellar["stacks"], stellar["cap_bdt"], stellar["seen"]) == \
        ("Special", "Stellar Signature", "No", 6000, 1)
    assert stellar["title"] == "Up to BDT 6,000 Savings with Stellar Signature"
    emi = _codes(out, code="ZEROEMI")[0]                       # listed, as a 0% payment option
    assert (emi["tier"], emi["published_pct"], emi["effective"]) == ("No discount", 0.0, "")
    assert len(_codes(out, code="FLIGHTINT")) == 1             # the default coupon isn't "No discount"


def test_sharetrip_airline_without_a_booking_page_gets_no_borrowed_coupons(memo):
    # Real case (2026-09-28): BS's booking page carried Stellar; G9's search fare did
    # not have a booking page captured. G9 must NOT be listed with BS's Stellar.
    details = [_st_details("BS", "DAC", "DXB", 8.5, 40000, "INTL")]
    search = [_st_search("G9", "DAC", "DXB", 2.0, 30000)]
    memo._remember("sharetrip_routed", "st.har", (details, search))
    out = all_codes.collect(sharetrip_hars=["st.har"])
    g9 = _codes(out, airline="G9")
    assert {e["tier"] for e in g9} == {"Common", "Not captured"}
    assert not _codes(g9, code="STLRSIQ326")


def test_zero_rates_are_no_discount_and_listed_once_per_airline(memo):
    # EMI/net-banking 0% codes repeat on every route; a low-cost carrier's automatic
    # rate can be 0%. Neither is a discount, and neither should flood the list.
    details = [_st_details("BS", "DAC", r, 8.5, 40000, "INTL") for r in ("DXB", "JED", "KUL")]
    search = [{**_st_search("G9", "DAC", r, 0.0, 30000), "coupon_code": "BUDGETFLY"}
              for r in ("DXB", "JED")]
    memo._remember("sharetrip_routed", "st.har", (details, search))
    out = all_codes.collect(sharetrip_hars=["st.har"])
    emi = _codes(out, code="ZEROEMI")
    assert len(emi) == 1 and emi[0]["route"] == all_codes.ALL_ROUTES and emi[0]["seen"] == 3
    budget = _codes(out, code="BUDGETFLY")
    assert [(e["tier"], e["route"]) for e in budget] == [("No discount", all_codes.ALL_ROUTES)]
    assert not _codes(out, airline="G9", tier="Common")


def test_sharetrip_airline_uses_its_own_terms_from_another_route(memo):
    # EK booked on DAC-JED; on DAC-DXB only searched at 90k: its OWN Stellar terms apply,
    # judged at that fare (the 6,000 cap binds: 6.67%).
    details = [_st_details("EK", "DAC", "JED", 8.0, 40000, "INTL")]
    search = [_st_search("EK", "DAC", "DXB", 8.0, 90000)]
    memo._remember("sharetrip_routed", "st.har", (details, search))
    out = all_codes.collect(sharetrip_hars=["st.har"])
    assert _codes(out, route="DAC-DXB", code="STLRSIQ326")[0]["effective"] == "6.67"
    assert not _codes(out, route="DAC-DXB", tier="Not captured")


def test_sharetrip_domestic_wallet_coupon_is_common_with_its_code(memo):
    details = [_st_details("BS", "DAC", "CXB", 5.0, 5000, "DOM")]
    memo._remember("sharetrip_routed", "st.har", (details, []))
    out = all_codes.collect(sharetrip_hars=["st.har"])
    wallet = _codes(out, code="BKASHDOM")[0]
    assert (wallet["tier"], wallet["who"], wallet["market"]) == ("Common", "bKash payment", "DOM")
    assert _codes(out, code="FLYGPSTAR")[0]["tier"] == "Special"


def test_other_wallets_count_as_the_common_rate():
    upay = {"couponCode": "UPAYDOM", "title": "Pay with Upay", "discount": 3,
            "discountType": "Percentage", "withDiscount": "Yes"}
    assert sharetrip_har.judge_cell(5, 5000, [upay])["common_code"] == "UPAYDOM"
    # "tap" must be a word, not any text containing it
    startup = {**upay, "couponCode": "STARTAPFLY", "title": "Startapp promo"}
    assert sharetrip_har.judge_cell(5, 5000, [startup])["common_code"] is None


def test_terms_are_collected_per_airline():
    rows = [_st_details("BS", "DAC", "DXB", 8, 40000, "INTL"),
            _st_details("G9", "DAC", "DXB", 0, 30000, "INTL", terms=[])]
    terms = sharetrip_har.terms_by_airline(rows)
    assert {c["couponCode"] for c in terms[("INTL", "BS")]} >= {"STLRSIQ326"}
    assert terms[("INTL", "G9")] == []


def test_a_range_shows_when_one_code_was_worth_different_amounts():
    obs = [all_codes._obs(market="INTL", route="DAC-DXB", ota="ShareTrip-B2C", airline="EK",
                          tier="Special", code="STLRSIQ326", who="Stellar", published=18,
                          effective=eff, basis="Base fare", fare=("2026-10-31", base),
                          date="2026-10-31")
           for eff, base in ((13.75, 43600), (0.87, 690000), (9.0, 66700))]
    (entry,) = all_codes.merge(obs)
    assert (entry["effective"], entry["seen"]) == ("0.87-13.75", 3)


def test_many_travel_dates_are_summarised():
    assert all_codes._fmt_dates({"2026-10-01", "2026-10-02"}) == "2026-10-01, 2026-10-02"
    days = {f"2026-10-{d:02d}" for d in range(1, 8)}
    assert all_codes._fmt_dates(days) == "2026-10-01 to 2026-10-07 (7 dates)"


# --- FirstTrip B2C: classed by who can use the code ---------------------------------

def _ft_row(**kw):
    row = {"airline": "BS", "origin": "DAC", "destination": "CXB", "base_fare_bdt": 4000,
           "departure": "2026-10-10T09:00", "flight_number": "BS141",
           "coupon_code": "", "headline_rate": 0, "coupon_cap_bdt": None,
           "dynamic_code": None, "dynamic_rate": None, "special_code": None, "special_rate": None}
    row.update(kw)
    return row


def test_firsttrip_classes_codes_by_who_can_use_them():
    rows = {("DAC", "CXB", "2026-10-10"): [
        _ft_row(dynamic_code="FTBSDOM", dynamic_rate=14, coupon_code="FTEBLDOM07",
                headline_rate=16, coupon_cap_bdt=500),
        _ft_row(coupon_code="FTBKASHDOM", headline_rate=10),
        _ft_row(coupon_code="FTUCBDOM", headline_rate=12),
        _ft_row(coupon_code="FTFLYDOM", headline_rate=9)]}
    out = all_codes.collect(b2c_rows_by_route=rows, b2c_fees={"common": 1.5, "card": 2.0})
    dyn = _codes(out, code="FTBSDOM")[0]
    assert (dyn["tier"], dyn["fee_pct"], dyn["travel_dates"]) == ("Common", 1.5, "2026-10-10")
    ebl = _codes(out, code="FTEBLDOM07")[0]
    # 16% of 4,000 = 640, capped at 500 -> 12.5% actually saved
    assert (ebl["tier"], ebl["who"], ebl["effective"], ebl["fee_pct"]) == ("Special", "EBL", "12.5", 2.0)
    assert _codes(out, code="FTBKASHDOM")[0]["who"] == "bKash payment"        # wallet: Common
    assert _codes(out, code="FTBKASHDOM")[0]["tier"] == "Common"
    assert _codes(out, code="FTUCBDOM")[0]["tier"] == "Special"               # newly known bank
    assert _codes(out, code="FTFLYDOM")[0]["tier"] == "Unclear"               # never guessed


def test_one_channel_failing_only_drops_that_channel(memo, capsys):
    memo._remember("gozayaan_routed", "gz.har", ([], {}))
    memo._remember("gozayaan_fares", "gz.har", [_gz_row("X1", "common", "Any", 5, 5)])
    out = all_codes.collect(gozayaan_hars=["gz.har"], b2c_rows_by_route={("A", "B", "d"): [{}]})
    assert [e["code"] for e in out] == ["X1"]
    assert "all-codes list: FirstTrip B2C skipped" in capsys.readouterr().out


# --- report, Excel and sync ---------------------------------------------------------

def _report(codes):
    return {"report_date": "04/10/2026", "report_time": "1200", "generated_at": "x",
            "grids": {"DOM": {"columns": [], "rows": []}, "INTL": {"columns": [], "rows": []}},
            "channel_status": {}, "sources": {}, "all_codes": codes}


def test_excel_gets_a_filterable_all_codes_sheet(memo):
    from openpyxl import load_workbook
    memo._remember("gozayaan_routed", "gz.har", ([], {"INTL": 2.1}))
    memo._remember("gozayaan_fares", "gz.har",
                   [_gz_row("RAMEX0126PBB13O", "specific", "City Bank AMEX", 13, 13, 30000)])
    report = _report(all_codes.collect(gozayaan_hars=["gz.har"]))
    path = Path(tempfile.mkdtemp()) / "r.xlsx"
    grid.write_single_sheet_xlsx(report, None, path)
    ws = load_workbook(path)["04 October (all codes)"]
    heads = [c.value for c in ws[3]]
    assert heads[:8] == ["Market", "Route", "Travel date(s)", "OTA", "Airline", "Tier",
                         "Promo code", "Who can use it"]
    row = {h: c.value for h, c in zip(heads, ws[4])}
    assert (row["Promo code"], row["Tier"], row["Route"], row["Cap (BDT)"], row["% of"]) == \
        ("RAMEX0126PBB13O", "Special", "DAC-DXB", 30000, "Booking total")
    assert row["Published %"] == pytest.approx(0.13)
    assert ws.auto_filter.ref.startswith("A3:")


def test_no_codes_means_no_sheet():
    from openpyxl import load_workbook
    path = Path(tempfile.mkdtemp()) / "r.xlsx"
    grid.write_single_sheet_xlsx(_report([]), None, path)
    assert not [n for n in load_workbook(path).sheetnames if "all codes" in n]


def test_codes_never_leave_the_machine():
    entry = all_codes.merge([all_codes._obs(market="INTL", route="DAC-DXB", ota="Go Zayaan",
                                            airline="BS", tier="Common", code="X", who="Any",
                                            published=5, effective=5, basis="Booking total")])
    assert "all_codes" not in json.dumps(sanitize_report_for_sync(_report(entry)))


def _gz_campaign(code, validation, kind="PERCENTAGE", amount=7, cap=100000):
    return {"discount_promo_code": code, "discount_name": f"{code} campaign",
            "discount_markup": {"markup_type": kind, "markup_amount": amount, "markup_max_amount": cap},
            "discount_validation": validation}


def test_gozayaan_wallet_and_open_card_campaigns_say_how_to_pay():
    rows = gozayaan_har.rows_from_discount_list(
        plating_carrier="BS", flight_type="DOM", product_price=5000, data={"result": [
            _gz_campaign("RDOMB", {"type": "MFS", "name": "bKash Only", "mfs_type_details": [{"name": "BKASH"}]}),
            _gz_campaign("RFLYDOM", {"type": "MFS", "name": "NAGAD, Upay, Tap, Rocket", "mfs_type_details": [
                {"name": "NAGAD"}, {"name": "UPAY"}, {"name": "TAP"}, {"name": "ROCKET"}]}),
            _gz_campaign("RGOFLY", {"type": "CARDS", "name": "All Card", "is_for_admin": True}),
            _gz_campaign("RVISA", {"type": "CARDS", "name": "All VISA"})]})
    got = {r["coupon_code"]: (r["eligibility_scope"], r["eligibility"]) for r in rows}
    assert got == {"RDOMB": ("common", "Pay with bKash"),
                   "RFLYDOM": ("common", "Pay with Nagad / Upay / Tap / Rocket"),
                   "RGOFLY": ("common", "Any card"), "RVISA": ("common", "Any VISA card")}


def test_gozayaan_flat_campaign_is_bdt_off_the_booking():
    (row,) = gozayaan_har.rows_from_discount_list(
        plating_carrier="BS", flight_type="OUTBOUND", product_price=40000, data={"result": [
            _gz_campaign("REINT07262K", {"type": "CARDS", "name": "EBL Visa credit cards",
                                         "bank_type_details": [{"bank_name": "EBL", "card_type": "Visa"}]},
                         kind="FLAT", amount=2000)]})
    assert (row["discount_type"], row["flat_bdt"], row["realized_pct"], row["eligibility_scope"]) == \
        ("FLAT", 2000, 5.0, "specific")
