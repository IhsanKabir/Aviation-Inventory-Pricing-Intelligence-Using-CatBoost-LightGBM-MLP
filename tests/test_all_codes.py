"""All codes / discounts: every promo code each OTA offered, with its real name."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from discount_engine import all_codes, grid
from discount_engine.sanitize import sanitize_report_for_sync


@pytest.fixture
def memo(monkeypatch):
    """A live parse memo, as inside build_report."""
    monkeypatch.setattr(grid, "_PARSE_MEMO", {})
    return grid


def _codes(entries, **match):
    return [e for e in entries if all(e[k] == v for k, v in match.items())]


# --- GoZayaan: every campaign, its own code, common vs card -------------------------

def _gz_row(code, scope, who, pct, realized, cap=None, carrier="BS"):
    return {"channel": "gozayaan", "airline": carrier, "flight_type": "OUTBOUND",
            "coupon_code": code, "discount_pct": pct, "cap_bdt": cap, "realized_pct": realized,
            "eligibility_scope": scope, "eligibility": who, "name": f"{code} offer",
            "origin": "DAC", "destination": "DXB"}


def test_gozayaan_lists_every_campaign_not_only_the_best(memo):
    rows = [_gz_row("RFLYINT0926BSO", "common", "Any online payment", 8, 8),
            _gz_row("RVISA2026BRO4", "common", "Most cards", 4, 4),
            _gz_row("RAMEX0126PBB13O", "specific", "City Bank AMEX", 13, 13, 30000),
            _gz_row("RAMEX0126GBB11O", "specific", "City Bank AMEX", 11, 11, 25000)]
    memo._remember("gozayaan_routed", "gz.har", (rows, {"INTL": 2.1}))
    out = all_codes.collect(gozayaan_hars=["gz.har"])
    assert [e["code"] for e in out] == ["RFLYINT0926BSO", "RVISA2026BRO4",
                                        "RAMEX0126PBB13O", "RAMEX0126GBB11O"]  # common first, best first
    assert {e["tier"] for e in _codes(out, who="City Bank AMEX")} == {"Special"}
    amex = _codes(out, code="RAMEX0126PBB13O")[0]
    assert (amex["route"], amex["market"], amex["cap_bdt"], amex["fee_pct"], amex["effective"]) == \
        ("DAC-DXB", "INTL", 30000, 2.1, "13")


# --- ShareTrip: automatic code, wallet stack and every card coupon ------------------

_TERMS = [
    {"couponCode": "STLRSIQ326", "title": "Up to BDT 6,000 Savings with Stellar Signature",
     "discount": 18, "discountType": "percent", "maximumDiscountAmount": 6000, "withDiscount": "No"},
    {"couponCode": "FLYGPSTAR", "title": "Exclusive for GPStar Customers!", "discount": 1,
     "discountType": "percent", "maximumDiscountAmount": 5000, "withDiscount": "Yes"},
    {"couponCode": "BKASHDOM", "title": "bKash payment", "discount": 2, "discountType": "percent",
     "maximumDiscountAmount": 0, "withDiscount": "Yes"},
    {"couponCode": "ZEROEMI", "title": "0% EMI", "discount": 0, "discountType": "percent",
     "withDiscount": "No"},
]


def _st_details(airline, origin, dest, auto, base, ftype):
    row = {"channel": "sharetrip", "airline": airline, "flight_type": ftype,
           "coupon_terms": _TERMS, "origin": origin, "destination": dest}
    row.update(grid.sharetrip_har.judge_cell(auto, base, _TERMS))
    return row


def test_sharetrip_names_the_automatic_code_and_every_coupon(memo):
    details = [_st_details("BS", "DAC", "DXB", 8.5, 40000, "INTL")]
    search = [{"airline": "BS", "flight_type": "INTL", "discount_pct": 8.5, "coupon_code": "FLIGHTINT",
               "base_fare_bdt": 40000, "origin": "DAC", "destination": "DXB"},
              {"airline": "EK", "flight_type": "INTL", "discount_pct": 8.0, "coupon_code": "FLIGHTINT",
               "base_fare_bdt": 90000, "origin": "DAC", "destination": "DXB"}]
    memo._remember("sharetrip_routed", "st.har", (details, search))
    out = all_codes.collect(sharetrip_hars=["st.har"])

    bs = _codes(out, airline="BS")
    auto = _codes(bs, code="FLIGHTINT")[0]
    assert (auto["tier"], auto["who"], auto["effective"], auto["seen"]) == \
        ("Common", "Anyone (automatic)", "8.5", 2)        # details + search sighting merged
    stellar = _codes(bs, code="STLRSIQ326")[0]
    assert (stellar["tier"], stellar["who"], stellar["stacks"], stellar["cap_bdt"]) == \
        ("Special", "Stellar Signature", "No", 6000)
    assert stellar["title"] == "Up to BDT 6,000 Savings with Stellar Signature"
    assert "ZEROEMI" not in {e["code"] for e in out}                 # 0% utility coupons skipped
    # EK was only in the search: judged with the market's terms at ITS fare, so the
    # 6,000 cap binds on a 90k base (6.67%), unlike BS's 40k base (18%).
    assert _codes(out, airline="EK", code="STLRSIQ326")[0]["effective"] == "6.67"


def test_sharetrip_domestic_wallet_coupon_is_common_with_its_code(memo):
    details = [_st_details("BS", "DAC", "CXB", 5.0, 5000, "DOM")]
    memo._remember("sharetrip_routed", "st.har", (details, []))
    out = all_codes.collect(sharetrip_hars=["st.har"])
    wallet = _codes(out, code="BKASHDOM")[0]
    assert (wallet["tier"], wallet["who"], wallet["market"]) == ("Common", "bKash payment", "DOM")
    assert _codes(out, code="FLYGPSTAR")[0]["tier"] == "Special"


def test_a_range_shows_when_one_code_was_worth_different_amounts():
    obs = [all_codes._obs(market="INTL", route="DAC-DXB", ota="ShareTrip-B2C", airline="EK",
                          tier="Special", code="STLRSIQ326", who="Stellar", published=18,
                          effective=eff) for eff in (13.75, 0.87, 9.0)]
    (entry,) = all_codes.merge(obs)
    assert (entry["effective"], entry["seen"]) == ("0.87-13.75", 3)


# --- FirstTrip B2C: dynamic (common), general promo (common), card coupon (special) --

def _ft_row(**kw):
    row = {"airline": "BS", "origin": "DAC", "destination": "CXB", "base_fare_bdt": 4000,
           "coupon_code": "", "headline_rate": 0, "coupon_cap_bdt": None,
           "dynamic_code": None, "dynamic_rate": None, "special_code": None, "special_rate": None}
    row.update(kw)
    return row


def test_firsttrip_classes_codes_by_who_can_use_them():
    rows = {("DAC", "CXB", "2026-10-10"): [
        _ft_row(dynamic_code="FTBSDOM", dynamic_rate=14, coupon_code="FTEBLDOM07",
                headline_rate=16, coupon_cap_bdt=500),
        _ft_row(coupon_code="FTFLYDOM", headline_rate=10)]}
    out = all_codes.collect(b2c_rows_by_route=rows, b2c_fees={"common": 1.5, "card": 2.0})
    dyn = _codes(out, code="FTBSDOM")[0]
    assert (dyn["tier"], dyn["fee_pct"]) == ("Common", 1.5)
    ebl = _codes(out, code="FTEBLDOM07")[0]
    # 16% of 4,000 = 640, capped at 500 -> 12.5% actually saved
    assert (ebl["tier"], ebl["who"], ebl["effective"], ebl["fee_pct"]) == ("Special", "EBL", "12.5", 2.0)
    assert _codes(out, code="FTFLYDOM")[0]["tier"] == "Common"


def test_one_channel_failing_only_drops_that_channel(memo, capsys):
    memo._remember("gozayaan_routed", "gz.har", ([_gz_row("X1", "common", "Any", 5, 5)], {}))
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
    memo._remember("gozayaan_routed", "gz.har", (
        [_gz_row("RAMEX0126PBB13O", "specific", "City Bank AMEX", 13, 13, 30000)], {"INTL": 2.1}))
    report = _report(all_codes.collect(gozayaan_hars=["gz.har"]))
    path = Path(tempfile.mkdtemp()) / "r.xlsx"
    grid.write_single_sheet_xlsx(report, None, path)
    wb = load_workbook(path)
    ws = wb["04 October (all codes)"]
    heads = [c.value for c in ws[3]]
    assert heads[:7] == ["Market", "Route", "OTA", "Airline", "Tier", "Promo code", "Who can use it"]
    row = {h: c.value for h, c in zip(heads, ws[4])}
    assert (row["Promo code"], row["Tier"], row["Route"], row["Cap (BDT)"]) == \
        ("RAMEX0126PBB13O", "Special", "DAC-DXB", 30000)
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
                                            published=5, effective=5)])
    assert "all_codes" not in json.dumps(sanitize_report_for_sync(_report(entry)))
