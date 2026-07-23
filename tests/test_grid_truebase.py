"""Regression tests for the --true-base recompute in ota_discount_grid.

Covers the two HIGH bugs found in review:
  1. BDFare margin must reuse the parser's commission_bdt (correct gross-agent fallback)
     instead of `(customer_net or agent) - agent`, which zeros out when customerNet is None.
  2. Unmatched domestic rows must be DROPPED in true-base mode, not left on their
     ratio-estimated value where summarize()'s max() could pick them.
Also asserts flag-OFF behaviour is unchanged.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "tools"))

import ota_discount_grid as g  # noqa: E402
from modules import true_base as tb_mod  # noqa: E402


def _oracle_bs():
    # BS gross 5549 -> base 4424 (implied fixed tax 1125)
    return tb_mod.build_from_rows(ft_b2b_rows=[
        {"airline": "BS", "origin": "DAC", "destination": "CGP",
         "gross_total_bdt": 5549, "base_fare_bdt": 4424}])


def _bd_row(airline, gross, agent, cust, comm_bdt, pct, dom=True):
    return {"channel": "bdfare", "persona": "B2B", "airline": airline,
            "origin": "DAC", "destination": "CGP", "domestic": dom,
            "gross_bdt": gross, "agent_bdt": agent, "customer_net_bdt": cust,
            "base_est_bdt": round(gross * 0.767), "base_source": "exact",
            "commission_bdt": comm_bdt, "commission_pct": pct}


def test_bdfare_truebase_is_agent_discount_off_gross(monkeypatch):
    # Unified model: (actual gross 5549 - agent 5211) / true base 4424 = 7.64%.
    rows = [_bd_row("BS", 5549, 5211, 5566, 355, 8.34)]
    monkeypatch.setattr(g.bdfare_har, "parse_commissions", lambda p, **kw: rows)
    cells = g.collect_bdfare("x.har", true_base=_oracle_bs())
    assert cells[("DOM", "BS")] == "7.64"   # NOT the 8.02 margin, NOT 0


def test_bdfare_truebase_drops_oracle_absent_domestic(monkeypatch):
    rows = [
        _bd_row("BS", 5549, 5211, 5566, 355, 8.34),
        _bd_row("QR", 5000, 4000, 4500, 500, 13.04),   # QR not in oracle -> must drop
    ]
    monkeypatch.setattr(g.bdfare_har, "parse_commissions", lambda p, **kw: rows)
    cells = g.collect_bdfare("x.har", true_base=_oracle_bs())
    assert cells[("DOM", "BS")] == "7.64"          # (5549-5211)/4424 agent discount
    assert ("DOM", "QR") not in cells              # dropped, not contaminated


def test_bdfare_flag_off_is_unchanged(monkeypatch):
    rows = [_bd_row("BS", 5549, 5211, 5566, 355, 8.34)]
    monkeypatch.setattr(g.bdfare_har, "parse_commissions", lambda p, **kw: rows)
    cells = g.collect_bdfare("x.har")              # no true_base -> original value
    assert cells[("DOM", "BS")] == "8.34"


def test_bdfare_intl_passthrough_in_truebase_mode(monkeypatch):
    # Intl row must be untouched (true base is domestic-only).
    rows = [_bd_row("EK", 60000, 55000, 58000, 3000, 6.5, dom=False)]
    monkeypatch.setattr(g.bdfare_har, "parse_commissions", lambda p, **kw: rows)
    cells = g.collect_bdfare("x.har", true_base=_oracle_bs())
    assert cells[("INTL", "EK")] == "6.5"


# --- GoZayaan convenience-surcharge annotation (fee parity with ShareTrip) ---

def _gz_summary():
    return {("BS", "DOM"): {"common_pct": 7.0,
                            "special": {"pct": 10.0, "eligibility": "Dhaka Bank Master"}},
            ("VQ", "DOM"): {"common_pct": 7.0, "special": None}}


def test_gozayaan_appends_surcharge_fee(monkeypatch):
    monkeypatch.setattr(g.gozayaan_har, "parse_discounts", lambda p, har=None: [])
    monkeypatch.setattr(g.gozayaan_har, "summarize_discounts", lambda rows: _gz_summary())
    monkeypatch.setattr(g.gozayaan_har, "parse_surcharge", lambda p, har=None: {"DOM": 2.1, "INTL": 2.1})
    cells = g.collect_gozayaan("x.har")
    # the booking-wide surcharge applies to the card special too
    assert cells[("DOM", "BS")] == "7(2.1% fee), 10 (Dhaka Bank Master, 2.1% fee)"
    assert cells[("DOM", "VQ")] == "7(2.1% fee)"


def test_firsttrip_b2c_two_tier_with_fees():
    from discount_engine.grid import _collect_firsttrip_b2c_rows
    # dynamic 14% = common (anyone); coupon 16% (FTEBLDOM07) = EBL card special
    rows = {("DAC", "CXB", "d"): [
        {"airline": "BS", "headline_rate": 16.0, "coupon_code": "FTEBLDOM07",
         "dynamic_rate": 14.0, "dynamic_code": "FTBSDOM",
         "realized_pct": 13.0, "coupon_cap_bdt": 515}]}
    assert _collect_firsttrip_b2c_rows(rows) == {("DOM", "BS"): "14, 16 (EBL)"}
    assert _collect_firsttrip_b2c_rows(rows, 1.0, 2.0) == {
        ("DOM", "BS"): "14(1% fee), 16 (EBL, 2% fee)"}
    # single-tier carrier (only common, no higher coupon) shows one rate
    solo = {("DAC", "CXB", "d"): [
        {"airline": "VQ", "headline_rate": 5.0, "coupon_code": "FTVQ",
         "dynamic_rate": 0.0, "dynamic_code": None,
         "realized_pct": 4.0, "coupon_cap_bdt": None}]}
    assert _collect_firsttrip_b2c_rows(solo, 1.0, 2.0) == {("DOM", "VQ"): "5(1% fee)"}


def test_firsttrip_b2c_keeps_highest_rate_across_routes_with_fee():
    """Regression: with a convenience fee present, the per-airline dedup must still keep
    the HIGHEST common rate across multiple DOM routes — not whichever route is last.
    (Before the fix, the fee-annotated cell couldn't be re-parsed and the last route won.)"""
    from discount_engine.grid import _collect_firsttrip_b2c_rows
    # BS is 12% on DAC-CGP but only 8% on DAC-CXB (processed LAST). 12% must survive.
    rows = {
        ("DAC", "CGP", "d"): [{"airline": "BS", "headline_rate": 12.0, "coupon_code": "FTBSDOM",
                               "dynamic_rate": 12.0, "dynamic_code": "FTBSDOM",
                               "realized_pct": 11.0, "coupon_cap_bdt": None}],
        ("DAC", "CXB", "d"): [{"airline": "BS", "headline_rate": 8.0, "coupon_code": "FTBSDOM",
                               "dynamic_rate": 8.0, "dynamic_code": "FTBSDOM",
                               "realized_pct": 7.0, "coupon_cap_bdt": None}],
    }
    assert _collect_firsttrip_b2c_rows(rows, 1.0, 2.0) == {("DOM", "BS"): "12(1% fee)"}
    # and with NO fee (the path that always worked) it also keeps 12
    assert _collect_firsttrip_b2c_rows(rows) == {("DOM", "BS"): "12"}


def test_firsttrip_b2c_card_coupon_without_dynamic_is_special_not_universal():
    """A card coupon (EBL) with NO general/dynamic discount must render as the card SPECIAL
    tier, not the universal common rate — anyone without the card gets 0%."""
    from discount_engine.grid import _collect_firsttrip_b2c_rows
    card_only = {("DAC", "CGP", "d"): [
        {"airline": "BS", "headline_rate": 16.0, "coupon_code": "FTEBLDOM07",
         "dynamic_rate": 0.0, "dynamic_code": None,
         "realized_pct": 15.0, "coupon_cap_bdt": None}]}
    assert _collect_firsttrip_b2c_rows(card_only) == {("DOM", "BS"): "0, 16 (EBL)"}
    # a GENERAL (non-card) promo coupon with no dynamic slot stays the universal common rate
    promo = {("DAC", "CGP", "d"): [
        {"airline": "BS", "headline_rate": 12.0, "coupon_code": "FTBSDOM",
         "dynamic_rate": 0.0, "dynamic_code": None,
         "realized_pct": 11.0, "coupon_cap_bdt": None}]}
    assert _collect_firsttrip_b2c_rows(promo) == {("DOM", "BS"): "12"}


def test_firsttrip_b2c_reads_dedicated_special_coupon_slot():
    """A card special carried ONLY in the offer's specialCoupon* fields (not couponCode)
    must still surface as the special tier (was previously dropped)."""
    from discount_engine.grid import _collect_firsttrip_b2c_rows
    rows = {("DAC", "CGP", "d"): [
        {"airline": "BS", "headline_rate": 14.0, "coupon_code": "FTBSDOM",
         "dynamic_rate": 0.0, "dynamic_code": None,
         "special_rate": 16.0, "special_code": "FTEBLDOM07",
         "realized_pct": 13.0, "coupon_cap_bdt": None}]}
    assert _collect_firsttrip_b2c_rows(rows) == {("DOM", "BS"): "14, 16 (EBL)"}


def test_firsttrip_parse_gateway_fee(tmp_path):
    import json
    from modules import firsttrip
    body = json.dumps({"data": {"data": [
        {"name": "Bkash", "chargePercentage": 1.5},
        {"name": "Nagad", "chargePercentage": 1.0},
        {"name": "Free", "chargePercentage": 0}]}})
    har = {"log": {"entries": [
        {"request": {"url": "https://api.firsttrip.com/api/PaymentGateway/GetActivePaymentGateway/"},
         "response": {"content": {"text": body}}}]}}
    p = tmp_path / "ft.har"
    p.write_text(json.dumps(har), encoding="utf-8")
    assert firsttrip.parse_b2c_gateway_fee(str(p)) == 1.0            # cheapest non-zero
    assert firsttrip.parse_b2c_gateway_fee(str(tmp_path / "none.har")) is None


def test_firsttrip_gateway_fee_from_booking_payment_method_list(tmp_path):
    """A booking/payment-page HAR carries fees under GetPaymentMethodList with
    convenienceChargeRate (not GetActivePaymentGateway/chargePercentage)."""
    import json
    from modules import firsttrip
    body = json.dumps({"data": [
        {"name": "Nagad", "gatewayChargeRate": 1.0, "convenienceChargeRate": 1.2},
        {"name": "bKash", "gatewayChargeRate": 1.5, "convenienceChargeRate": 1.65},
        {"name": "VISA", "gatewayChargeRate": 1.5, "convenienceChargeRate": 1.96},
        {"name": "EBL", "gatewayChargeRate": 1.5, "convenienceChargeRate": 1.96},
        {"name": "AMEX", "gatewayChargeRate": 3.0, "convenienceChargeRate": 2.91}]})
    har = {"log": {"entries": [
        {"request": {"url": "https://b2c-api.firsttrip.com/flight/api/v1/GeneralPurpose/GetPaymentMethodList"},
         "response": {"content": {"text": body}}}]}}
    p = tmp_path / "ft_booking.har"
    p.write_text(json.dumps(har), encoding="utf-8")
    fees = firsttrip.b2c_gateway_fees(firsttrip.parse_b2c_gateways(str(p)))
    assert fees == {"common": 1.2, "card": 1.96}    # Nagad cheapest; cheapest CARD = Visa/EBL


def test_gozayaan_no_surcharge_is_bare(monkeypatch):
    # No product_surcharge endpoint captured -> cells stay fee-free (no crash).
    monkeypatch.setattr(g.gozayaan_har, "parse_discounts", lambda p, har=None: [])
    monkeypatch.setattr(g.gozayaan_har, "summarize_discounts", lambda rows: _gz_summary())
    monkeypatch.setattr(g.gozayaan_har, "parse_surcharge", lambda p, har=None: {})
    cells = g.collect_gozayaan("x.har")
    assert cells[("DOM", "VQ")] == "7"


def test_sharetrip_parsers_accept_shared_har(monkeypatch):
    # The collector loads each big ShareTrip HAR once and passes har= to all three
    # parsers; a HAR must be read from disk at most once per file, not three times.
    from modules import sharetrip_har as st
    reads = {"n": 0}
    real_load = st._load_har
    def counting_load(p):
        reads["n"] += 1
        return {"log": {"entries": []}}
    monkeypatch.setattr(st, "_load_har", counting_load)
    monkeypatch.setattr(st, "summarize_details", lambda rows: {})
    monkeypatch.setattr(st, "summarize_discounts", lambda rows: {})
    g.collect_sharetrip_b2c(["a.har", "b.har"])
    assert reads["n"] == 2   # one load per file, NOT 3x (details+gateways+search)


def test_parse_surcharge_normalizes_int_to_intl(tmp_path):
    # product_type "INT" in the API must key as "INTL" (grid's route-type key).
    import json
    from modules import gozayaan_har as gz
    def _entry(pt, sur):
        return {"request": {"url": "https://x/api/business_rules/product_surcharge/"},
                "response": {"content": {"text": json.dumps(
                    {"result": {"surcharge": sur, "product_type": pt}})}}}
    har = tmp_path / "gz.har"
    har.write_text(json.dumps({"log": {"entries": [_entry("DOM", 2.1), _entry("INT", 2.1)]}}),
                   encoding="utf-8")
    assert gz.parse_surcharge(har) == {"DOM": 2.1, "INTL": 2.1}
    assert gz.parse_surcharge(tmp_path / "missing.har") == {}   # unreadable -> empty, no raise


if __name__ == "__main__":
    import subprocess
    raise SystemExit(subprocess.call([sys.executable, "-m", "pytest", __file__, "-q"]))
