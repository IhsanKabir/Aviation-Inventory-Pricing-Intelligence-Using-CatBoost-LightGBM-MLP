"""Cap-aware coupon judging — pinned to the REAL 2026-07-06 capture numbers.

Ground truth (verified by direct arithmetic on the six per-airline booking HARs):
  * automatic displayPrice.discount is a percent of BASE fare, floored, and is
    airline-specific (VQ 5.5 / BG 6 / 2A 6.5 / BS 7 dom; BG 6.5 / BS 7.1 intl);
  * coupon terms are market-uniform; caps invert rankings on intl fares:
    on BS intl (base 80,765) the "18%" Stellar Signature is capped at 6,000 BDT
    (= 7.43% of base) and LOSES to the 1% GPStar loyalty stack (807 + auto 5,734
    = 6,541 BDT = 8.10% of base);
  * international has NO wallet (bKash/Nagad) coupon — the common rate is the
    automatic discount alone.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from discount_engine import grid  # noqa: E402
from modules import sharetrip_har  # noqa: E402

# Real INTL coupon terms (subset of the uniform 2026-07-06 set, verbatim values).
INTL_COUPONS = [
    {"couponCode": "SKYINT0726", "title": "Exclusive for SkyTrip Cardholders", "discount": 15,
     "discountType": "Percentage", "withDiscount": "No", "maximumDiscountAmount": 5000},
    {"couponCode": "STLRSIQ326", "title": "Up to 20x usage per quarter with Stellar Signature",
     "discount": 18, "discountType": "Percentage", "withDiscount": "No", "maximumDiscountAmount": 6000},
    {"couponCode": "STLRPIQ326", "title": "Up to 20x usage per quarter with Stellar Platinum",
     "discount": 15, "discountType": "Percentage", "withDiscount": "No", "maximumDiscountAmount": 3000},
    {"couponCode": "FLYGPSTAR", "title": "Exclusive for GPStar Customers!", "discount": 1,
     "discountType": "Percentage", "withDiscount": "Yes", "maximumDiscountAmount": 5000},
    {"couponCode": "FLIGHTINT", "title": "Your Gateway to Savings", "discount": 0,
     "discountType": "Percentage", "withDiscount": "Yes", "maximumDiscountAmount": 0},
]

# Real DOM coupon terms (subset).
DOM_COUPONS = [
    {"couponCode": "bKASHDOM26", "title": "Exclusive for bKash Users", "discount": 2,
     "discountType": "Percentage", "withDiscount": "Yes", "maximumDiscountAmount": 0},
    {"couponCode": "NAGADDOM26", "title": "Exclusive for Nagad Users!", "discount": 0.5,
     "discountType": "Percentage", "withDiscount": "Yes", "maximumDiscountAmount": 0},
    {"couponCode": "FLYBA0526", "title": "Best Deal on Domestic Flight with Bank Asia",
     "discount": 5, "discountType": "Percentage", "withDiscount": "Yes", "maximumDiscountAmount": 0},
    {"couponCode": "STLRSDQ326", "title": "Up to 20x usage per quarter with Stellar Signature",
     "discount": 18, "discountType": "Percentage", "withDiscount": "No", "maximumDiscountAmount": 1500},
    {"couponCode": "SKYDOM0726", "title": "Exclusive for SkyTrip Cardholders", "discount": 15,
     "discountType": "Percentage", "withDiscount": "No", "maximumDiscountAmount": 1000},
    {"couponCode": "FLYINSIDE", "title": "Your Gateway to Savings on All Online Payments",
     "discount": 0, "discountType": "Percentage", "withDiscount": "Yes", "maximumDiscountAmount": 0},
]


def test_intl_cap_inversion_loyalty_stack_beats_18pct_card():
    # BS intl, base 80,765 / auto 7.1% — the real 2026-07-06 numbers.
    cell = sharetrip_har.judge_cell(7.1, 80765, INTL_COUPONS)
    assert cell["common_pct"] == 7.1          # no wallet coupon on intl
    assert cell["common_code"] is None
    # Winner: GPStar 1% stack -> 807 + 5,734 auto = 6,541 = 8.10% of base.
    assert cell["special_pct"] == 8.1
    assert cell["special_label"] == "GPStar"
    assert cell["special_capped"] is False
    # Best CARD shown alongside: Stellar Signature capped at 6,000 = 7.43%.
    assert cell["card_pct"] == 7.4
    assert cell["card_label"] == "Stellar Signature"
    assert cell["card_capped"] is True


def test_intl_cell_text_shows_judged_values_not_raw_18():
    cell = sharetrip_har.judge_cell(7.1, 80765, INTL_COUPONS)
    assert grid._sharetrip_cell_text(cell) == \
        "7.1, 8.1 (GPStar), 7.4 (Stellar Signature, capped)"


def test_dom_caps_dont_bind_on_cheap_fares():
    # VQ dom, base 4,424 / auto 5.5%: Stellar 18% = 796 BDT < cap 1,500.
    cell = sharetrip_har.judge_cell(5.5, 4424, DOM_COUPONS)
    assert cell["common_pct"] == 7.5          # 5.5 auto + 2 bKash
    assert cell["common_code"] == "bKASHDOM26"
    assert cell["special_pct"] == 18.0        # 796/4424 = 17.99 -> 18.0
    assert cell["special_label"] == "Stellar Signature"
    assert cell["special_capped"] is False
    assert cell["card_pct"] is None           # best card IS the winner: no duplicate
    assert grid._sharetrip_cell_text(cell) == "7.5(Bkash), 18 (Stellar Signature)"


def test_judged_table_covers_all_nonzero_coupons():
    cell = sharetrip_har.judge_cell(5.5, 4424, DOM_COUPONS)
    codes = {j["code"]: j for j in cell["judged"]}
    assert "FLYINSIDE" not in codes           # 0% coupons carry no money
    # Bank Asia 5% stack: 221 + 243 auto = 464 = 10.49% — judged, not ignored.
    assert codes["FLYBA0526"]["effective_pct"] == 10.49
    assert codes["FLYBA0526"]["stacks_with_auto"] is True
    assert codes["NAGADDOM26"]["label"] == "Nagad"


def test_stellar_signature_and_platinum_get_distinct_labels():
    cell = sharetrip_har.judge_cell(7.1, 80765, INTL_COUPONS)
    labels = {j["code"]: j["label"] for j in cell["judged"]}
    assert labels["STLRSIQ326"] == "Stellar Signature"
    assert labels["STLRPIQ326"] == "Stellar Platinum"


def test_no_observed_fare_falls_back_to_nominal_percentages():
    # base<=0: caps can't be evaluated; nominal % used, nothing marked capped.
    cell = sharetrip_har.judge_cell(6, 0, DOM_COUPONS)
    assert cell["common_pct"] == 8            # 6 + 2 bKash
    assert cell["special_pct"] == 18.0
    assert cell["special_capped"] is False


# Real gateway ids (subset of the 114-entry catalog) with their convenience charges.
GATEWAYS = {
    "ddb4e30d59cd428e8f0c22e10eb40e2d": {"name": "bKash", "charge_pct": 2.0},
    "3f693d87f77c4676b3b0846d8abf1832": {"name": "Nagad", "charge_pct": 1.5},
    "9wyawmn8hd4oks4ocww0sww8ck0o448": {"name": "Stellar Signature", "charge_pct": 2.0},
    "bjl4v8f8h7kkckowwwoc0gwow4c0g4k": {"name": "SkyTrip", "charge_pct": 2.0},
    "lif63ywzn80csckck4s0wskogk4skcc": {"name": "Stellar Platinum", "charge_pct": 2.0},
}

INTL_WITH_GATEWAYS = [
    {**INTL_COUPONS[1], "gateway": ["9wyawmn8hd4oks4ocww0sww8ck0o448"]},         # Stellar Sig 2%
    {**INTL_COUPONS[3], "gateway": ["ddb4e30d59cd428e8f0c22e10eb40e2d",
                                    "3f693d87f77c4676b3b0846d8abf1832"]},        # GPStar: min 1.5%
]

DOM_WITH_GATEWAYS = [
    {**DOM_COUPONS[0], "gateway": ["ddb4e30d59cd428e8f0c22e10eb40e2d"]},         # bKash 2%
    {**DOM_COUPONS[3], "gateway": ["9wyawmn8hd4oks4ocww0sww8ck0o448"]},          # Stellar Sig 2%
]


def test_convenience_fee_nets_and_annotates():
    # BS intl with fees: GPStar net = (6541 - 1.5% x 84,918)/80,765 = 6.52%;
    # Stellar Sig net = (6000 - 2% x 85,459)/80,765 = 5.31% — GPStar still wins.
    cell = sharetrip_har.judge_cell(7.1, 80765, INTL_WITH_GATEWAYS, total_fare=91459,
                                    gateways=GATEWAYS)
    by_code = {j["code"]: j for j in cell["judged"]}
    assert by_code["FLYGPSTAR"]["fee_pct"] == 1.5
    assert by_code["FLYGPSTAR"]["net_pct"] == 6.52
    assert by_code["STLRSIQ326"]["fee_pct"] == 2.0
    assert by_code["STLRSIQ326"]["net_pct"] == 5.31
    assert cell["special_label"] == "GPStar"          # ranked by NET
    assert cell["special_fee_pct"] == 1.5
    assert grid._sharetrip_cell_text(cell) == \
        "7.1, 8.1 (GPStar, 1.5% fee), 7.4 (Stellar Signature, capped, 2% fee)"


def test_wallet_fee_annotated_on_common():
    cell = sharetrip_har.judge_cell(5.5, 4424, DOM_WITH_GATEWAYS, total_fare=5549,
                                    gateways=GATEWAYS)
    assert cell["common_fee_pct"] == 2.0
    assert grid._sharetrip_cell_text(cell) == \
        "7.5(Bkash, 2% fee), 18 (Stellar Signature, 2% fee)"


def test_no_gateway_catalog_keeps_effective_ranking_and_clean_text():
    # Without the fee catalog the judge behaves exactly as before (no annotations).
    cell = sharetrip_har.judge_cell(5.5, 4424, DOM_COUPONS)
    assert all(j["fee_pct"] is None and j["net_pct"] is None for j in cell["judged"])
    assert grid._sharetrip_cell_text(cell) == "7.5(Bkash), 18 (Stellar Signature)"


if __name__ == "__main__":
    import subprocess
    raise SystemExit(subprocess.call([sys.executable, "-m", "pytest", __file__, "-q"]))
