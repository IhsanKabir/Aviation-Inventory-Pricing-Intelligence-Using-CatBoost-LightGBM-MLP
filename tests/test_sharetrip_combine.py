"""ShareTrip cells: judged coupons + market-terms combine.

Coupon TERMS (rates, caps, stackability) are market-uniform across airlines
(verified on the 2026-07-06 captures); only the automatic displayPrice.discount
is airline-specific. A booking capture supplies the terms; airlines seen only in
a search are judged with those terms at their OWN observed base fare, so caps
are re-evaluated per airline instead of copying one airline's special verbatim.
"""
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from discount_engine import grid  # noqa: E402


def _har(entries) -> str:
    p = Path(tempfile.mkdtemp(prefix="st_")) / "cap.har"
    p.write_text(json.dumps({"log": {"entries": entries}}), encoding="utf-8")
    return str(p)


def _corrupt_har() -> str:
    """A truncated HAR — valid JSON start, unterminated string (real field case:
    an interrupted browser export)."""
    p = Path(tempfile.mkdtemp(prefix="st_bad_")) / "cap.har"
    p.write_text('{"log": {"entries": [{"response": {"content": {"text": "abc', encoding="utf-8")
    return str(p)


def test_corrupt_sharetrip_har_is_skipped_not_fatal():
    # One truncated HAR among valid ones must NOT abort the run — it is skipped
    # with a warning and the valid files still produce cells.
    booking = _booking_har("BS", 7, 5000)
    cells = grid.collect_sharetrip_b2c([_corrupt_har(), booking])
    assert cells[("DOM", "BS")] == "9(Bkash), 18 (Stellar Signature)"


def _search_har(airlines_disc):
    mf = [{"legs": [{"marketingAirline": a}], "domestic": True,
           "displayPrice": {"discount": d, "totalFare": {"base": base}},
           "promotionalCoupon": {"couponCode": "FLYINSIDE"}}
          for a, d, base in airlines_disc]
    return _har([{"request": {"url": ".../api/v2/flight/search/available-flights", "method": "POST"},
                  "response": {"content": {"text": json.dumps({"response": {"matchedFlights": mf}})}}}])


_COUPONS = [
    {"couponCode": "bKASHDOM26", "title": "Exclusive for bKash Users", "discount": 2,
     "discountType": "Percentage", "withDiscount": "Yes", "maximumDiscountAmount": 0},
    {"couponCode": "STLRSDQ326", "title": "Stellar Signature quarterly", "discount": 18,
     "discountType": "Percentage", "withDiscount": "No", "maximumDiscountAmount": 1500},
    {"couponCode": "FLYINSIDE", "title": "Default gateway coupon", "discount": 0,
     "discountType": "Percentage", "withDiscount": "Yes", "maximumDiscountAmount": 0},
]


def _booking_har(airline, auto, base_fare, coupons=_COUPONS):
    resp = {"legs": [{"marketingAirline": airline}], "isDomestic": True,
            "displayPrice": {"discount": auto, "totalFare": {"base": base_fare}},
            "coupons": coupons}
    return _har([{"request": {"url": ".../api/v2/flight/search/details", "method": "POST"},
                  "response": {"content": {"text": json.dumps({"response": resp})}}}])


def test_one_booking_shares_terms_with_all_searched_airlines():
    search = _search_har([("BS", 7, 5000), ("2A", 6.5, 5000), ("BG", 6, 5000)])
    booking = _booking_har("BS", 7, 5000)
    cells = grid.collect_sharetrip_b2c([search, booking])

    # BS: exact booking cell — common 7+2 bKash, Stellar 18% of 5000 = 900 < cap 1500.
    assert cells[("DOM", "BS")] == "9(Bkash), 18 (Stellar Signature)"
    # 2A / BG appear ONLY in the search, judged with the shared TERMS at their fare.
    assert cells[("DOM", "2A")] == "8.5(Bkash), 18 (Stellar Signature)"
    assert cells[("DOM", "BG")] == "8(Bkash), 18 (Stellar Signature)"


def test_search_fill_reevaluates_caps_at_each_airlines_fare():
    # BG's fare is 50,000: Stellar 18% = 9,000, capped at 1,500 -> effective 3%.
    search = _search_har([("BG", 6, 50000)])
    booking = _booking_har("BS", 7, 5000)
    cells = grid.collect_sharetrip_b2c([search, booking])
    assert cells[("DOM", "BG")] == "8(Bkash), 3 (Stellar Signature, capped)"
    # The cheap-fare booking cell keeps the honest un-capped 18.
    assert cells[("DOM", "BS")] == "9(Bkash), 18 (Stellar Signature)"


def test_per_airline_booking_still_takes_precedence():
    search = _search_har([("2A", 6.5, 5000)])
    booking = _booking_har("2A", 8, 5000)
    cells = grid.collect_sharetrip_b2c([search, booking])
    assert cells[("DOM", "2A")] == "10(Bkash), 18 (Stellar Signature)"   # booking, not search 6.5


def test_default_zero_coupon_never_labels_the_common_rate():
    # Search-only cell with NO booking terms: plain automatic rate, no "(Bkash)"
    # stamp from the 0% FLYINSIDE promotionalCoupon (the old mislabel).
    search = _search_har([("VQ", 5.5, 5000)])
    cells = grid.collect_sharetrip_b2c([search])
    assert cells[("DOM", "VQ")] == "5.5"


def test_single_string_path_still_accepted():
    booking = _booking_har("BS", 7, 5000)
    cells = grid.collect_sharetrip_b2c(booking)   # back-compat: bare string
    assert cells[("DOM", "BS")] == "9(Bkash), 18 (Stellar Signature)"


if __name__ == "__main__":
    import subprocess
    raise SystemExit(subprocess.call([sys.executable, "-m", "pytest", __file__, "-q"]))
