"""ShareTrip 'search + one booking -> detailed for all airlines' combine.

The card special (Stellar/EBL 18%) is uniform across airlines (verified on real
captures), so one booking supplies it for everyone while the search supplies each
airline's common rate — no per-airline booking capture needed.
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


def _search_har(airlines_disc):
    mf = [{"legs": [{"marketingAirline": a}], "domestic": True,
           "displayPrice": {"discount": d, "totalFare": {"base": 5000}},
           "promotionalCoupon": {"couponCode": "FLYINSIDE"}}
          for a, d in airlines_disc]
    return _har([{"request": {"url": ".../api/v2/flight/search/available-flights", "method": "POST"},
                  "response": {"content": {"text": json.dumps({"response": {"matchedFlights": mf}})}}}])


def _booking_har(airline, base, special_pct, special_title):
    resp = {"legs": [{"marketingAirline": airline}], "isDomestic": True,
            "displayPrice": {"discount": base},
            "coupons": [
                {"couponCode": "BKASH", "title": "bKash", "discount": 2,
                 "discountType": "percentage", "withDiscount": "Yes"},
                {"couponCode": "SPECIAL", "title": special_title, "discount": special_pct,
                 "discountType": "percentage", "withDiscount": "No"},
            ]}
    return _har([{"request": {"url": ".../api/v2/flight/search/details", "method": "POST"},
                  "response": {"content": {"text": json.dumps({"response": resp})}}}])


def test_one_booking_covers_all_searched_airlines():
    search = _search_har([("BS", 7), ("2A", 6.5), ("BG", 6)])
    booking = _booking_har("BS", 7, 18, "Stellar Bank")
    cells = grid.collect_sharetrip_b2c([search, booking])

    # BS uses its exact booking cell (base 7 + bKash 2 = 9), plus the special.
    assert cells[("DOM", "BS")] == "9(Bkash), 18 (Stellar)"
    # 2A / BG appear ONLY in the search, yet inherit the uniform 18% (Stellar).
    assert cells[("DOM", "2A")] == "6.5(Bkash), 18 (Stellar)"
    assert cells[("DOM", "BG")] == "6(Bkash), 18 (Stellar)"


def test_per_airline_booking_still_takes_precedence():
    # A booking capture for 2A overrides the search-derived cell for 2A.
    search = _search_har([("2A", 6.5)])
    booking = _booking_har("2A", 8, 18, "Stellar Bank")
    cells = grid.collect_sharetrip_b2c([search, booking])
    assert cells[("DOM", "2A")] == "10(Bkash), 18 (Stellar)"   # exact booking, not search 6.5


def test_single_string_path_still_accepted():
    booking = _booking_har("BS", 7, 18, "Stellar Bank")
    cells = grid.collect_sharetrip_b2c(booking)   # back-compat: bare string
    assert cells[("DOM", "BS")] == "9(Bkash), 18 (Stellar)"


if __name__ == "__main__":
    import subprocess
    raise SystemExit(subprocess.call([sys.executable, "-m", "pytest", __file__, "-q"]))
