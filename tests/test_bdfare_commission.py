"""BDFare commission = grossAmount - agentAmount (VAT-neutral), % of base.

Field-verified on BG 247 DAC-DXB (2026-07-07 screenshots): gross 45,834 /
agent payable 43,337 / real base 35,138 -> 7.106%. The old numerator
(customerNetAmount - agentAmount = 2,635) counted the AIT VAT the agent pays
regardless of channel and overstated the rate (7.5%).
"""
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from modules import bdfare_har  # noqa: E402


def _fare_summary(itinerary_id, airline, base, tax, origin="DAC", dest="DXB"):
    """A GetAirSearchItinerary entry shaped like the real July-2 capture."""
    return {
        "request": {"url": "https://bdfare.com/bdfare-search/api/v2/Search/"
                           f"GetAirSearchItinerary?requestId=r1&itineraryId={itinerary_id}",
                    "method": "GET"},
        "response": {"content": {"text": json.dumps({
            "requestId": "r1",
            "flightInfos": [{
                "itineraries": [{"departure": origin, "arrival": dest,
                                 "legs": [{"airlineCode": airline}]}],
                "travelerFareSummaries": [{"travelerType": "Adult",
                                           "baseFare": base, "tax": tax}],
            }],
        })}},
    }


def _har(offers, details=None, extra_entries=()) -> str:
    entries = [{
        "request": {"url": "https://bdfare.com/bdfare-search/api/v2/Search/AirSearch",
                    "method": "POST"},
        "response": {"content": {"text": json.dumps({"offers": offers})}},
    }]
    if details is not None:
        entries.append({
            "request": {"url": "https://bdfare.com/bdfare-search/api/v2/Search/GetAirSearchItinerary",
                        "method": "GET"},
            "response": {"content": {"text": json.dumps(details)}},
        })
    entries.extend(extra_entries)
    p = Path(tempfile.mkdtemp(prefix="bdf_")) / "cap.har"
    p.write_text(json.dumps({"log": {"entries": entries}}), encoding="utf-8")
    return str(p)


BG247 = {  # the user's verified flight
    "airlineCode": "BG", "grossAmount": 45834, "agentAmount": 43337,
    "customerNetAmount": 45972, "itineraryId": "itin-bg247",
    "journeyWises": [{"departure": "DAC", "arrival": "DXB"}],
}


def test_commission_is_gross_minus_agent_with_exact_base():
    # The offer's own Fare Summary was captured -> exact base 35,138.
    har = _har([BG247], extra_entries=[_fare_summary("itin-bg247", "BG", 35138, 10696)])
    (row,) = bdfare_har.parse_commissions(har)
    assert row["commission_bdt"] == 2497            # 45,834 - 43,337 (VAT excluded)
    assert row["commission_pct"] == 7.11            # 2,497 / 35,138 -> the user's 7.106%
    assert row["base_source"] == "exact"
    assert row["domestic"] is False


def test_fallback_ratio_still_close_when_no_detail_captured():
    rows = bdfare_har.parse_commissions(_har([BG247]))
    (row,) = rows
    # base est = 45,834 x 0.767 = 35,155 -> 2,497 / 35,155 = 7.10%
    assert row["commission_bdt"] == 2497
    assert abs(row["commission_pct"] - 7.10) < 0.02
    assert row["base_source"] == "default_ratio"


AI238 = {  # field case 2026-07-07: huge tax share broke the global-ratio estimate
    "airlineCode": "AI", "grossAmount": "BDT 40468", "agentAmount": "BDT 38686",
    "customerNetAmount": "BDT 40590", "itineraryId": "itin-ai238",
    "journeyWises": [{"departure": "DAC", "arrival": "DXB"}],
}


def test_ai_high_tax_fare_uses_exact_base_not_global_ratio():
    # Grid showed 5.74% (gross x 0.767 = 31,039 est. base); truth is
    # (40,468 - 38,686) / 23,803 = 7.49% — AI's base share is 58.8%, not 76.7%.
    har = _har([AI238], extra_entries=[_fare_summary("itin-ai238", "AI", 23803, 16665)])
    (row,) = bdfare_har.parse_commissions(har)
    assert row["commission_bdt"] == 1782
    assert row["commission_pct"] == 7.49
    assert row["base_source"] == "exact"


def test_same_airline_route_ratio_covers_unopened_offers():
    # A second AI DAC-DXB offer without its own Fare Summary inherits AI's
    # measured ratio (23,803/40,468 = 0.5882) instead of the global 0.767.
    other = {"airlineCode": "AI", "grossAmount": 50000, "agentAmount": 46000,
             "customerNetAmount": 50150, "itineraryId": "itin-ai-other",
             "journeyWises": [{"departure": "DAC", "arrival": "DXB"}]}
    har = _har([AI238, other], extra_entries=[_fare_summary("itin-ai238", "AI", 23803, 16665)])
    rows = {r["gross_bdt"]: r for r in bdfare_har.parse_commissions(har)}
    assert rows[50000]["base_source"] == "airline_ratio"
    assert rows[50000]["base_est_bdt"] == round(50000 * 23803 / 40468)
    assert rows[40468]["base_source"] == "exact"


def test_nearby_airport_and_mixed_carrier_offers_are_skipped():
    # Field case 2026-07-07: a DAC-DXB search returned a BG+EY itinerary ending at
    # DAC-XNB (Dubai Chelsea BUS STATION) via BDFare's nearby-airport feature —
    # neither the searched route nor one airline's fare. Both guards must drop it.
    bus_leg = {"airlineCode": "BG", "grossAmount": "BDT 170900",
               "agentAmount": "BDT 159499", "customerNetAmount": "BDT 171413",
               "nearbyAirports": ["XNB"],
               "flightSummary": [{"airlineCode": ["BG", "EY"]}],
               "journeyWises": [{"departure": "DAC", "arrival": "XNB"}]}
    mixed_only = {"airlineCode": "BG", "grossAmount": 99000, "agentAmount": 92000,
                  "customerNetAmount": 99300,
                  "flightSummary": [{"airlineCode": ["BG", "EY"]}],
                  "journeyWises": [{"departure": "DAC", "arrival": "DXB"}]}
    rows = bdfare_har.parse_commissions(_har([BG247, bus_leg, mixed_only]))
    assert [r["gross_bdt"] for r in rows] == [45834]     # only the real BG fare


def test_cell_uses_cheapest_offer_not_best_percent():
    # Field case 2026-07-07: a premium 170k DAC-XNB itinerary paid 8.7% while the
    # lead 65k DAC-DXB economy fare paid ~7.2 — the cell must reflect the fare
    # people actually compare (cheapest), with the spread kept for the run log.
    premium = {"airlineCode": "BG", "grossAmount": 170900, "agentAmount": 159499,
               "customerNetAmount": 171413,
               "journeyWises": [{"departure": "DAC", "arrival": "XNB"}]}
    rows = bdfare_har.parse_commissions(_har([BG247, premium]))
    summary = bdfare_har.summarize_commissions(rows)
    cell = summary[("INTL", "BG")]
    assert cell["offer_gross_bdt"] == 45834          # cheapest wins the cell
    assert cell["value"] == rows[0]["commission_pct"] if rows[0]["gross_bdt"] == 45834 \
        else cell["value"] == 7.11
    assert cell["n_offers"] == 2
    assert cell["pct_max"] > cell["value"]           # premium spread preserved


if __name__ == "__main__":
    import subprocess
    raise SystemExit(subprocess.call([sys.executable, "-m", "pytest", __file__, "-q"]))
