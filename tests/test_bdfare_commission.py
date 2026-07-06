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


def _har(offers, details=None) -> str:
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
    p = Path(tempfile.mkdtemp(prefix="bdf_")) / "cap.har"
    p.write_text(json.dumps({"log": {"entries": entries}}), encoding="utf-8")
    return str(p)


BG247 = {  # the user's verified flight
    "airlineCode": "BG", "grossAmount": 45834, "agentAmount": 43337,
    "customerNetAmount": 45972,
    "journeyWises": [{"departure": "DAC", "arrival": "DXB"}],
}


def test_commission_is_gross_minus_agent_not_customer_net():
    # Itinerary detail gives the exact base/(base+tax) ratio: 35,138 / 45,834.
    rows = bdfare_har.parse_commissions(_har([BG247], details={"baseFare": 35138, "tax": 10696}))
    (row,) = rows
    assert row["commission_bdt"] == 2497            # 45,834 - 43,337 (VAT excluded)
    assert row["commission_pct"] == 7.11            # 2,497 / 35,138 -> the user's 7.106%
    assert row["domestic"] is False


def test_fallback_ratio_still_close_when_no_detail_captured():
    rows = bdfare_har.parse_commissions(_har([BG247]))
    (row,) = rows
    # base est = 45,834 x 0.767 = 35,155 -> 2,497 / 35,155 = 7.10%
    assert row["commission_bdt"] == 2497
    assert abs(row["commission_pct"] - 7.10) < 0.02


if __name__ == "__main__":
    import subprocess
    raise SystemExit(subprocess.call([sys.executable, "-m", "pytest", __file__, "-q"]))
