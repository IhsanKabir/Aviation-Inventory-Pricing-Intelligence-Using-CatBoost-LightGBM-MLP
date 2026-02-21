import json
import unittest
from pathlib import Path
from unittest.mock import patch

from modules.parser import extract_offers_from_response as parse_bg
from modules.novoair_parser import extract_offers_from_response as parse_vq


REQUIRED_FIELDS = [
    "airline",
    "flight_number",
    "origin",
    "destination",
    "departure",
    "arrival",
    "cabin",
    "fare_basis",
    "brand",
    "price_total_bdt",
    "seat_available",
    "inventory_confidence",
    "source_endpoint",
]


def _load_fixture(name: str):
    p = Path("tests") / "fixtures" / name
    return json.loads(p.read_text(encoding="utf-8"))


class ConnectorContractTests(unittest.TestCase):
    @patch("modules.parser.resolve_seat_capacity", return_value=74)
    def test_bg_contract_fields_present(self, _mock):
        payload = _load_fixture("bg_response_contract.json")
        rows = parse_bg(payload)
        self.assertGreaterEqual(len(rows), 1)
        for row in rows:
            for f in REQUIRED_FIELDS:
                self.assertIn(f, row, f"missing field {f}")

    @patch("modules.novoair_parser.resolve_seat_capacity", return_value=72)
    def test_vq_contract_fields_present(self, _mock):
        payload = _load_fixture("vq_response_contract.json")
        rows = parse_vq(payload, requested_date="2026-03-20", requested_cabin="Economy")
        self.assertGreaterEqual(len(rows), 1)
        for row in rows:
            for f in REQUIRED_FIELDS:
                self.assertIn(f, row, f"missing field {f}")


if __name__ == "__main__":
    unittest.main()
