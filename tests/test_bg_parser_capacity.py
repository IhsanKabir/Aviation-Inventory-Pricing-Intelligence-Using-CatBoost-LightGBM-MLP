import unittest
from unittest.mock import patch

from modules.biman import _apply_passenger_mix_defaults
from modules.parser import extract_offers_from_response


class BGParserCapacityTests(unittest.TestCase):
    @patch("modules.parser.resolve_seat_capacity", return_value=None)
    def test_dash8_capacity_and_inventory(self, _mock_dynamic):
        payload = {
            "unbundledOffers": [
                [
                    {
                        "status": "AVAILABLE",
                        "brandId": "PROMO",
                        "seatsRemaining": {"count": 3},
                        "fare": {"alternatives": [[{"amount": 5000, "currency": "BDT"}]]},
                        "taxes": {"alternatives": [[{"amount": 1000, "currency": "BDT"}]]},
                        "total": {"alternatives": [[{"amount": 6000, "currency": "BDT"}]]},
                        "itineraryPart": [
                            {
                                "stops": 0,
                                "segments": [
                                    {
                                        "flight": {"airlineCode": "BG", "flightNumber": "123"},
                                        "equipment": "DH8",
                                        "origin": "DAC",
                                        "destination": "CXB",
                                        "departure": "2026-03-20T08:00:00",
                                        "arrival": "2026-03-20T09:00:00",
                                        "cabinClass": "Economy",
                                        "bookingClass": "Y",
                                        "fareBasis": "YOW",
                                    }
                                ],
                            }
                        ],
                    }
                ]
            ]
        }

        rows = extract_offers_from_response(payload)
        self.assertEqual(1, len(rows))
        row = rows[0]
        self.assertEqual(74, row["seat_capacity"])
        self.assertEqual(3, row["seat_available"])
        self.assertEqual("reported", row["inventory_confidence"])
        self.assertEqual("api/graphql:bookingAirSearch", row["source_endpoint"])
        self.assertAlmostEqual(95.9, row["estimated_load_factor_pct"], places=1)

    @patch("modules.parser.resolve_seat_capacity", return_value=162)
    def test_unknown_inventory_when_seats_missing(self, _mock_dynamic):
        payload = {
            "unbundledOffers": [
                [
                    {
                        "status": "AVAILABLE",
                        "brandId": "PROMO",
                        "fare": {"alternatives": [[{"amount": 5000, "currency": "BDT"}]]},
                        "taxes": {"alternatives": [[{"amount": 1000, "currency": "BDT"}]]},
                        "total": {"alternatives": [[{"amount": 6000, "currency": "BDT"}]]},
                        "itineraryPart": [
                            {
                                "segments": [
                                    {
                                        "flight": {"airlineCode": "BG", "flightNumber": "321"},
                                        "equipment": "738",
                                        "origin": "DAC",
                                        "destination": "CXB",
                                        "departure": "2026-03-20T10:00:00",
                                        "arrival": "2026-03-20T11:00:00",
                                        "cabinClass": "Economy",
                                    }
                                ]
                            }
                        ],
                    }
                ]
            ]
        }
        rows = extract_offers_from_response(payload)
        self.assertEqual(1, len(rows))
        self.assertIsNone(rows[0]["seat_available"])
        self.assertEqual("unknown", rows[0]["inventory_confidence"])

    def test_biman_passenger_defaults(self):
        rows = [{"adt_count": None, "chd_count": None, "inf_count": None}]
        payload = {"variables": {"airSearchInput": {"passengers": {"ADT": 1, "CHD": 0, "INF": 0}}}}
        _apply_passenger_mix_defaults(rows, payload)
        self.assertEqual(1, rows[0]["adt_count"])
        self.assertEqual(0, rows[0]["chd_count"])
        self.assertEqual(0, rows[0]["inf_count"])


if __name__ == "__main__":
    unittest.main()
