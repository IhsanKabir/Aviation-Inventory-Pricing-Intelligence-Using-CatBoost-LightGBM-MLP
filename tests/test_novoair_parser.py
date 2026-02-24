import unittest
from unittest.mock import patch

from modules.novoair_parser import extract_offers_from_response


class NovoairParserTests(unittest.TestCase):
    @patch("modules.novoair_parser.resolve_seat_capacity", return_value=72)
    def test_flight_selection_rows_include_inventory_and_refs(self, _mock_capacity):
        resp = {
            "flightSearchModel": {
                "flightSearchData": {
                    "adultCount": 1,
                    "childCount": 0,
                    "infantCount": 0,
                }
            },
            "flightResultsModel": {"fareSearchReference": "ref-123"},
            "flightSelections": {
                "fareRefNum": "frn-456",
                "currency": {"code": "BDT"},
                "fareFamilies": [
                    {"code": "PP", "title": "Special Promo", "cabin": "Y"},
                    {"code": "FL", "title": "Flexible", "cabin": "Y"},
                ],
                "flightBlocks": [
                    {
                        "from": "DAC",
                        "into": "CXB",
                        "flightDates": [
                            {
                                "date": "2026-03-20",
                                "flights": [
                                    {
                                        "itinerary": [
                                            {
                                                "flight": "VQ-927",
                                                "type": "ATR725",
                                                "TOD": "2026-03-20T13:00:00",
                                                "TOA": "2026-03-20T14:05:00",
                                                "stops": None,
                                            }
                                        ],
                                        "familyFares": {
                                            "PP": {"one": 5000, "all": 5000, "seats": 5},
                                            "FL": {"one": 9000, "all": 9000, "seats": None},
                                        },
                                    }
                                ],
                            }
                        ],
                    }
                ],
            },
        }

        rows = extract_offers_from_response(resp, requested_date="2026-03-20", requested_cabin="Economy")
        self.assertEqual(2, len(rows))

        by_fare = {r["fare_basis"]: r for r in rows}
        promo = by_fare["PP"]
        flex = by_fare["FL"]

        self.assertEqual(5, promo["seat_available"])
        self.assertEqual("reported", promo["inventory_confidence"])
        self.assertEqual(72, promo["seat_capacity"])
        self.assertEqual("flight_selection.aspx?ajax=true&action=flightSearch", promo["source_endpoint"])
        self.assertEqual("frn-456", promo["fare_ref_num"])
        self.assertEqual("ref-123", promo["fare_search_reference"])
        self.assertEqual(1, promo["adt_count"])
        self.assertEqual(0, promo["chd_count"])
        self.assertEqual(0, promo["inf_count"])

        self.assertIsNone(flex["seat_available"])
        self.assertEqual("unknown", flex["inventory_confidence"])

    @patch("modules.novoair_parser.resolve_seat_capacity", return_value=72)
    def test_passenger_info_rows_keep_seat_null(self, _mock_capacity):
        resp = {
            "bookingSummary": {
                "PaxTypeCount": {"ADT": 1, "CHD": 0, "INF": 0},
                "Itinerary": {
                    "travelSegments": [
                        {
                            "tripSegments": [
                                {
                                    "departureCityCode": "DAC",
                                    "arrivalCityCode": "CXB",
                                    "departing": "2026-03-20T12:30:00",
                                    "arriving": "2026-03-20T13:25:00",
                                    "flightNumber": "VQ-909",
                                    "aircraftType": "ATR725",
                                    "cabinClass": "Y",
                                    "stops": 0,
                                }
                            ],
                            "costSummary": [
                                {
                                    "currencyCode": "BDT",
                                    "fareBasis": "KOW",
                                    "baseFare": 5425.0,
                                    "totalTaxes": 925.0,
                                    "totalFees": 200.0,
                                    "totalSurcharges": 50.0,
                                    "totalFare": 6600.0,
                                }
                            ],
                        }
                    ]
                },
            }
        }

        rows = extract_offers_from_response(resp, requested_date="2026-03-20", requested_cabin="Economy")
        self.assertEqual(1, len(rows))
        row = rows[0]
        self.assertIsNone(row["seat_available"])
        self.assertEqual("unknown", row["inventory_confidence"])
        self.assertEqual("passenger_info.aspx?get=DATA", row["source_endpoint"])


if __name__ == "__main__":
    unittest.main()
