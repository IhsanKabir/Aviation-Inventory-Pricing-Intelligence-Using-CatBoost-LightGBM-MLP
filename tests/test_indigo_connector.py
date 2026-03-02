import os
import unittest
from unittest.mock import MagicMock, patch

from modules.indigo import fetch_flights


class _DummyResp:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body
        self.text = body if isinstance(body, str) else str(body)

    def json(self):
        if isinstance(self._body, Exception):
            raise self._body
        if isinstance(self._body, str):
            raise ValueError("not json")
        return self._body


class IndigoConnectorTests(unittest.TestCase):
    @patch("modules.indigo.Requester")
    @patch.dict(os.environ, {"INDIGO_SOURCE_MODE": "direct", "INDIGO_TOKEN_REFRESH_ENABLED": "1"}, clear=False)
    def test_direct_normalizes_rows(self, mock_requester_cls):
        requester = MagicMock()
        requester.timeout = 30
        requester.session = MagicMock()
        mock_requester_cls.return_value = requester

        search_body = {
            "data": {
                "currencyCode": "BDT",
                "trips": [
                    {
                        "origin": "DAC",
                        "destination": "CCU",
                        "journeysAvailable": [
                            {
                                "segKey": "DAC1106CCU",
                                "stops": 0,
                                "isSold": False,
                                "fillingFast": True,
                                "journeyKey": "journey-1",
                                "designator": {
                                    "origin": "DAC",
                                    "destination": "CCU",
                                    "departure": "2026-04-20T17:10:00",
                                    "arrival": "2026-04-20T17:40:00",
                                },
                                "segments": [
                                    {
                                        "identifier": {
                                            "identifier": "1106",
                                            "carrierCode": "6E",
                                            "equipmentType": "320",
                                        }
                                    }
                                ],
                                "passengerFares": [
                                    {
                                        "productClass": "R",
                                        "fareAvailabilityKey": "fare-1",
                                        "totalFareAmount": 9115,
                                        "totalPublishFare": 3878,
                                        "totalTax": 5237,
                                        "FareClass": "Economy",
                                        "baggageData": {
                                            "checkinBaggageWeight": 30,
                                            "handBaggageWeight": 7,
                                        },
                                    }
                                ],
                            }
                        ],
                    }
                ],
            },
            "errors": None,
        }

        requester.session.put.return_value = _DummyResp(200, {"data": {"success": True}})
        requester.session.post.return_value = _DummyResp(200, search_body)

        out = fetch_flights(
            origin="DAC",
            destination="CCU",
            date="2026-04-20",
            cabin="Economy",
            adt=1,
            chd=0,
            inf=0,
            airline_code="6E",
        )
        self.assertTrue(out["ok"])
        self.assertEqual(1, len(out["rows"]))
        row = out["rows"][0]
        self.assertEqual("6E", row["airline"])
        self.assertEqual("1106", row["flight_number"])
        self.assertEqual("DAC", row["origin"])
        self.assertEqual("CCU", row["destination"])
        self.assertEqual("Economy", row["cabin"])
        self.assertEqual("R", row["fare_basis"])
        self.assertEqual(9115.0, row["price_total_bdt"])
        self.assertEqual("unknown", row["inventory_confidence"])
        self.assertEqual("goindigo:v1/flight/search", row["source_endpoint"])

    @patch("modules.indigo.fetch_from_sharetrip")
    @patch("modules.indigo.Requester")
    @patch.dict(os.environ, {"INDIGO_SOURCE_MODE": "auto"}, clear=False)
    def test_auto_falls_back_to_sharetrip_on_block(self, mock_requester_cls, mock_sharetrip):
        requester = MagicMock()
        requester.timeout = 30
        requester.session = MagicMock()
        mock_requester_cls.return_value = requester

        requester.session.put.return_value = _DummyResp(200, {"data": {"success": True}})
        requester.session.post.return_value = _DummyResp(403, "Access Denied")
        mock_sharetrip.return_value = {
            "ok": True,
            "raw": {"source": "sharetrip"},
            "originalResponse": {"code": "SUCCESS"},
            "rows": [
                {
                    "airline": "6E",
                    "flight_number": "6E123",
                    "origin": "DAC",
                    "destination": "CCU",
                    "departure": "2026-04-20T12:00:00",
                    "arrival": "2026-04-20T13:00:00",
                    "cabin": "Economy",
                    "fare_basis": "R",
                    "brand": "SHARETRIP_OTA",
                    "price_total_bdt": 5000.0,
                    "seat_available": None,
                    "inventory_confidence": "unknown_ota",
                    "source_endpoint": "sharetrip:v2/search",
                }
            ],
        }

        out = fetch_flights(
            origin="DAC",
            destination="CCU",
            date="2026-04-20",
            cabin="Economy",
            adt=1,
            chd=0,
            inf=0,
            airline_code="6E",
        )
        self.assertTrue(out["ok"])
        self.assertEqual("indigo_auto", (out.get("raw") or {}).get("source"))
        self.assertEqual(1, len(out["rows"]))
        self.assertEqual("6E", out["rows"][0]["airline"])


if __name__ == "__main__":
    unittest.main()
