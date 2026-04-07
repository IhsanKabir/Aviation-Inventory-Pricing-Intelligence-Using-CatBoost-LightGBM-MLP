import os
import unittest
from unittest.mock import MagicMock, patch

from modules.bdfare import fetch_flights_for_airline


class _DummyResp:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body
        self.text = str(body)

    def json(self):
        return self._body


class BdfareConnectorTests(unittest.TestCase):
    @patch("modules.bdfare.Requester")
    @patch.dict(os.environ, {"BDFARE_MAX_POLLS": "2", "BDFARE_POLL_SLEEP_SEC": "0"}, clear=False)
    def test_normalizes_and_filters_airline_rows(self, mock_requester_cls):
        requester = MagicMock()
        requester.timeout = 30
        requester.session = MagicMock()
        mock_requester_cls.return_value = requester

        airsearch_body = {
            "requestId": "rid-123",
            "flightInfos": [],
            "resultCompleted": False,
            "error": None,
        }
        get_body = {
            "requestId": "rid-123",
            "resultCompleted": True,
            "error": None,
            "flightInfos": [
                {
                    "flightSummary": [
                        {
                            "airlineName": "US-Bangla Airlines",
                            "airlineFlightNumber": "BS 201",
                            "departureDate": "31 Mar, Tue",
                            "departureTime": "10:00",
                            "departureAirportCode": "DAC",
                            "arrivalDate": "31 Mar, Tue",
                            "arrivalTime": "10:30",
                            "arrivalAirportCode": "CCU",
                            "numberOfAdditionalDaysTravel": 0,
                            "journeyDuration": "1h 0m",
                        }
                    ],
                    "grossAmount": "BDT 10158",
                    "netAmount": "BDT 9828",
                    "agentAmount": "BDT 9828",
                    "customerNetAmount": "BDT 10189",
                    "refundable": True,
                    "airlineCode": "BS",
                    "stopKey": ["NS"],
                    "duration": 60,
                    "amount": 9828,
                    "currency": "BDT",
                    "itineraryId": "itn-bs-1",
                    "itineraryType": "Publish",
                    "productClass": None,
                },
                {
                    "flightSummary": [
                        {
                            "airlineName": "Biman Bangladesh Airlines",
                            "airlineFlightNumber": "BG 395",
                            "departureDate": "31 Mar, Tue",
                            "departureTime": "17:15",
                            "departureAirportCode": "DAC",
                            "arrivalDate": "31 Mar, Tue",
                            "arrivalTime": "17:45",
                            "arrivalAirportCode": "CCU",
                            "numberOfAdditionalDaysTravel": 0,
                            "journeyDuration": "1h 0m",
                        }
                    ],
                    "grossAmount": "BDT 9915",
                    "netAmount": "BDT 9605",
                    "customerNetAmount": "BDT 9945",
                    "refundable": True,
                    "airlineCode": "BG",
                    "stopKey": ["NS"],
                    "duration": 60,
                    "amount": 9605,
                    "currency": "BDT",
                    "itineraryId": "itn-bg-1",
                    "itineraryType": "Publish",
                },
            ],
        }

        def _post_side_effect(url, json_payload=None, headers=None, timeout=None, **kwargs):  # noqa: ARG001
            if url.endswith("/AirSearch"):
                return _DummyResp(200, airsearch_body)
            if "/GetAirSearch?requestId=rid-123" in url:
                return _DummyResp(200, get_body)
            if "/RefreshAirSearch?requestId=rid-123" in url:
                return _DummyResp(200, {"requestId": "rid-123", "resultCompleted": True})
            return _DummyResp(500, {"error": "unexpected url"})

        requester.post_raw.side_effect = _post_side_effect

        out = fetch_flights_for_airline(
            airline_code="BS",
            origin="DAC",
            destination="CCU",
            date="2026-03-31",
            cabin="Economy",
            adt=1,
            chd=0,
            inf=0,
        )
        self.assertTrue(out["ok"])
        self.assertEqual(1, len(out["rows"]))
        row = out["rows"][0]
        self.assertEqual("BS", row["airline"])
        self.assertEqual("BS 201", row["flight_number"])
        self.assertEqual("DAC", row["origin"])
        self.assertEqual("CCU", row["destination"])
        self.assertEqual(10189.0, row["price_total_bdt"])
        self.assertEqual(10158.0, row["ota_gross_fare"])
        self.assertEqual("bdfare:v2/Search/GetAirSearch", row["source_endpoint"])

    @patch("modules.bdfare.Requester")
    @patch.dict(os.environ, {"BDFARE_MAX_POLLS": "2", "BDFARE_POLL_SLEEP_SEC": "0"}, clear=False)
    def test_populates_via_airports_from_layover_list(self, mock_requester_cls):
        requester = MagicMock()
        requester.timeout = 30
        requester.session = MagicMock()
        mock_requester_cls.return_value = requester

        airsearch_body = {"requestId": "rid-via", "flightInfos": [], "resultCompleted": False, "error": None}
        get_body = {
            "requestId": "rid-via",
            "resultCompleted": True,
            "error": None,
            "flightInfos": [
                {
                    "flightSummary": [
                        {
                            "airlineName": "US-Bangla Airlines",
                            "airlineFlightNumber": "BS 341",
                            "departureDate": "31 Mar, Tue",
                            "departureTime": "10:00",
                            "departureAirportCode": "DAC",
                            "arrivalDate": "31 Mar, Tue",
                            "arrivalTime": "18:30",
                            "arrivalAirportCode": "DXB",
                            "numberOfAdditionalDaysTravel": 0,
                            "journeyDuration": "9h 30m",
                        }
                    ],
                    "grossAmount": "BDT 30158",
                    "netAmount": "BDT 28228",
                    "agentAmount": "BDT 28228",
                    "customerNetAmount": "BDT 30189",
                    "refundable": True,
                    "airlineCode": "BS",
                    "stopKey": ["1S"],
                    "layoverAirports": ["AUH"],
                    "duration": 570,
                    "amount": 28228,
                    "currency": "BDT",
                    "itineraryId": "itn-bs-via",
                    "itineraryType": "Publish",
                }
            ],
        }

        def _post_side_effect(url, json_payload=None, headers=None, timeout=None, **kwargs):  # noqa: ARG001
            if url.endswith("/AirSearch"):
                return _DummyResp(200, airsearch_body)
            if "/GetAirSearch?requestId=rid-via" in url:
                return _DummyResp(200, get_body)
            if "/RefreshAirSearch?requestId=rid-via" in url:
                return _DummyResp(200, {"requestId": "rid-via", "resultCompleted": True})
            return _DummyResp(500, {"error": "unexpected url"})

        requester.post_raw.side_effect = _post_side_effect

        out = fetch_flights_for_airline(
            airline_code="BS",
            origin="DAC",
            destination="DXB",
            date="2026-03-31",
            cabin="Economy",
            adt=1,
            chd=0,
            inf=0,
        )

        self.assertTrue(out["ok"])
        self.assertEqual("AUH", out["rows"][0]["via_airports"])


if __name__ == "__main__":
    unittest.main()
