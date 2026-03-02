import json
import os
import unittest
from unittest.mock import MagicMock, patch

from modules.amybd import fetch_flights_for_airline


class _DummyResp:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body
        self.text = str(body)

    def json(self):
        return self._body


class AmybdConnectorTests(unittest.TestCase):
    @patch("modules.amybd.Requester")
    @patch.dict(os.environ, {"AMYBD_TOKEN": "", "AMYBD_DISABLE_DEFAULT_TOKEN": "1"}, clear=False)
    def test_normalizes_bs_and_2a_rows(self, mock_requester_cls):
        requester = MagicMock()
        requester.timeout = 30
        requester.session = MagicMock()
        mock_requester_cls.return_value = requester

        body = {
            "success": True,
            "message": "",
            "SearchID": 128465712,
            "Trips": [
                {
                    "stAirline": "USBANGLA",
                    "stAirCode": "BS",
                    "fNo": "BS 343",
                    "fFrom": "DAC",
                    "fDest": "CGP",
                    "fDTime": "2026-03-27T06:15:00",
                    "fATime": "2026-03-27T07:10:00",
                    "fDursec": 55,
                    "fModel": "Boeing 737-800",
                    "fBag": "Baggage: 20 kg",
                    "fClsNam": "E",
                    "fCabin": "Y",
                    "fFare": 4749,
                    "fTBFare": 3524,
                    "fSeat": "9",
                    "fRefund": "",
                    "fSoft": "x-soft",
                    "fAMYid": 1772375634308,
                    "search_id": "6b6040e8-999e-4523-b7df-7efd3e4183fc",
                    "csource": "BD",
                    "fLegs": [
                        {
                            "DTime": "2026-03-27T06:15:00",
                            "ATime": "2026-03-27T07:10:00",
                            "xFrom": "DAC",
                            "xDest": "CGP",
                            "xACode": "BS",
                            "xFlight": "343",
                            "xClass": "E",
                            "xDur": 55,
                        }
                    ],
                },
                {
                    "stAirline": "AIR ASTRA",
                    "stAirCode": "2A",
                    "fNo": "2A 411",
                    "fFrom": "DAC",
                    "fDest": "CGP",
                    "fDTime": "2026-03-27T07:45:00",
                    "fATime": "2026-03-27T08:40:00",
                    "fDursec": 55,
                    "fModel": "ATR 72",
                    "fBag": "Baggage: 20 kg",
                    "fClsNam": "E",
                    "fCabin": "Y",
                    "fFare": 4749,
                    "fTBFare": 3524,
                    "fSeat": "3",
                    "fRefund": None,
                    "fSoft": "x-soft-2a",
                    "fAMYid": 1772375634330,
                    "search_id": "64f7e354-8ebc-4a61-bba7-055477b5607d",
                    "csource": "BD",
                    "fLegs": [
                        {
                            "DTime": "2026-03-27T07:45:00",
                            "ATime": "2026-03-27T08:40:00",
                            "xFrom": "DAC",
                            "xDest": "CGP",
                            "xACode": "2A",
                            "xFlight": "411",
                            "xClass": "E",
                            "xDur": 55,
                        }
                    ],
                },
            ],
        }

        def _post_side_effect(url, data=None, headers=None, timeout=None):  # noqa: ARG001
            return _DummyResp(200, body)

        requester.session.post.side_effect = _post_side_effect

        out_bs = fetch_flights_for_airline(
            airline_code="BS",
            origin="DAC",
            destination="CGP",
            date="2026-03-27",
            cabin="Economy",
            adt=1,
            chd=0,
            inf=0,
        )
        self.assertTrue(out_bs["ok"])
        self.assertEqual(1, len(out_bs["rows"]))
        row_bs = out_bs["rows"][0]
        self.assertEqual("BS", row_bs["airline"])
        self.assertEqual("343", row_bs["flight_number"])
        self.assertEqual("DAC", row_bs["origin"])
        self.assertEqual("CGP", row_bs["destination"])
        self.assertEqual("Economy", row_bs["cabin"])
        self.assertEqual("E", row_bs["fare_basis"])
        self.assertEqual(4749.0, row_bs["price_total_bdt"])
        self.assertEqual(9, row_bs["seat_available"])
        self.assertEqual("reported_ota", row_bs["inventory_confidence"])
        self.assertEqual("atapi.aspx:_FLIGHTSEARCH_", row_bs["source_endpoint"])

        out_2a = fetch_flights_for_airline(
            airline_code="2A",
            origin="DAC",
            destination="CGP",
            date="2026-03-27",
            cabin="Economy",
            adt=1,
            chd=0,
            inf=0,
        )
        self.assertTrue(out_2a["ok"])
        self.assertEqual(1, len(out_2a["rows"]))
        row_2a = out_2a["rows"][0]
        self.assertEqual("2A", row_2a["airline"])
        self.assertEqual("411", row_2a["flight_number"])
        self.assertEqual(3, row_2a["seat_available"])

    @patch("modules.amybd.Requester")
    @patch.dict(os.environ, {"AMYBD_TOKEN": "", "AMYBD_DISABLE_DEFAULT_TOKEN": "1"}, clear=False)
    def test_falls_back_to_flightsearchopen_when_primary_not_ok(self, mock_requester_cls):
        requester = MagicMock()
        requester.timeout = 30
        requester.session = MagicMock()
        mock_requester_cls.return_value = requester

        fail_body = {"success": False, "message": "fallback please", "Trips": []}
        ok_body = {
            "success": True,
            "message": "",
            "SearchID": 999,
            "Trips": [
                {
                    "stAirline": "USBANGLA",
                    "stAirCode": "BS",
                    "fNo": "BS 343",
                    "fFrom": "DAC",
                    "fDest": "CGP",
                    "fDTime": "2026-03-27T06:15:00",
                    "fATime": "2026-03-27T07:10:00",
                    "fDursec": 55,
                    "fModel": "Boeing 737-800",
                    "fBag": "Baggage: 20 kg",
                    "fClsNam": "E",
                    "fCabin": "Y",
                    "fFare": 4749,
                    "fTBFare": 3524,
                    "fSeat": "9",
                    "fRefund": "",
                    "fSoft": "x-soft",
                    "fAMYid": 1772375634308,
                    "search_id": "6b6040e8-999e-4523-b7df-7efd3e4183fc",
                    "csource": "BD",
                    "fLegs": [
                        {
                            "DTime": "2026-03-27T06:15:00",
                            "ATime": "2026-03-27T07:10:00",
                            "xFrom": "DAC",
                            "xDest": "CGP",
                            "xACode": "BS",
                            "xFlight": "343",
                            "xClass": "E",
                            "xDur": 55,
                        }
                    ],
                }
            ],
        }
        called_commands = []
        token_presence = []

        def _post_side_effect(url, data=None, headers=None, timeout=None):  # noqa: ARG001
            payload = json.loads(data)
            cmd = payload.get("CMND")
            called_commands.append(cmd)
            token_presence.append("TOKEN" in payload)
            if cmd == "_FLIGHTSEARCH_":
                return _DummyResp(200, fail_body)
            return _DummyResp(200, ok_body)

        requester.session.post.side_effect = _post_side_effect

        out = fetch_flights_for_airline(
            airline_code="BS",
            origin="DAC",
            destination="CGP",
            date="2026-03-27",
            cabin="Economy",
            adt=1,
            chd=0,
            inf=0,
        )
        self.assertTrue(out["ok"])
        self.assertEqual("_FLIGHTSEARCHOPEN_", out["raw"]["search_command_used"])
        self.assertEqual(["_FLIGHTSEARCH_", "_FLIGHTSEARCHOPEN_"], called_commands)
        self.assertEqual([False, False], token_presence)
        self.assertEqual("atapi.aspx:_FLIGHTSEARCHOPEN_", out["rows"][0]["source_endpoint"])

    @patch("modules.amybd.Requester")
    @patch.dict(os.environ, {"AMYBD_TOKEN": "", "AMYBD_DISABLE_DEFAULT_TOKEN": "1"}, clear=False)
    def test_propagates_svdid_into_raw_and_rows(self, mock_requester_cls):
        requester = MagicMock()
        requester.timeout = 30
        requester.session = MagicMock()
        mock_requester_cls.return_value = requester

        body = {
            "success": True,
            "message": "",
            "SearchID": 128466672,
            "svdid": "DACCGP15-Apr-2026100OWDOMY0",
            "Trips": [
                {
                    "stAirline": "AIR ASTRA",
                    "stAirCode": "2A",
                    "fNo": "2A 411",
                    "fFrom": "DAC",
                    "fDest": "CGP",
                    "fDTime": "2026-04-15T07:45:00",
                    "fATime": "2026-04-15T08:40:00",
                    "fDursec": 55,
                    "fModel": "ATR 72",
                    "fBag": "Baggage: 20 kg",
                    "fClsNam": "E",
                    "fCabin": "Y",
                    "fFare": 4749,
                    "fTBFare": 3524,
                    "fSeat": "9",
                    "fRefund": None,
                    "fSoft": "x-soft-2a",
                    "fAMYid": 1772375634330,
                    "search_id": "64f7e354-8ebc-4a61-bba7-055477b5607d",
                    "csource": "BD",
                    "fLegs": [
                        {
                            "DTime": "2026-04-15T07:45:00",
                            "ATime": "2026-04-15T08:40:00",
                            "xFrom": "DAC",
                            "xDest": "CGP",
                            "xACode": "2A",
                            "xFlight": "411",
                            "xClass": "E",
                            "xDur": 55,
                        }
                    ],
                }
            ],
        }

        def _post_side_effect(url, data=None, headers=None, timeout=None):  # noqa: ARG001
            return _DummyResp(200, body)

        requester.session.post.side_effect = _post_side_effect

        out = fetch_flights_for_airline(
            airline_code="2A",
            origin="DAC",
            destination="CGP",
            date="2026-04-15",
            cabin="Economy",
            adt=1,
            chd=0,
            inf=0,
        )
        self.assertTrue(out["ok"])
        self.assertEqual("DACCGP15-Apr-2026100OWDOMY0", out["raw"]["search_svdid"])
        self.assertEqual("DACCGP15-Apr-2026100OWDOMY0", out["rows"][0]["fare_search_signature"])


if __name__ == "__main__":
    unittest.main()
