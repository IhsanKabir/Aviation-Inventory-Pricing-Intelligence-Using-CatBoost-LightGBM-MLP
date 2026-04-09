import unittest
from unittest.mock import MagicMock, patch

from modules.gozayaan import fetch_flights_for_airline


class _DummyResp:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body
        self.text = str(body)

    def json(self):
        return self._body


class GozayaanConnectorTests(unittest.TestCase):
    @patch("modules.gozayaan._active_rate_limit_state", return_value=None)
    @patch("modules.gozayaan.Requester")
    def test_bs_row_normalization_and_penalties(self, mock_requester_cls, _mock_cooldown):
        requester = MagicMock()
        requester.timeout = 30
        requester.session = MagicMock()
        mock_requester_cls.return_value = requester

        search_body = {
            "status": True,
            "result": {
                "search_id": "s-123",
            },
        }
        legs_body = {
            "status": True,
            "result": {
                "status": "DONE",
                "progress": 1,
                "expected_progress": 1,
                "fares": [
                    {
                        "id": "f-1",
                        "hash": "leg-h1",
                        "leg_hashes": ["leg-h1"],
                        "hash_str": "BS|DAC-CXB-2026-04-13-BS-157-AT7",
                        "currency": "BDT",
                        "total_base_amount": 4024,
                        "total_tax_amount": 1225,
                        "total_fare_amount": 5249,
                    }
                ],
                "legs": [
                    {
                        "hash": "leg-h1",
                        "segment_hashes": ["seg-h1"],
                        "travel_time": 65,
                        "departure_date_time": "2026-04-13T07:00:00",
                        "arrival_date_time": "2026-04-13T08:05:00",
                    }
                ],
                "segments": [
                    {
                        "hash": "seg-h1",
                        "origin": "DAC",
                        "destination": "CXB",
                        "departure_date_time": "2026-04-13T07:00:00",
                        "arrival_date_time": "2026-04-13T08:05:00",
                        "flight_number": "157",
                        "marketing_carrier": "BS",
                        "operating_carrier": "BS",
                        "equipment": "AT7",
                    }
                ],
            },
        }
        leg_fares_body = {
            "status": True,
            "result": {
                "fares": [
                    {
                        "id": "f-1",
                        "hash": "leg-h1",
                        "leg_hashes": ["leg-h1"],
                        "hash_str": "BS|DAC-CXB-2026-04-13-BS-157-AT7",
                        "currency": "BDT",
                        "total_base_amount": 4024,
                        "total_tax_amount": 1225,
                        "total_fare_amount": 5249,
                        "fare_type": "PUBLIC",
                        "leg_wise_fare_rules": {
                            "leg-h1": {
                                "ADT": {
                                    "fare_basis": "EDOMO",
                                    "booking_code": "E",
                                    "fare_family": "Economy Saver",
                                    "cabin_class": "Economy",
                                    "changeable": True,
                                    "refundable": True,
                                    "baggage_policy": {
                                        "unit": "KG",
                                        "check_in_quantity": "20",
                                    },
                                }
                            }
                        },
                    }
                ],
                "policies": [
                    {
                        "time_frame": "Before 24 hours of flight departure",
                        "change_fee": 1500,
                        "cancellation_fee": 2000,
                        "currency": "BDT",
                        "changeable": True,
                        "refundable": True,
                    },
                    {
                        "time_frame": "Within 24 hours before flight departure",
                        "change_fee": 2000,
                        "cancellation_fee": 2500,
                        "currency": "BDT",
                        "changeable": True,
                        "refundable": True,
                    },
                    {
                        "time_frame": "Within 6 hours before flight departure, or no show",
                        "change_fee": 3500,
                        "cancellation_fee": 3500,
                        "currency": "BDT",
                        "changeable": True,
                        "refundable": True,
                    },
                ],
            },
        }

        def _post_side_effect(url, json=None, headers=None, timeout=None):  # noqa: A002
            if url.endswith("/flight/v4.0/search/"):
                return _DummyResp(200, search_body)
            if url.endswith("/flight/v4.0/search/legs/"):
                return _DummyResp(200, legs_body)
            if url.endswith("/flight/v4.0/search/legs/fares/"):
                return _DummyResp(200, leg_fares_body)
            return _DummyResp(500, {"status": False})

        requester.session.post.side_effect = _post_side_effect

        out = fetch_flights_for_airline(
            airline_code="BS",
            origin="DAC",
            destination="CXB",
            date="2026-04-13",
            cabin="Economy",
            adt=1,
            chd=0,
            inf=0,
        )

        self.assertTrue(out["ok"])
        self.assertEqual(1, len(out["rows"]))
        row = out["rows"][0]
        self.assertEqual("BS", row["airline"])
        self.assertEqual("157", row["flight_number"])
        self.assertEqual("DAC", row["origin"])
        self.assertEqual("CXB", row["destination"])
        self.assertEqual("EDOMO", row["fare_basis"])
        self.assertEqual("Economy Saver", row["brand"])
        self.assertEqual(5249.0, row["price_total_bdt"])
        self.assertEqual("unknown_ota", row["inventory_confidence"])
        self.assertEqual("api/flight/v4.0/search/legs/fares", row["source_endpoint"])
        self.assertEqual(1500.0, row["fare_change_fee_before_24h"])
        self.assertEqual(2000.0, row["fare_cancel_fee_before_24h"])
        self.assertEqual(3500.0, row["fare_change_fee_no_show"])
        self.assertEqual(3500.0, row["fare_cancel_fee_no_show"])

    @patch("modules.gozayaan._active_rate_limit_state", return_value=None)
    @patch("modules.gozayaan.Requester")
    def test_populates_via_airports_from_multisegment_journey(self, mock_requester_cls, _mock_cooldown):
        requester = MagicMock()
        requester.timeout = 30
        requester.session = MagicMock()
        mock_requester_cls.return_value = requester

        search_body = {"status": True, "result": {"search_id": "s-via"}}
        legs_body = {
            "status": True,
            "result": {
                "status": "DONE",
                "progress": 1,
                "expected_progress": 1,
                "fares": [
                    {
                        "id": "f-via",
                        "hash": "leg-h-via",
                        "leg_hashes": ["leg-h-via"],
                        "hash_str": "BS|DAC-DXB-2026-04-13-BS-341-AT7",
                        "currency": "BDT",
                        "total_base_amount": 20000,
                        "total_tax_amount": 5000,
                        "total_fare_amount": 25000,
                    }
                ],
                "legs": [
                    {
                        "hash": "leg-h-via",
                        "segment_hashes": ["seg-a", "seg-b"],
                        "travel_time": 600,
                        "departure_date_time": "2026-04-13T07:00:00",
                        "arrival_date_time": "2026-04-13T17:00:00",
                    }
                ],
                "segments": [
                    {
                        "hash": "seg-a",
                        "origin": "DAC",
                        "destination": "AUH",
                        "departure_date_time": "2026-04-13T07:00:00",
                        "arrival_date_time": "2026-04-13T11:00:00",
                        "flight_number": "341",
                        "marketing_carrier": "BS",
                        "operating_carrier": "BS",
                        "equipment": "AT7",
                    },
                    {
                        "hash": "seg-b",
                        "origin": "AUH",
                        "destination": "DXB",
                        "departure_date_time": "2026-04-13T13:00:00",
                        "arrival_date_time": "2026-04-13T17:00:00",
                        "flight_number": "341",
                        "marketing_carrier": "BS",
                        "operating_carrier": "BS",
                        "equipment": "AT7",
                    },
                ],
            },
        }
        leg_fares_body = {
            "status": True,
            "result": {
                "fares": [
                    {
                        "id": "f-via",
                        "hash": "leg-h-via",
                        "leg_hashes": ["leg-h-via"],
                        "hash_str": "BS|DAC-DXB-2026-04-13-BS-341-AT7",
                        "currency": "BDT",
                        "total_base_amount": 20000,
                        "total_tax_amount": 5000,
                        "total_fare_amount": 25000,
                        "fare_type": "PUBLIC",
                        "leg_wise_fare_rules": {
                            "leg-h-via": {
                                "ADT": {
                                    "fare_basis": "VIAFARE",
                                    "booking_code": "V",
                                    "fare_family": "Economy",
                                    "cabin_class": "Economy",
                                }
                            }
                        },
                    }
                ],
                "policies": [],
            },
        }

        def _post_side_effect(url, json=None, headers=None, timeout=None):  # noqa: A002
            if url.endswith("/flight/v4.0/search/"):
                return _DummyResp(200, search_body)
            if url.endswith("/flight/v4.0/search/legs/"):
                return _DummyResp(200, legs_body)
            if url.endswith("/flight/v4.0/search/legs/fares/"):
                return _DummyResp(200, leg_fares_body)
            return _DummyResp(500, {"status": False})

        requester.session.post.side_effect = _post_side_effect

        out = fetch_flights_for_airline(
            airline_code="BS",
            origin="DAC",
            destination="DXB",
            date="2026-04-13",
            cabin="Economy",
            adt=1,
            chd=0,
            inf=0,
        )

        self.assertTrue(out["ok"])
        self.assertEqual("AUH", out["rows"][0]["via_airports"])

    @patch("modules.gozayaan._active_rate_limit_state", return_value=None)
    @patch("modules.gozayaan.Requester")
    @patch("modules.gozayaan._run_refresh_command")
    @patch("modules.gozayaan._resolve_active_kong_token")
    def test_rate_limit_retry_after_token_refresh(
        self,
        mock_resolve_token,
        mock_refresh_command,
        mock_requester_cls,
        _mock_cooldown,
    ):
        requester = MagicMock()
        requester.timeout = 30
        requester.session = MagicMock()
        mock_requester_cls.return_value = requester

        mock_refresh_command.return_value = {"ok": True}

        state = {"n": 0}

        def _resolve_side_effect(min_ttl_sec=0, search_payload=None):  # noqa: ARG001
            state["n"] += 1
            if state["n"] == 1:
                return None
            return {
                "token": "eyJhbGciOiJub25lIn0.eyJleHAiOjQxMDI0NDQ4MDB9.",
                "source": "cache",
                "expires_at_utc": "2100-01-01T00:00:00+00:00",
                "ttl_sec": 9999999,
            }

        mock_resolve_token.side_effect = _resolve_side_effect

        search_rate_limited = {
            "status": False,
            "error": {"code": "420", "message": "API rate limit exceeded"},
        }
        search_ok = {"status": True, "result": {"search_id": "s-456"}}
        legs_body = {
            "status": True,
            "result": {
                "status": "DONE",
                "progress": 1,
                "expected_progress": 1,
                "fares": [
                    {
                        "id": "f-2",
                        "hash": "leg-h2",
                        "leg_hashes": ["leg-h2"],
                        "hash_str": "2A|DAC-CXB-2026-04-13-2A-103-AT7",
                        "currency": "BDT",
                        "total_base_amount": 3800,
                        "total_tax_amount": 900,
                        "total_fare_amount": 4700,
                    }
                ],
                "legs": [
                    {
                        "hash": "leg-h2",
                        "segment_hashes": ["seg-h2"],
                        "travel_time": 60,
                        "departure_date_time": "2026-04-13T08:00:00",
                        "arrival_date_time": "2026-04-13T09:00:00",
                    }
                ],
                "segments": [
                    {
                        "hash": "seg-h2",
                        "origin": "DAC",
                        "destination": "CXB",
                        "departure_date_time": "2026-04-13T08:00:00",
                        "arrival_date_time": "2026-04-13T09:00:00",
                        "flight_number": "103",
                        "marketing_carrier": "2A",
                        "operating_carrier": "2A",
                        "equipment": "AT7",
                    }
                ],
            },
        }
        leg_fares_body = {
            "status": True,
            "result": {
                "fares": [
                    {
                        "id": "f-2",
                        "hash": "leg-h2",
                        "leg_hashes": ["leg-h2"],
                        "hash_str": "2A|DAC-CXB-2026-04-13-2A-103-AT7",
                        "currency": "BDT",
                        "total_base_amount": 3800,
                        "total_tax_amount": 900,
                        "total_fare_amount": 4700,
                        "fare_type": "PUBLIC",
                        "leg_wise_fare_rules": {
                            "leg-h2": {
                                "ADT": {
                                    "fare_basis": "YOTATEST",
                                    "booking_code": "Y",
                                    "fare_family": "Economy",
                                    "cabin_class": "Economy",
                                    "changeable": True,
                                    "refundable": False,
                                }
                            }
                        },
                    }
                ],
                "policies": [],
            },
        }

        counter = {"search_calls": 0}

        def _post_side_effect(url, json=None, headers=None, timeout=None):  # noqa: A002
            if url.endswith("/flight/v4.0/search/"):
                counter["search_calls"] += 1
                if counter["search_calls"] == 1:
                    return _DummyResp(200, search_rate_limited)
                return _DummyResp(200, search_ok)
            if url.endswith("/flight/v4.0/search/legs/"):
                return _DummyResp(200, legs_body)
            if url.endswith("/flight/v4.0/search/legs/fares/"):
                return _DummyResp(200, leg_fares_body)
            return _DummyResp(500, {"status": False})

        requester.session.post.side_effect = _post_side_effect

        out = fetch_flights_for_airline(
            airline_code="2A",
            origin="DAC",
            destination="CXB",
            date="2026-04-13",
            cabin="Economy",
            adt=1,
            chd=0,
            inf=0,
        )

        self.assertTrue(out["ok"])
        self.assertEqual(2, counter["search_calls"])
        self.assertTrue(out["raw"].get("search_retry_attempted"))
        self.assertIn("token_refresh", out["raw"])
        self.assertEqual(1, len(out["rows"]))
        self.assertEqual("2A", out["rows"][0]["airline"])


if __name__ == "__main__":
    unittest.main()
