import unittest
from unittest.mock import patch


class OtaFallbackConnectorTests(unittest.TestCase):
    def test_bs_falls_back_to_bdfare_when_sharetrip_empty(self):
        import modules.bs as bs

        sharetrip_empty = {"raw": {"source": "sharetrip"}, "originalResponse": None, "rows": [], "ok": False}
        bdfare_ok = {"raw": {"source": "bdfare"}, "originalResponse": {}, "rows": [{"airline": "BS"}], "ok": True}

        with patch.object(bs, "fetch_from_sharetrip", return_value=sharetrip_empty), patch.object(
            bs, "fetch_from_bdfare", return_value=bdfare_ok
        ):
            out = bs.fetch_flights(origin="DAC", destination="CGP", date="2026-03-27")

        self.assertTrue(out.get("ok"))
        self.assertEqual("bdfare", (out.get("raw") or {}).get("source"))
        self.assertEqual(1, len(out.get("rows") or []))

    def test_airastra_falls_back_to_bdfare_when_sharetrip_empty(self):
        import modules.airastra as airastra

        sharetrip_empty = {"raw": {"source": "sharetrip"}, "originalResponse": None, "rows": [], "ok": False}
        bdfare_ok = {"raw": {"source": "bdfare"}, "originalResponse": {}, "rows": [{"airline": "2A"}], "ok": True}

        with patch.object(airastra, "fetch_from_sharetrip", return_value=sharetrip_empty), patch.object(
            airastra, "fetch_from_bdfare", return_value=bdfare_ok
        ):
            out = airastra.fetch_flights(origin="DAC", destination="CGP", date="2026-03-27")

        self.assertTrue(out.get("ok"))
        self.assertEqual("bdfare", (out.get("raw") or {}).get("source"))
        self.assertEqual(1, len(out.get("rows") or []))


if __name__ == "__main__":
    unittest.main()
