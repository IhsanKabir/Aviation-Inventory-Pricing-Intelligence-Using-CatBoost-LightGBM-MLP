import unittest
from unittest.mock import patch

import modules.fleet_mapping as fleet_mapping
from modules.fleet_mapping import _lookup_capacity, _parse_bg_capacity, _parse_vq_capacity


class FleetMappingTests(unittest.TestCase):
    def test_parse_vq_capacity(self):
        html = "<p>The ATR 72-500, is an airworthy 72 - seater turboprop aircraft.</p>"
        mapping = _parse_vq_capacity(html)
        self.assertEqual(72, mapping.get("ATR725"))
        self.assertEqual(72, mapping.get("ATR72-500"))

    def test_parse_bg_capacity_and_aliases(self):
        html = """
        <table>
          <tr><td>Boeing 787-8</td><td>4</td><td>Business:24</td><td>271</td></tr>
          <tr><td>Boeing 737-800</td><td>4</td><td>Business:12</td><td>162</td></tr>
          <tr><td>Dash 8-400</td><td>5</td><td>Economy:74</td><td>74</td></tr>
        </table>
        """
        mapping = _parse_bg_capacity(html)
        self.assertEqual(271, mapping.get("BOEING 787-8"))
        self.assertEqual(271, mapping.get("788"))
        self.assertEqual(162, mapping.get("738"))
        self.assertEqual(74, mapping.get("Q400"))

    def test_lookup_capacity_flexible_match(self):
        mapping = {
            "BOEING 787-9": 298,
            "789": 298,
            "DH8": 74,
        }
        self.assertEqual(298, _lookup_capacity(mapping, "Boeing 787-9 Dreamliner", None))
        self.assertEqual(74, _lookup_capacity(mapping, None, "DH8"))

    @patch("modules.fleet_mapping._write_cache")
    @patch("modules.fleet_mapping._download_html", return_value=None)
    @patch("modules.fleet_mapping._read_cache", return_value={})
    @patch(
        "modules.fleet_mapping._load_config",
        return_value={
            "refresh_hours": 24,
            "failure_retry_minutes": 60,
            "sources": {
                "VQ": "https://www.flynovoair.com/about/fleet",
                "BG": "https://www.biman-airlines.com/fleet",
            },
        },
    )
    def test_airline_scoped_refresh_only_fetches_target_airline(
        self,
        _mock_cfg,
        _mock_cache,
        mock_download,
        _mock_write,
    ):
        fleet_mapping.get_fleet_capacity_map(force_refresh=True, airlines=["VQ"])
        self.assertEqual(1, mock_download.call_count)
        self.assertIn("flynovoair.com/about/fleet", mock_download.call_args[0][0])


if __name__ == "__main__":
    unittest.main()
