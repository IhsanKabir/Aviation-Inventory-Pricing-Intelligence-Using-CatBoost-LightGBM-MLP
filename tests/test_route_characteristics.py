"""Tests for route characteristics module."""

import unittest
import pandas as pd

from core.route_characteristics import (
    calculate_route_distance,
    classify_route_type,
    is_hub_airport,
    add_route_characteristics,
    estimate_competition_level,
    get_route_characteristics_columns,
)


class TestRouteCharacteristics(unittest.TestCase):
    """Test route characteristics feature engineering."""

    def test_calculate_route_distance(self):
        """Test distance calculation between airports."""
        # DAC to DXB distance (approximately 2900 km)
        distance = calculate_route_distance("DAC", "DXB")
        self.assertIsNotNone(distance)
        self.assertGreater(distance, 3400)
        self.assertLess(distance, 3600)

        # DAC to CXB distance (approximately 150 km - domestic)
        distance = calculate_route_distance("DAC", "CXB")
        self.assertIsNotNone(distance)
        self.assertLess(distance, 500)

        # Unknown airport
        distance = calculate_route_distance("XXX", "YYY")
        self.assertIsNone(distance)

    def test_classify_route_type(self):
        """Test route type classification."""
        self.assertEqual(classify_route_type(300), "domestic")
        self.assertEqual(classify_route_type(1500), "regional")
        self.assertEqual(classify_route_type(3000), "international_short")
        self.assertEqual(classify_route_type(6000), "long_haul")
        self.assertEqual(classify_route_type(None), "unknown")

    def test_is_hub_airport(self):
        """Test hub airport identification."""
        # Global major hubs
        self.assertTrue(is_hub_airport("DXB", "global_major"))
        self.assertTrue(is_hub_airport("SIN", "global_major"))

        # Middle East hubs
        self.assertTrue(is_hub_airport("DOH", "middle_east"))
        self.assertTrue(is_hub_airport("AUH", "middle_east"))

        # Non-hub
        self.assertFalse(is_hub_airport("CXB", "global_major"))

    def test_add_route_characteristics(self):
        """Test adding route characteristics to dataframe."""
        df = pd.DataFrame({
            "airline": ["BS", "BG", "BS"],
            "origin": ["DAC", "DAC", "DAC"],
            "destination": ["DXB", "SIN", "CXB"],
        })

        result = add_route_characteristics(df)

        # Check columns are added
        expected_cols = [
            "route_distance_km",
            "route_type",
            "route_type_code",
            "origin_is_hub",
            "destination_is_hub",
            "is_hub_spoke",
            "is_hub_to_hub",
        ]
        for col in expected_cols:
            self.assertIn(col, result.columns)

        # Check distance calculations
        self.assertIsNotNone(result.loc[0, "route_distance_km"])
        self.assertGreater(result.loc[0, "route_distance_km"], 2500)  # DAC-DXB

        # Check hub indicators
        self.assertEqual(result.loc[0, "destination_is_hub"], 1)  # DXB is hub
        self.assertEqual(result.loc[0, "is_hub_spoke"], 1)  # Has hub endpoint

        # Check domestic route
        self.assertEqual(result.loc[2, "is_bangladesh_domestic"], 1)  # DAC-CXB

    def test_estimate_competition_level(self):
        """Test competition level estimation."""
        df = pd.DataFrame({
            "airline": ["BS", "BG", "BS", "TG", "AI"],
            "origin": ["DAC", "DAC", "DAC", "DAC", "DAC"],
            "destination": ["DXB", "DXB", "SIN", "SIN", "SIN"],
        })

        result = estimate_competition_level(df)

        # Check columns are added
        self.assertIn("route_airline_count", result.columns)
        self.assertIn("competition_level", result.columns)
        self.assertIn("competition_level_code", result.columns)

        # DAC-DXB has 2 airlines (BS, BG) = duopoly
        dxb_routes = result[result["destination"] == "DXB"]
        self.assertEqual(dxb_routes["route_airline_count"].iloc[0], 2)
        self.assertEqual(dxb_routes["competition_level"].iloc[0], "duopoly")

        # DAC-SIN has 3 airlines = competitive
        sin_routes = result[result["destination"] == "SIN"]
        self.assertEqual(sin_routes["route_airline_count"].iloc[0], 3)
        self.assertEqual(sin_routes["competition_level"].iloc[0], "competitive")

    def test_route_type_encoding(self):
        """Test route type numeric encoding."""
        df = pd.DataFrame({
            "airline": ["BS"] * 4,
            "origin": ["DAC"] * 4,
            "destination": ["CXB", "DEL", "DXB", "IST"],  # domestic, regional, int_short, long_haul
        })

        result = add_route_characteristics(df)

        # Check encodings are consistent
        domestic_code = result[result["route_type"] == "domestic"]["route_type_code"].iloc[0]
        regional_code = result[result["route_type"] == "regional"]["route_type_code"].iloc[0]

        self.assertEqual(domestic_code, 1)
        self.assertGreater(regional_code, domestic_code)

    def test_get_route_characteristics_columns(self):
        """Test getting feature column names."""
        cols = get_route_characteristics_columns()

        self.assertIsInstance(cols, list)
        self.assertGreater(len(cols), 0)
        self.assertIn("route_distance_km", cols)
        self.assertIn("is_hub_spoke", cols)


if __name__ == "__main__":
    unittest.main()
