"""Tests for booking curve features module."""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from core.booking_curve_features import (
    add_booking_curve_features,
    get_booking_curve_feature_columns,
    add_booking_curve_aggregates,
    identify_booking_curve_anomalies,
)


class TestBookingCurveFeatures(unittest.TestCase):
    """Test booking curve feature engineering."""

    def setUp(self):
        """Set up test data."""
        # Create test dataframe with search and departure dates
        dates = pd.date_range("2026-01-01", periods=20, freq="D")
        self.df = pd.DataFrame({
            "event_day": dates,
            "departure_day": dates + timedelta(days=30),  # 30 days advance
            "airline": ["BS"] * 20,
            "origin": ["DAC"] * 20,
            "destination": ["DXB"] * 20,
        })

    def test_add_booking_curve_features(self):
        """Test adding booking curve features."""
        result = add_booking_curve_features(self.df)

        # Check that all expected columns are added
        expected_cols = get_booking_curve_feature_columns()
        for col in expected_cols:
            self.assertIn(col, result.columns)

        # Check booking advance calculation
        self.assertTrue((result["booking_advance_days"] == 30).all())

        # Check window buckets (30 days is in 15-30 bucket)
        self.assertTrue((result["booking_window_15_30"] == 1).all())
        self.assertTrue((result["booking_window_0_7"] == 0).all())

        # Check peak booking window (30-45 days inclusive)
        self.assertTrue((result["is_peak_booking_window"] == 1).all())

    def test_booking_curve_with_varying_advance(self):
        """Test booking curve features with varying advance periods."""
        # Create data with different advance periods
        df = pd.DataFrame({
            "event_day": pd.date_range("2026-01-01", periods=5, freq="D"),
            "departure_day": [
                datetime(2026, 1, 5),   # 4 days advance
                datetime(2026, 1, 12),  # 10 days advance
                datetime(2026, 1, 25),  # 22 days advance
                datetime(2026, 2, 10),  # 37 days advance
                datetime(2026, 5, 1),   # 120 days advance
            ],
            "airline": ["BS"] * 5,
            "origin": ["DAC"] * 5,
            "destination": ["DXB"] * 5,
        })

        result = add_booking_curve_features(df)

        # Check advance days calculation
        self.assertAlmostEqual(result.loc[0, "booking_advance_days"], 4, places=0)
        self.assertAlmostEqual(result.loc[1, "booking_advance_days"], 10, places=0)
        self.assertAlmostEqual(result.loc[2, "booking_advance_days"], 22, places=0)

        # Check window classifications
        self.assertEqual(result.loc[0, "is_late_booking"], 1)  # 4 days = late
        self.assertEqual(result.loc[1, "booking_window_8_14"], 1)  # 10 days
        self.assertEqual(result.loc[3, "is_peak_booking_window"], 1)  # 37 days
        self.assertEqual(result.loc[4, "is_early_booking"], 1)  # 120 days

    def test_booking_curve_aggregates(self):
        """Test booking curve aggregate statistics."""
        df = add_booking_curve_features(self.df)
        result = add_booking_curve_aggregates(df, ["airline", "origin", "destination"])

        # Check aggregate columns are added
        self.assertIn("avg_booking_advance", result.columns)
        self.assertIn("std_booking_advance", result.columns)
        self.assertIn("pct_late_bookings", result.columns)
        self.assertIn("pct_peak_bookings", result.columns)

        # Check values
        self.assertAlmostEqual(result["avg_booking_advance"].iloc[0], 30, places=0)

    def test_booking_curve_phase(self):
        """Test booking curve phase classification."""
        df = pd.DataFrame({
            "event_day": pd.date_range("2026-01-01", periods=4, freq="D"),
            "departure_day": [
                datetime(2026, 1, 6),   # 5 days - late
                datetime(2026, 1, 16),  # 15 days - standard
                datetime(2026, 2, 15),  # 45 days - peak
                datetime(2026, 5, 1),   # 120 days - early
            ],
            "airline": ["BS"] * 4,
            "origin": ["DAC"] * 4,
            "destination": ["DXB"] * 4,
        })

        result = add_booking_curve_features(df)

        # Check phase classification
        self.assertEqual(result.loc[0, "booking_curve_phase"], 0)  # late
        self.assertEqual(result.loc[1, "booking_curve_phase"], 1)  # standard
        self.assertEqual(result.loc[2, "booking_curve_phase"], 2)  # peak
        self.assertEqual(result.loc[3, "booking_curve_phase"], 3)  # early

    def test_get_booking_curve_feature_columns(self):
        """Test getting feature column names."""
        cols = get_booking_curve_feature_columns()

        self.assertIsInstance(cols, list)
        self.assertGreater(len(cols), 0)
        self.assertIn("booking_advance_days", cols)
        self.assertIn("is_peak_booking_window", cols)


if __name__ == "__main__":
    unittest.main()
