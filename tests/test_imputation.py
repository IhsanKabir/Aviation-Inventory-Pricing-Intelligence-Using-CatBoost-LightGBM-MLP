"""
Unit tests for robust imputation pipeline.
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from core.imputation import (
    RobustImputer,
    impute_with_similar_routes,
    get_imputation_quality_metrics
)


class TestRobustImputer(unittest.TestCase):
    """Test robust imputation functionality."""

    def setUp(self):
        """Set up test data with missing values."""
        # Create sample data with missing values
        dates = pd.date_range("2026-01-01", periods=100, freq="D")
        np.random.seed(42)

        self.df = pd.DataFrame({
            "date": dates,
            "price": np.random.normal(1000, 100, 100),
            "capacity": np.random.randint(100, 200, 100),
            "events": np.random.randint(0, 10, 100),
            "airline": np.random.choice(["BS", "BG", "US"], 100),
            "origin": np.random.choice(["DAC", "DXB", "KUL"], 100),
            "destination": np.random.choice(["DXB", "KUL", "SIN"], 100),
        })

        # Introduce missing values (20% missing)
        mask = np.random.rand(100, 3) < 0.2
        self.df.loc[mask[:, 0], "price"] = np.nan
        self.df.loc[mask[:, 1], "capacity"] = np.nan
        self.df.loc[mask[:, 2], "events"] = np.nan

        # Introduce categorical missing (10% missing)
        mask_cat = np.random.rand(100) < 0.1
        self.df.loc[mask_cat, "airline"] = np.nan

    def test_knn_imputation_numeric(self):
        """Test KNN imputation for numeric features."""
        imputer = RobustImputer(n_neighbors=5)
        result = imputer.fit_transform(
            self.df,
            numeric_cols=["price", "capacity", "events"],
            categorical_cols=["airline", "origin", "destination"]
        )

        # Check that no NaN values remain in numeric columns
        self.assertEqual(result["price"].isna().sum(), 0)
        self.assertEqual(result["capacity"].isna().sum(), 0)
        self.assertEqual(result["events"].isna().sum(), 0)

    def test_categorical_imputation(self):
        """Test mode imputation for categorical features."""
        imputer = RobustImputer()
        result = imputer.fit_transform(
            self.df,
            numeric_cols=["price", "capacity", "events"],
            categorical_cols=["airline", "origin", "destination"]
        )

        # Check that no NaN values remain in categorical columns
        self.assertEqual(result["airline"].isna().sum(), 0)

        # Check that mode was used
        self.assertIn("airline", imputer.categorical_modes)

    def test_timeseries_imputation(self):
        """Test forward/backward fill for time series."""
        # Create time series with gaps
        ts_df = pd.DataFrame({
            "date": pd.date_range("2026-01-01", periods=10, freq="D"),
            "value": [1, 2, np.nan, 4, np.nan, np.nan, 7, 8, 9, 10]
        })

        imputer = RobustImputer()
        result = imputer.fit_transform(
            ts_df,
            numeric_cols=[],
            categorical_cols=[],
            timeseries_cols=["value"]
        )

        # Check forward fill worked
        self.assertEqual(result.loc[2, "value"], 2)  # Forward filled from 2
        self.assertEqual(result.loc[4, "value"], 4)  # Forward filled from 4

    def test_imputation_stats_tracking(self):
        """Test that imputation statistics are tracked."""
        imputer = RobustImputer()
        result = imputer.fit_transform(
            self.df,
            numeric_cols=["price", "capacity", "events"],
            categorical_cols=["airline"]
        )

        # Check that stats were recorded
        self.assertIn("columns", imputer.imputation_stats)
        self.assertIn("price", imputer.imputation_stats["columns"])

        # Check imputation report
        report = imputer.get_imputation_report()
        self.assertGreater(len(report), 0)
        self.assertIn("imputation_rate", report.columns)

    def test_high_imputation_warning(self):
        """Test warning for high imputation rates."""
        # Create data with >20% missing
        df_high_missing = self.df.copy()
        mask = np.random.rand(100) < 0.3
        df_high_missing.loc[mask, "price"] = np.nan

        imputer = RobustImputer(warn_threshold=0.20)

        # Should trigger warning (we're not checking the warning itself in test)
        result = imputer.fit_transform(
            df_high_missing,
            numeric_cols=["price"],
            categorical_cols=[]
        )

        # But should still impute
        self.assertEqual(result["price"].isna().sum(), 0)


class TestSimilarRoutesImputation(unittest.TestCase):
    """Test route-specific imputation."""

    def test_impute_with_similar_routes(self):
        """Test imputation using similar routes."""
        # Create route data with missing values
        df = pd.DataFrame({
            "origin": ["DAC", "DAC", "DAC", "DXB", "DXB", "DXB"] * 5,
            "destination": ["DXB", "DXB", "DXB", "KUL", "KUL", "KUL"] * 5,
            "price": [1000, 1050, np.nan, 2000, np.nan, 2100] * 5,
            "capacity": [150, 160, 155, 200, 210, np.nan] * 5,
        })

        result = impute_with_similar_routes(
            df,
            route_cols=["origin", "destination"],
            numeric_cols=["price", "capacity"],
            n_neighbors=2
        )

        # Check that missing values were imputed
        self.assertEqual(result["price"].isna().sum(), 0)
        self.assertEqual(result["capacity"].isna().sum(), 0)


class TestImputationQualityMetrics(unittest.TestCase):
    """Test imputation quality metrics."""

    def test_quality_metrics(self):
        """Test calculation of imputation quality metrics."""
        # Create before and after dataframes
        df_before = pd.DataFrame({
            "a": [1, 2, np.nan, 4],
            "b": [10, np.nan, 30, np.nan],
            "c": [100, 200, 300, 400]
        })

        df_after = pd.DataFrame({
            "a": [1, 2, 3, 4],
            "b": [10, 20, 30, 40],
            "c": [100, 200, 300, 400]
        })

        metrics = get_imputation_quality_metrics(df_before, df_after)

        # Check metrics
        self.assertEqual(metrics["overall_completeness"], 1.0)
        self.assertEqual(metrics["columns_fully_imputed"], 3)
        self.assertEqual(metrics["total_imputed_values"], 3)
        self.assertEqual(metrics["imputation_success_rate"], 1.0)


if __name__ == "__main__":
    unittest.main()
