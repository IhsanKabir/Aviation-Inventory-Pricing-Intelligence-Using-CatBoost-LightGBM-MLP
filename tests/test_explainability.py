"""Tests for SHAP explainability module."""

import unittest
import numpy as np
import pandas as pd

from core.explainability import (
    compute_shap_feature_importance,
    get_top_features,
    format_feature_importance_for_output,
    explain_prediction_change,
)


class TestExplainability(unittest.TestCase):
    """Test explainability functions."""

    def setUp(self):
        """Set up test data."""
        # Create simple test data
        np.random.seed(42)
        self.X = pd.DataFrame({
            "feature1": np.random.randn(100),
            "feature2": np.random.randn(100),
            "feature3": np.random.randn(100),
        })
        self.y = (
            2 * self.X["feature1"] +
            0.5 * self.X["feature2"] +
            np.random.randn(100) * 0.1
        )

    def test_compute_shap_without_shap_library(self):
        """Test graceful handling when SHAP is not available."""
        # This should return available=False if shap can't be imported
        # or successfully compute if shap is available
        result = compute_shap_feature_importance(None, self.X, "tree")
        self.assertIn("available", result)
        self.assertIsInstance(result["available"], bool)

    def test_get_top_features_empty(self):
        """Test getting top features with empty importance dict."""
        importance_dict = {"available": False, "features": [], "importance": []}
        result = get_top_features(importance_dict, top_n=5)
        self.assertEqual(result, [])

    def test_get_top_features_with_data(self):
        """Test getting top features with valid data."""
        importance_dict = {
            "available": True,
            "features": ["feat1", "feat2", "feat3", "feat4", "feat5"],
            "importance": [0.5, 0.3, 0.8, 0.1, 0.2],
        }
        result = get_top_features(importance_dict, top_n=3)

        # Should return top 3 features sorted by importance
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0][0], "feat3")  # Highest importance (0.8)
        self.assertEqual(result[1][0], "feat1")  # Second (0.5)
        self.assertEqual(result[2][0], "feat2")  # Third (0.3)

    def test_format_feature_importance_for_output(self):
        """Test formatting feature importance for output."""
        importance_dict = {
            "available": True,
            "features": ["feat1", "feat2", "feat3"],
            "importance": [0.5, 0.3, 0.8],
        }
        result = format_feature_importance_for_output(importance_dict, top_n=5)

        # Should have 5 feature/value pairs
        self.assertIn("shap_feature_1", result)
        self.assertIn("shap_value_1", result)
        self.assertIn("shap_feature_5", result)
        self.assertIn("shap_value_5", result)

        # Top feature should be feat3 with value 0.8
        self.assertEqual(result["shap_feature_1"], "feat3")
        self.assertAlmostEqual(result["shap_value_1"], 0.8, places=5)

        # Features 4 and 5 should be None (not enough features)
        self.assertIsNone(result["shap_feature_4"])
        self.assertIsNone(result["shap_value_4"])

    def test_explain_prediction_change(self):
        """Test explaining prediction changes."""
        current_features = {"feat1": 10, "feat2": 20, "feat3": 30}
        previous_features = {"feat1": 8, "feat2": 20, "feat3": 25}
        importance_dict = {
            "available": True,
            "features": ["feat1", "feat2", "feat3"],
            "importance": [0.8, 0.2, 0.5],
        }

        result = explain_prediction_change(
            current_features,
            previous_features,
            importance_dict,
            threshold_pct=10.0
        )

        # feat1 changed by 25% (8 to 10)
        # feat2 changed by 0%
        # feat3 changed by 20% (25 to 30)
        # Both feat1 and feat3 should be in explanations
        self.assertGreater(len(result), 0)
        feature_names = [r["feature"] for r in result]
        self.assertIn("feat1", feature_names)
        self.assertIn("feat3", feature_names)
        self.assertNotIn("feat2", feature_names)  # No change

    def test_explain_prediction_change_unavailable(self):
        """Test prediction change explanation when SHAP unavailable."""
        current_features = {"feat1": 10}
        previous_features = {"feat1": 5}
        importance_dict = {"available": False}

        result = explain_prediction_change(
            current_features,
            previous_features,
            importance_dict
        )

        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()
