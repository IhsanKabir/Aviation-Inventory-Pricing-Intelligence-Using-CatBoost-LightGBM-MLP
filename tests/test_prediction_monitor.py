"""
Unit tests for prediction monitoring.
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from core.prediction_monitor import (
    PredictionMonitor,
    monitor_prediction_drift
)


class TestPredictionMonitor(unittest.TestCase):
    """Test prediction monitoring functionality."""

    def setUp(self):
        """Set up test monitor."""
        self.monitor = PredictionMonitor(baseline_window_days=7, alert_threshold=0.20)

    def test_log_prediction(self):
        """Test logging predictions."""
        self.monitor.log_prediction(
            route="DAC-DXB",
            target="price_events",
            predicted_value=5.0,
            actual_value=5.5
        )

        self.assertEqual(len(self.monitor.metrics_history), 1)
        record = self.monitor.metrics_history[0]
        self.assertEqual(record['route'], "DAC-DXB")
        self.assertEqual(record['target'], "price_events")
        self.assertEqual(record['error'], 0.5)

    def test_calculate_metrics(self):
        """Test metrics calculation."""
        # Log several predictions
        routes = ["DAC-DXB", "DAC-DXB", "DXB-KUL"]
        targets = ["price_events", "price_events", "price_events"]
        predicted = [5.0, 6.0, 3.0]
        actual = [5.5, 5.8, 3.5]

        for r, t, p, a in zip(routes, targets, predicted, actual):
            self.monitor.log_prediction(r, t, p, a)

        # Calculate overall metrics
        metrics = self.monitor.calculate_metrics()

        self.assertEqual(metrics['count'], 3)
        self.assertAlmostEqual(metrics['mae'], np.mean([0.5, 0.2, 0.5]), places=2)

    def test_calculate_metrics_by_route(self):
        """Test metrics calculation filtered by route."""
        # Log predictions for multiple routes
        self.monitor.log_prediction("DAC-DXB", "price_events", 5.0, 5.5)
        self.monitor.log_prediction("DAC-DXB", "price_events", 6.0, 5.8)
        self.monitor.log_prediction("DXB-KUL", "price_events", 3.0, 3.5)

        # Calculate metrics for specific route
        metrics = self.monitor.calculate_metrics(route="DAC-DXB")

        self.assertEqual(metrics['count'], 2)
        self.assertAlmostEqual(metrics['mae'], np.mean([0.5, 0.2]), places=2)

    def test_update_baseline(self):
        """Test baseline calculation."""
        # Log historical predictions
        base_date = datetime.now() - timedelta(days=10)
        for i in range(5):
            self.monitor.log_prediction(
                route="DAC-DXB",
                target="price_events",
                predicted_value=5.0 + i * 0.1,
                actual_value=5.0 + i * 0.1 + 0.2,
                timestamp=base_date + timedelta(days=i)
            )

        # Update baseline
        self.monitor.update_baseline("DAC-DXB", "price_events")

        # Check baseline was stored
        key = "DAC-DXB_price_events"
        self.assertIn(key, self.monitor.baselines)
        self.assertIsNotNone(self.monitor.baselines[key]['mae'])

    def test_check_for_degradation_no_degradation(self):
        """Test degradation check when performance is stable."""
        # Log baseline predictions (MAE ~0.2)
        base_date = datetime.now() - timedelta(days=15)
        for i in range(10):
            self.monitor.log_prediction(
                route="DAC-DXB",
                target="price_events",
                predicted_value=5.0,
                actual_value=5.2,
                timestamp=base_date + timedelta(days=i)
            )

        # Log recent predictions (MAE still ~0.2)
        recent_date = datetime.now() - timedelta(days=3)
        for i in range(5):
            self.monitor.log_prediction(
                route="DAC-DXB",
                target="price_events",
                predicted_value=5.0,
                actual_value=5.2,
                timestamp=recent_date + timedelta(days=i)
            )

        # Check degradation
        result = self.monitor.check_for_degradation("DAC-DXB", "price_events", window_days=7)

        self.assertFalse(result['degraded'])

    def test_check_for_degradation_with_degradation(self):
        """Test degradation detection when performance worsens."""
        # Log baseline predictions (MAE ~0.2)
        base_date = datetime.now() - timedelta(days=15)
        for i in range(10):
            self.monitor.log_prediction(
                route="DAC-DXB",
                target="price_events",
                predicted_value=5.0,
                actual_value=5.2,
                timestamp=base_date + timedelta(days=i)
            )

        # Log recent predictions with worse accuracy (MAE ~0.5, 2.5x worse)
        recent_date = datetime.now() - timedelta(days=3)
        for i in range(5):
            self.monitor.log_prediction(
                route="DAC-DXB",
                target="price_events",
                predicted_value=5.0,
                actual_value=5.5,
                timestamp=recent_date + timedelta(days=i)
            )

        # Check degradation (threshold 0.20 = 20%)
        result = self.monitor.check_for_degradation("DAC-DXB", "price_events", window_days=7)

        self.assertTrue(result['degraded'])
        self.assertGreater(result['degradation_pct'], 0.20)

    def test_get_summary_report_by_route(self):
        """Test summary report generation grouped by route."""
        # Log predictions for multiple routes
        self.monitor.log_prediction("DAC-DXB", "price_events", 5.0, 5.5)
        self.monitor.log_prediction("DAC-DXB", "price_events", 6.0, 5.8)
        self.monitor.log_prediction("DXB-KUL", "price_events", 3.0, 3.5)
        self.monitor.log_prediction("DXB-KUL", "price_events", 3.2, 3.3)

        # Generate report
        report = self.monitor.get_summary_report(group_by='route')

        self.assertEqual(len(report), 2)
        self.assertIn('mae', report.columns)
        self.assertIn('rmse', report.columns)
        self.assertIn('prediction_count', report.columns)

    def test_get_summary_report_by_target(self):
        """Test summary report generation grouped by target."""
        # Log predictions for multiple targets
        self.monitor.log_prediction("DAC-DXB", "price_events", 5.0, 5.5)
        self.monitor.log_prediction("DAC-DXB", "capacity", 150.0, 155.0)
        self.monitor.log_prediction("DXB-KUL", "price_events", 3.0, 3.5)

        # Generate report
        report = self.monitor.get_summary_report(group_by='target')

        self.assertEqual(len(report), 2)
        self.assertIn('price_events', report['target'].values)
        self.assertIn('capacity', report['target'].values)

    def test_directional_accuracy(self):
        """Test directional accuracy calculation."""
        # Log predictions with changing actuals
        self.monitor.log_prediction("DAC-DXB", "price_events", 5.0, 5.0)  # baseline
        self.monitor.log_prediction("DAC-DXB", "price_events", 5.5, 5.5)  # correct up
        self.monitor.log_prediction("DAC-DXB", "price_events", 5.8, 5.2)  # wrong direction
        self.monitor.log_prediction("DAC-DXB", "price_events", 5.0, 4.8)  # correct down

        metrics = self.monitor.calculate_metrics(route="DAC-DXB")

        # Should have 3 directional predictions (excluding first baseline)
        self.assertIsNotNone(metrics['directional_accuracy'])


class TestMonitorPredictionDrift(unittest.TestCase):
    """Test prediction drift detection."""

    def test_no_drift(self):
        """Test when no significant drift detected."""
        historical = pd.DataFrame({'mae': [0.5, 0.6, 0.55]})
        recent = pd.DataFrame({'mae': [0.52, 0.58, 0.56]})

        result = monitor_prediction_drift(historical, recent, metric_col='mae', threshold=0.20)

        self.assertFalse(result['drifted'])

    def test_drift_detected(self):
        """Test when significant drift detected."""
        historical = pd.DataFrame({'mae': [0.5, 0.6, 0.55]})
        recent = pd.DataFrame({'mae': [0.8, 0.9, 0.85]})  # ~50% worse

        result = monitor_prediction_drift(historical, recent, metric_col='mae', threshold=0.20)

        self.assertTrue(result['drifted'])
        self.assertGreater(result['drift_pct'], 0.20)


if __name__ == "__main__":
    unittest.main()
