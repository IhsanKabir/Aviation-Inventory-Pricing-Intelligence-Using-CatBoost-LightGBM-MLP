"""
Real-time prediction performance monitoring.

Tracks prediction accuracy (MAE/RMSE) by route/target/day and alerts on degradation.
Enables proactive issue detection before users notice problems.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import json
import warnings


class PredictionMonitor:
    """
    Monitors prediction performance in real-time.

    Tracks metrics:
    - MAE (Mean Absolute Error)
    - RMSE (Root Mean Squared Error)
    - MAPE (Mean Absolute Percentage Error)
    - Directional accuracy (% predictions with correct direction)
    """

    def __init__(self, baseline_window_days: int = 30, alert_threshold: float = 0.20):
        """
        Initialize prediction monitor.

        Args:
            baseline_window_days: Days of history to use for baseline calculation
            alert_threshold: Alert if MAE degrades by more than this fraction (0.20 = 20%)
        """
        self.baseline_window_days = baseline_window_days
        self.alert_threshold = alert_threshold
        self.metrics_history = []
        self.baselines = {}

    def log_prediction(self,
                      route: str,
                      target: str,
                      predicted_value: float,
                      actual_value: Optional[float] = None,
                      timestamp: Optional[datetime] = None):
        """
        Log a prediction for monitoring.

        Args:
            route: Route identifier (e.g., "DAC-DXB")
            target: Target variable (e.g., "price_events")
            predicted_value: Predicted value
            actual_value: Actual value (if available)
            timestamp: Prediction timestamp (defaults to now)
        """
        if timestamp is None:
            timestamp = datetime.now()

        record = {
            'timestamp': timestamp,
            'route': route,
            'target': target,
            'predicted_value': predicted_value,
            'actual_value': actual_value,
            'error': None if actual_value is None else abs(predicted_value - actual_value),
            'directional_correct': None
        }

        # Calculate directional accuracy if we have previous actual value
        if actual_value is not None and len(self.metrics_history) > 0:
            prev_records = [r for r in self.metrics_history
                           if r['route'] == route and r['target'] == target and r['actual_value'] is not None]
            if prev_records:
                prev_actual = prev_records[-1]['actual_value']
                predicted_direction = np.sign(predicted_value - prev_actual)
                actual_direction = np.sign(actual_value - prev_actual)
                record['directional_correct'] = (predicted_direction == actual_direction)

        self.metrics_history.append(record)

    def calculate_metrics(self,
                         route: Optional[str] = None,
                         target: Optional[str] = None,
                         start_date: Optional[datetime] = None,
                         end_date: Optional[datetime] = None) -> Dict[str, float]:
        """
        Calculate performance metrics for specified filters.

        Args:
            route: Filter by route (None = all routes)
            target: Filter by target (None = all targets)
            start_date: Filter by start date (None = all history)
            end_date: Filter by end date (None = now)

        Returns:
            Dictionary with metrics: MAE, RMSE, MAPE, directional_accuracy
        """
        # Filter records
        filtered = self._filter_records(route, target, start_date, end_date)

        if not filtered:
            return {
                'mae': np.nan,
                'rmse': np.nan,
                'mape': np.nan,
                'directional_accuracy': np.nan,
                'count': 0
            }

        # Calculate metrics
        errors = [r['error'] for r in filtered if r['error'] is not None]
        actuals = [r['actual_value'] for r in filtered if r['actual_value'] is not None]
        directional = [r['directional_correct'] for r in filtered if r['directional_correct'] is not None]

        metrics = {
            'count': len(filtered),
            'mae': np.mean(errors) if errors else np.nan,
            'rmse': np.sqrt(np.mean([e**2 for e in errors])) if errors else np.nan,
            'mape': np.mean([abs(r['error'] / r['actual_value']) for r in filtered
                           if r['error'] is not None and r['actual_value'] is not None and r['actual_value'] != 0]) if actuals else np.nan,
            'directional_accuracy': np.mean(directional) if directional else np.nan
        }

        return metrics

    def update_baseline(self, route: str, target: str):
        """
        Update baseline metrics for a route/target combination.

        Uses last N days of history to establish baseline performance.

        Args:
            route: Route identifier
            target: Target variable
        """
        cutoff_date = datetime.now() - timedelta(days=self.baseline_window_days)
        baseline_metrics = self.calculate_metrics(route, target, start_date=cutoff_date)

        key = f"{route}_{target}"
        self.baselines[key] = baseline_metrics

    def _calculate_baseline_metrics(
        self,
        route: str,
        target: str,
        *,
        recent_window_days: int,
        reference_time: Optional[datetime] = None,
    ) -> Dict[str, float]:
        """Calculate a baseline window that does not overlap the recent comparison window."""
        if reference_time is None:
            reference_time = datetime.now()

        recent_cutoff = reference_time - timedelta(days=recent_window_days)
        baseline_start = recent_cutoff - timedelta(days=self.baseline_window_days)
        baseline_metrics = self.calculate_metrics(
            route,
            target,
            start_date=baseline_start,
            end_date=recent_cutoff,
        )

        if baseline_metrics.get('count', 0) > 0:
            return baseline_metrics

        return self.calculate_metrics(route, target, end_date=recent_cutoff)

    def check_for_degradation(self, route: str, target: str, window_days: int = 7) -> Dict[str, any]:
        """
        Check if recent performance has degraded compared to baseline.

        Args:
            route: Route identifier
            target: Target variable
            window_days: Number of recent days to check

        Returns:
            Dictionary with:
            - degraded: Boolean indicating if degradation detected
            - current_mae: Current MAE
            - baseline_mae: Baseline MAE
            - degradation_pct: Percentage degradation
            - message: Alert message if degraded
        """
        # Get baseline
        key = f"{route}_{target}"
        if key not in self.baselines:
            self.baselines[key] = self._calculate_baseline_metrics(
                route,
                target,
                recent_window_days=window_days,
            )

        baseline = self.baselines.get(key, {})
        baseline_mae = baseline.get('mae', np.nan)

        # Get recent metrics
        cutoff_date = datetime.now() - timedelta(days=window_days)
        recent_metrics = self.calculate_metrics(route, target, start_date=cutoff_date)
        current_mae = recent_metrics.get('mae', np.nan)

        # Check for degradation
        if np.isnan(baseline_mae) or np.isnan(current_mae):
            return {
                'degraded': False,
                'current_mae': current_mae,
                'baseline_mae': baseline_mae,
                'degradation_pct': np.nan,
                'message': 'Insufficient data for degradation check'
            }

        degradation_pct = (current_mae - baseline_mae) / baseline_mae if baseline_mae != 0 else 0
        degraded = degradation_pct > self.alert_threshold

        result = {
            'degraded': degraded,
            'current_mae': current_mae,
            'baseline_mae': baseline_mae,
            'degradation_pct': degradation_pct,
            'message': ''
        }

        if degraded:
            result['message'] = (
                f"ALERT: Prediction accuracy degraded for {route} - {target}. "
                f"MAE increased by {degradation_pct:.1%} (baseline: {baseline_mae:.3f}, current: {current_mae:.3f})"
            )
            warnings.warn(result['message'])

        return result

    def get_summary_report(self, group_by: str = 'route') -> pd.DataFrame:
        """
        Generate summary report of prediction performance.

        Args:
            group_by: Grouping level ('route', 'target', 'both')

        Returns:
            DataFrame with performance metrics per group
        """
        if not self.metrics_history:
            return pd.DataFrame()

        df = pd.DataFrame(self.metrics_history)

        # Filter to records with actual values
        df_with_actuals = df[df['actual_value'].notna()].copy()

        if len(df_with_actuals) == 0:
            return pd.DataFrame()

        # Group and aggregate
        if group_by == 'route':
            grouped = df_with_actuals.groupby('route')
        elif group_by == 'target':
            grouped = df_with_actuals.groupby('target')
        elif group_by == 'both':
            grouped = df_with_actuals.groupby(['route', 'target'])
        else:
            raise ValueError(f"Invalid group_by value: {group_by}")

        summary = grouped.agg({
            'error': ['mean', 'std', 'count'],
            'directional_correct': 'mean'
        }).reset_index()

        # Flatten column names
        summary.columns = ['_'.join(col).strip('_') for col in summary.columns.values]

        # Rename for clarity
        summary = summary.rename(columns={
            'error_mean': 'mae',
            'error_std': 'mae_std',
            'error_count': 'prediction_count',
            'directional_correct_mean': 'directional_accuracy'
        })

        # Calculate RMSE
        if group_by == 'route':
            summary['rmse'] = df_with_actuals.groupby('route').apply(
                lambda x: np.sqrt(np.mean(x['error']**2))
            ).values
        elif group_by == 'target':
            summary['rmse'] = df_with_actuals.groupby('target').apply(
                lambda x: np.sqrt(np.mean(x['error']**2))
            ).values
        elif group_by == 'both':
            summary['rmse'] = df_with_actuals.groupby(['route', 'target']).apply(
                lambda x: np.sqrt(np.mean(x['error']**2))
            ).values

        return summary.sort_values('mae', ascending=False)

    def _filter_records(self,
                       route: Optional[str] = None,
                       target: Optional[str] = None,
                       start_date: Optional[datetime] = None,
                       end_date: Optional[datetime] = None) -> List[Dict]:
        """Filter metrics history based on criteria."""
        filtered = self.metrics_history

        if route is not None:
            filtered = [r for r in filtered if r['route'] == route]

        if target is not None:
            filtered = [r for r in filtered if r['target'] == target]

        if start_date is not None:
            filtered = [r for r in filtered if r['timestamp'] >= start_date]

        if end_date is not None:
            filtered = [r for r in filtered if r['timestamp'] <= end_date]

        # Only include records with actual values for metrics calculation
        filtered = [r for r in filtered if r['actual_value'] is not None]

        return filtered

    def export_metrics(self, filepath: str):
        """
        Export metrics history to JSON file.

        Args:
            filepath: Output file path
        """
        # Convert timestamps to strings for JSON serialization
        export_data = []
        for record in self.metrics_history:
            record_copy = record.copy()
            record_copy['timestamp'] = record_copy['timestamp'].isoformat()
            export_data.append(record_copy)

        with open(filepath, 'w') as f:
            json.dump(export_data, f, indent=2)

    def import_metrics(self, filepath: str):
        """
        Import metrics history from JSON file.

        Args:
            filepath: Input file path
        """
        with open(filepath, 'r') as f:
            import_data = json.load(f)

        # Convert timestamp strings back to datetime
        for record in import_data:
            record['timestamp'] = datetime.fromisoformat(record['timestamp'])
            self.metrics_history.append(record)


def monitor_prediction_drift(historical_df: pd.DataFrame,
                            recent_df: pd.DataFrame,
                            metric_col: str = 'mae',
                            threshold: float = 0.20) -> Dict[str, any]:
    """
    Compare recent predictions to historical baseline to detect drift.

    Args:
        historical_df: DataFrame with historical metrics (baseline)
        recent_df: DataFrame with recent metrics
        metric_col: Metric column to compare (e.g., 'mae', 'rmse')
        threshold: Alert if metric degrades by more than this fraction

    Returns:
        Dictionary with drift analysis results
    """
    if metric_col not in historical_df.columns or metric_col not in recent_df.columns:
        return {'error': f"Metric column '{metric_col}' not found"}

    historical_mean = historical_df[metric_col].mean()
    recent_mean = recent_df[metric_col].mean()

    drift_pct = (recent_mean - historical_mean) / historical_mean if historical_mean != 0 else 0
    drifted = abs(drift_pct) > threshold

    return {
        'drifted': drifted,
        'historical_mean': historical_mean,
        'recent_mean': recent_mean,
        'drift_pct': drift_pct,
        'threshold': threshold,
        'message': f"Drift detected: {metric_col} changed by {drift_pct:.1%}" if drifted else "No significant drift"
    }
