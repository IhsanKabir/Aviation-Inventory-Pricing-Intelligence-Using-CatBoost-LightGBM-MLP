# Next Phase Improvements - Implementation Roadmap

**Created**: 2026-03-23
**Status**: Ready for Implementation
**Prerequisites**: All 6 Quick Wins Completed ✓

---

## Executive Summary

This document outlines the next 5 high-priority improvements that build on the successfully completed Quick Wins. These improvements focus on **integrating existing modules** that have been built but not yet connected to the prediction pipeline.

**Key Insight**: 3 out of 5 of these improvements require only integration work - the engineering is already done!

**Expected Combined Impact**:
- **Accuracy**: +10-15% MAE reduction (beyond the 26% from Quick Wins)
- **Coverage**: +15-20% (from 75% to 88-90% of routes)
- **User Trust**: +30% (from SHAP explanations and performance tracking)
- **Operational Visibility**: Real-time model performance monitoring

---

## Priority 1: SHAP Feature Importance Export Integration

### Status: Module Ready, Integration Needed
**Effort**: 1 day | **Impact**: HIGH | **Risk**: LOW

### Current State
- ✓ `core/explainability.py` fully implemented
- ✓ Functions: `compute_shap_feature_importance()`, `get_top_features()`, `format_feature_importance_for_output()`
- ✓ Supports CatBoost, LightGBM, and MLP models
- ✓ Graceful fallback if SHAP unavailable
- ✗ **NOT integrated** into prediction pipeline

### Integration Gap
The module exists but is not being called in `predict_next_day.py`. Users cannot see which features drive each prediction.

### Implementation Plan

**File**: `predict_next_day.py`

**Step 1**: Import explainability module (around line 13)
```python
from core.explainability import (
    compute_shap_feature_importance,
    format_feature_importance_for_output
)
```

**Step 2**: Add SHAP computation after model training (around line 1724)
```python
# After: next_day_df = add_prediction_confidence(...)
# Add SHAP feature importance
if not next_day_df.empty and chosen_model is not None:
    try:
        shap_importance = compute_shap_feature_importance(
            model=chosen_model,
            X=feature_matrix,
            feature_names=feature_columns,
            max_samples=100  # Limit for performance
        )
        if shap_importance:
            # Format top 5 features for output
            shap_cols = format_feature_importance_for_output(
                shap_importance,
                top_n=5
            )
            # Add to prediction output
            for col_name, col_value in shap_cols.items():
                next_day_df[col_name] = col_value
    except Exception as e:
        # Graceful fallback - don't break predictions
        print(f"Warning: SHAP computation failed: {e}")
```

**Step 3**: Update output CSV columns documentation

### Expected Output
Predictions will include new columns:
- `shap_feature_1`, `shap_value_1`
- `shap_feature_2`, `shap_value_2`
- `shap_feature_3`, `shap_value_3`
- `shap_feature_4`, `shap_value_4`
- `shap_feature_5`, `shap_value_5`

### User Benefit
RM team can see: "Price increased because: lag1 (+$120), holiday_next_week (+$80), peak_booking_window (+$45)"

### Testing
```bash
python predict_next_day.py --target price_events --report-start-date 2026-03-20 --report-end-date 2026-03-23
# Check output CSV for shap_* columns
```

---

## Priority 2: Booking Curve Features Integration

### Status: Module Ready, Integration Needed
**Effort**: 1 day | **Impact**: HIGH | **Risk**: LOW

### Current State
- ✓ `core/booking_curve_features.py` fully implemented
- ✓ 13 features: booking windows, peak indicators, booking progress, log transforms
- ✓ Tests exist: `tests/test_booking_curve_features.py`
- ✗ **NOT integrated** into feature engineering pipeline

### Integration Gap
Holiday features are applied (lines 970, 1037) but booking curve features are not. This is a critical gap because booking curve effects drive pricing behavior.

### Implementation Plan

**File**: `predict_next_day.py`

**Step 1**: Import booking curve module (around line 13)
```python
from core.booking_curve_features import (
    add_booking_curve_features,
    get_booking_curve_feature_columns
)
```

**Step 2**: Define booking curve columns (around line 50)
```python
BOOKING_CURVE_FEATURE_COLS = [
    "booking_advance_days",
    "booking_window_0_7",
    "booking_window_8_14",
    "booking_window_15_30",
    "booking_window_31_60",
    "booking_window_61_90",
    "booking_window_91plus",
    "is_peak_booking_window",
    "is_late_booking",
    "is_early_booking",
    "booking_curve_phase",
    "booking_progress",
    "log_booking_advance"
]
```

**Step 3**: Create safe wrapper (around line 380)
```python
def _apply_booking_curve_features_safe(
    df: pd.DataFrame,
    search_col: str = "report_day",
    departure_col: str = "departure_day"
) -> pd.DataFrame:
    """Safely apply booking curve features to DataFrame."""
    if df.empty or search_col not in df.columns or departure_col not in df.columns:
        return df
    try:
        return add_booking_curve_features(
            df,
            search_date_col=search_col,
            departure_date_col=departure_col
        )
    except Exception as e:
        print(f"Warning: Booking curve feature extraction failed: {e}")
        # Add zero-filled columns as fallback
        for col in get_booking_curve_feature_columns():
            if col not in df.columns:
                df[col] = 0
        return df
```

**Step 4**: Apply in feature engineering pipeline (around line 971)
```python
df = _apply_market_priors_safe(df)
df = _apply_holiday_features_safe(df)
df = _apply_booking_curve_features_safe(df)  # Add this line
return df
```

**Step 5**: Apply in backtest data loading (around line 1038)
```python
df = _apply_market_priors_safe(df)
df = _apply_holiday_features_safe(df)
df = _apply_booking_curve_features_safe(df)  # Add this line
return df
```

### Expected Impact
- **Price Events**: +8-12% accuracy (booking curve is THE driver of pricing)
- **Soldout Rate**: +5-8% accuracy (late bookings correlate with capacity constraints)

### Testing
```bash
python predict_next_day.py --target price_events --report-start-date 2026-04-05 --report-end-date 2026-04-15
# Test around Eid period when booking curves are most visible
```

---

## Priority 3: Route Characteristics Integration

### Status: Module Ready, Integration Needed
**Effort**: 1 day | **Impact**: HIGH | **Risk**: LOW

### Current State
- ✓ `core/route_characteristics.py` fully implemented
- ✓ Features: distance, route type, hub indicators, competition level
- ✓ Config exists: `config/route_characteristics.json`
- ✓ 20+ airports mapped with coordinates
- ✗ **NOT integrated** into feature engineering

### Integration Gap
Currently only using generic temporal features (day_of_week, days_to_departure). Missing critical route-specific signals that affect pricing and capacity.

### Implementation Plan

**File**: `predict_next_day.py`

**Step 1**: Import route characteristics module (around line 13)
```python
from core.route_characteristics import (
    calculate_route_distance,
    get_route_type,
    add_route_characteristics
)
```

**Step 2**: Define route characteristic columns (around line 60)
```python
ROUTE_CHARACTERISTIC_COLS = [
    "route_distance_km",
    "route_type",  # 0=domestic, 1=regional, 2=long-haul
    "is_hub_origin",
    "is_hub_destination",
    "is_hub_route",
    "competition_level"  # 0=monopoly, 1=duopoly, 2=high-competition
]
```

**Step 3**: Apply in feature engineering (around line 972)
```python
def _apply_route_characteristics_safe(df: pd.DataFrame) -> pd.DataFrame:
    """Safely apply route characteristics to DataFrame."""
    if df.empty or 'origin' not in df.columns or 'destination' not in df.columns:
        return df
    try:
        return add_route_characteristics(df)
    except Exception as e:
        print(f"Warning: Route characteristics extraction failed: {e}")
        # Add zero-filled columns as fallback
        for col in ROUTE_CHARACTERISTIC_COLS:
            if col not in df.columns:
                df[col] = 0
        return df

# In pipeline (around line 973):
df = _apply_market_priors_safe(df)
df = _apply_holiday_features_safe(df)
df = _apply_booking_curve_features_safe(df)
df = _apply_route_characteristics_safe(df)  # Add this line
return df
```

### Expected Impact
- **Coverage**: Routes with similar characteristics can share learnings
- **Accuracy**: +6-10% for route-specific pricing patterns
- **Enables**: Transfer learning (Priority 4) to work properly

### Testing
```bash
python predict_next_day.py --target min_price_bdt --report-start-date 2026-03-20 --report-end-date 2026-03-23
# Check that DAC-DXB (regional) has different behavior than DAC-CXB (domestic)
```

---

## Priority 4: Transfer Learning for Sparse Routes

### Status: Module Ready, Integration Needed
**Effort**: 2 days | **Impact**: MEDIUM-HIGH | **Risk**: MEDIUM

### Current State
- ✓ `core/transfer_learning.py` fully implemented
- ✓ Functions: `find_similar_routes()`, `apply_transfer_learning_model()`
- ✓ Handles cold-start problem for new/sparse routes
- ✗ **NOT integrated** - routes with <14 days history still get NO predictions

### Integration Gap
In `predict_next_day.py` around line 461-550 (`_build_ml_dl_predictions`):
- Currently: `if len(train_x) < min_history_rows` → skip route
- Should: Try transfer learning from similar routes

### Implementation Plan

**File**: `predict_next_day.py`

**Step 1**: Import transfer learning module (around line 13)
```python
from core.transfer_learning import (
    find_similar_routes,
    apply_transfer_learning_model
)
```

**Step 2**: Add transfer learning fallback (around line 570)
```python
def _build_ml_dl_predictions(...):
    # ... existing code ...

    # After minimum history check fails:
    if len(train_x) < min_history_rows:
        # Try transfer learning instead of giving up
        try:
            similar_routes = find_similar_routes(
                origin=origin,
                destination=destination,
                df_all_routes=full_history_df,  # Need to pass this
                top_n=3
            )

            if similar_routes:
                # Build transfer learning model from similar routes
                transfer_model = apply_transfer_learning_model(
                    sparse_route_data=train_df,
                    similar_routes_data=similar_routes,
                    target=target,
                    model_type=model_name
                )

                if transfer_model:
                    pred = transfer_model.predict(pred_f)
                    return _clip_to_bounds(float(pred[0]), lower, upper)
        except Exception as e:
            print(f"Transfer learning failed for {origin}-{destination}: {e}")

        # Original fallback if transfer learning also fails
        return None

    # ... rest of existing code ...
```

**Step 3**: Pass full dataset to enable similarity search
- Requires refactoring to make `full_history_df` available in scope
- Cache similar routes to avoid recomputation

### Expected Impact
- **Coverage**: +15-20% (from 75% to 88-90% of routes)
- **New Routes**: Can predict from day 1 instead of waiting 14 days
- **Data Efficiency**: Leverages knowledge from established routes

### Risk Mitigation
- Validate transfer learning predictions against holdout data first
- Only use if similar routes have R² > 0.7 correlation
- Flag predictions as "transfer_learning_based" in output

### Testing
```bash
# Test on a new route or one with sparse data
python predict_next_day.py --route DAC-JSR --target price_events --report-start-date 2026-03-20
# Should now get predictions even if <14 days of history
```

---

## Priority 5: Actual vs Predicted Tracking & Performance Dashboard

### Status: Module Ready, Integration Needed
**Effort**: 1.5 days | **Impact**: HIGH | **Risk**: LOW

### Current State
- ✓ `core/prediction_monitor.py` exists with PredictionMonitor class
- ✓ Methods: `log_prediction()`, `calculate_mae()`, `check_drift()`
- ✓ `scheduler/maintenance_tasks.py` exists for scheduled tasks
- ✗ **NOT integrated** - predictions are not being logged or tracked

### Integration Gap
- Predictions are written to CSV but not logged to monitor
- No real-time MAE tracking against actuals
- No alert when performance degrades
- RM team has no production accuracy visibility

### Implementation Plan

**Step 1**: Initialize monitor in predict_next_day.py (around line 15)
```python
from core.prediction_monitor import PredictionMonitor

# Initialize monitor
prediction_monitor = PredictionMonitor(database_url=DEFAULT_DATABASE_URL)
```

**Step 2**: Log predictions after CSV write (around line 1735)
```python
# After writing CSV output
if not next_day_df.empty:
    try:
        # Log each prediction for monitoring
        for idx, row in next_day_df.iterrows():
            prediction_monitor.log_prediction(
                route=f"{row['origin']}-{row['destination']}",
                target=target,
                prediction_date=row['report_day'],
                predicted_value=row.get('prediction', row.get('ml_pred')),
                actual_value=None,  # Will be filled later when actual is available
                model_type=row.get('chosen_model', 'unknown'),
                confidence=row.get('prediction_confidence', 'medium')
            )
    except Exception as e:
        print(f"Warning: Prediction logging failed: {e}")
```

**Step 3**: Create daily performance report tool

**New File**: `tools/daily_performance_report.py`
```python
"""Generate daily actual vs predicted performance report."""

import argparse
from datetime import datetime, timedelta
import pandas as pd
from core.prediction_monitor import PredictionMonitor

def generate_daily_report(database_url, lookback_days=7):
    monitor = PredictionMonitor(database_url)

    # Get predictions from last N days
    predictions = monitor.get_recent_predictions(days=lookback_days)

    # Join with actuals (from flight_offer_raw_meta or predictions table)
    # Calculate MAE, RMSE, directional accuracy

    # Generate report
    report = {
        "date": datetime.now().isoformat(),
        "overall_mae": predictions["mae"].mean(),
        "routes_evaluated": predictions["route"].nunique(),
        "alerts": []
    }

    # Check for degraded routes
    for route in predictions["route"].unique():
        route_mae = predictions[predictions["route"] == route]["mae"].mean()
        baseline_mae = monitor.get_baseline_mae(route)

        if route_mae > baseline_mae * 1.2:  # 20% degradation
            report["alerts"].append({
                "route": route,
                "current_mae": route_mae,
                "baseline_mae": baseline_mae,
                "degradation": f"{((route_mae / baseline_mae - 1) * 100):.1f}%"
            })

    return report

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument("--lookback-days", type=int, default=7)
    args = parser.parse_args()

    report = generate_daily_report(args.database_url, args.lookback_days)
    print(json.dumps(report, indent=2))
```

**Step 4**: Add to maintenance schedule

**File**: `scheduler/maintenance_tasks.py`
```python
# Add daily performance report task
{
    "name": "daily_performance_report",
    "command": "python tools/daily_performance_report.py",
    "schedule": "0 9 * * *",  # 9 AM daily
    "enabled": True
}
```

### Expected Impact
- **Visibility**: Real-time model performance tracking
- **Governance**: Automatic alerts when accuracy degrades
- **Trust**: RM team can validate predictions against actuals
- **Retraining**: Data-driven decision on when to retrain models

### Testing
```bash
# Run predictions for past week
python predict_next_day.py --target price_events --report-start-date 2026-03-16 --report-end-date 2026-03-23

# Generate performance report
python tools/daily_performance_report.py --lookback-days 7
```

---

## Quick Integration Opportunities (Bonus Items)

### A. Quantile Calibration Check (0.5 days)
- Validate that quantile predictions are statistically correct
- Check if 10% of actuals fall below q0.1, 50% below q0.5, etc.
- Add to weekly maintenance tasks

### B. Feature Drift Detection Enhancement (1 day)
- Extend `tools/model_drift_monitor.py` with PSI (Population Stability Index)
- Detect when feature distributions shift (data quality issue)
- Alert before model accuracy degrades

### C. Data Freshness Dashboard (0.5 days)
- Hook `tools/data_quality_gates.py` into operator dashboard
- Show last accumulation time, data age alerts
- Integrate with `tools/notify_ops_health.py`

---

## Implementation Schedule (Recommended 5-Day Sprint)

### Day 1: High-Impact Integrations
- **Morning**: SHAP feature importance export (Priority 1)
- **Afternoon**: Booking curve features integration (Priority 2)
- **End of Day**: Run test predictions, validate outputs

### Day 2: Route Features & Testing
- **Morning**: Route characteristics integration (Priority 3)
- **Afternoon**: Test on historical data, validate accuracy improvements
- **End of Day**: Commit and document changes

### Day 3: Transfer Learning
- **All Day**: Transfer learning integration (Priority 4)
- Complex integration requiring careful testing
- **End of Day**: Validate coverage improvements

### Day 4: Performance Monitoring
- **Morning**: Prediction monitoring integration (Priority 5)
- **Afternoon**: Build daily performance report tool
- **End of Day**: Set up maintenance schedule

### Day 5: Testing, Documentation & Deployment
- **Morning**: Comprehensive integration testing
- **Afternoon**: Update documentation, create deployment guide
- **End of Day**: Production deployment preparation

---

## Success Metrics

Track these metrics before and after implementation:

| Metric | Before | Target After | How to Measure |
|--------|--------|--------------|----------------|
| MAE (price_events) | 0.26 | 0.22 | Backtest on last 30 days |
| Directional F1 | 0.74 | 0.80 | F1_macro from backtest |
| Route Coverage | 75% | 88% | Routes with predictions / total routes |
| User Trust (SHAP) | N/A | 4.5/5 | RM team survey |
| Monitoring Coverage | 0% | 100% | Predictions logged / total predictions |

---

## Risk Assessment

### Low Risk (Priorities 1, 2, 3, 5)
- Modules are mature and tested
- Integration is straightforward
- Graceful fallbacks built in
- Can deploy incrementally

### Medium Risk (Priority 4)
- Transfer learning requires validation
- Similarity matching may not always work well
- Need to validate on holdout data first
- Can disable if performance doesn't improve

### Mitigation Strategy
1. Deploy behind feature flag for testing
2. Validate on historical data before production
3. Monitor performance metrics daily
4. Rollback capability via git

---

## Dependencies

### Technical
- All Quick Wins must be complete ✓
- Python dependencies installed (requirements.txt)
- Database access for prediction monitoring
- Sufficient compute for SHAP calculations (can limit to 100 samples)

### Operational
- RM team feedback mechanism
- Production deployment process
- Monitoring alert webhook configured

---

## Next Steps After This Phase

Once these 5 priorities are complete, consider:

1. **Multi-horizon forecasting** (Section 1.3 in roadmap)
   - 1-day, 3-day, 7-day forecasts
   - Enables booking curve modeling

2. **Walk-forward validation** (Section 1.3)
   - More robust backtesting
   - Stability across time windows

3. **API endpoint enhancements** (Section 5.3)
   - Real-time scoring endpoint
   - SHAP explanations via API

4. **Troubleshooting guide** (Section 6.1)
   - Common issues and solutions
   - Diagnostic commands

---

## Conclusion

These 5 improvements build directly on the Quick Wins foundation and require minimal new engineering. The modules exist - we just need to connect them.

**Total Expected Impact**:
- Accuracy: +10-15% additional MAE reduction
- Coverage: +15-20% more routes
- Transparency: 100% of predictions explainable
- Governance: Real-time performance tracking

**Development Time**: 5 days (1 developer)

**Risk Level**: LOW-MEDIUM (mostly integration work)

**ROI**: VERY HIGH (built on existing investments)

---

**Document Owner**: Aviation Intelligence Platform Team
**Created**: 2026-03-23
**Status**: Ready for Implementation
**Next Review**: 2026-03-30 (after completion)
