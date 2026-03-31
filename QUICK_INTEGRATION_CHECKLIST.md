# Quick Integration Checklist

**Purpose**: Fast-reference checklist for integrating existing feature modules
**For**: Developers implementing NEXT_PHASE_IMPROVEMENTS.md
**Status**: Ready to implement

---

## Integration Status Overview

| Priority | Feature Module | Status | Effort | Files to Modify |
|----------|----------------|--------|--------|-----------------|
| 1 | SHAP Feature Importance | ⚠️ Built, Not Integrated | 1 day | predict_next_day.py |
| 2 | Booking Curve Features | ⚠️ Built, Not Integrated | 1 day | predict_next_day.py |
| 3 | Route Characteristics | ⚠️ Built, Not Integrated | 1 day | predict_next_day.py |
| 4 | Transfer Learning | ⚠️ Built, Not Integrated | 2 days | predict_next_day.py |
| 5 | Prediction Monitoring | ⚠️ Built, Not Integrated | 1.5 days | predict_next_day.py + new tool |

**Legend**: ✓ Done | ⚠️ Needs Integration | ✗ Missing

---

## Pre-Integration Checklist

Before starting, verify:

- [ ] All 6 Quick Wins completed (see IMPROVEMENT_VERIFICATION_REPORT.md)
- [ ] Dependencies installed: `pip install -r requirements.txt`
- [ ] Database accessible: `echo $DATABASE_URL`
- [ ] Git branch created: `git checkout -b feature/next-phase-integrations`
- [ ] Backup current predict_next_day.py: `cp predict_next_day.py predict_next_day.py.backup`

---

## Priority 1: SHAP Integration (30 minutes)

### Location: `predict_next_day.py`

### Step 1: Add import (line ~13)
```python
from core.explainability import (
    compute_shap_feature_importance,
    format_feature_importance_for_output
)
```

### Step 2: Add SHAP computation (line ~1724, after confidence bands)
```python
# After: next_day_df = add_prediction_confidence(...)
if not next_day_df.empty and 'ml_pred' in next_day_df.columns:
    try:
        # Get model and feature matrix from training
        # (need to store these from _build_ml_dl_predictions)
        shap_importance = compute_shap_feature_importance(
            model=chosen_model,  # Store this earlier
            X=feature_matrix,    # Store this earlier
            feature_names=feature_columns,
            max_samples=100
        )
        if shap_importance:
            shap_cols = format_feature_importance_for_output(shap_importance, top_n=5)
            for col_name, col_value in shap_cols.items():
                next_day_df[col_name] = col_value
    except Exception as e:
        print(f"Warning: SHAP computation failed: {e}")
```

### Step 3: Test
```bash
python predict_next_day.py --target price_events --report-start-date 2026-03-20 --report-end-date 2026-03-21
grep "shap_feature_1" output/predictions/next_day_*.csv
```

---

## Priority 2: Booking Curve Integration (30 minutes)

### Location: `predict_next_day.py`

### Step 1: Add import (line ~13)
```python
from core.booking_curve_features import (
    add_booking_curve_features,
    get_booking_curve_feature_columns
)
```

### Step 2: Define feature columns (line ~50, after HOLIDAY_FEATURE_COLS)
```python
BOOKING_CURVE_FEATURE_COLS = get_booking_curve_feature_columns()
```

### Step 3: Create wrapper function (line ~380, after _apply_holiday_features_safe)
```python
def _apply_booking_curve_features_safe(df: pd.DataFrame) -> pd.DataFrame:
    """Safely apply booking curve features to DataFrame."""
    if df.empty or 'report_day' not in df.columns or 'departure_day' not in df.columns:
        return df
    try:
        return add_booking_curve_features(df,
            search_date_col="report_day",
            departure_date_col="departure_day")
    except Exception as e:
        print(f"Warning: Booking curve feature extraction failed: {e}")
        for col in BOOKING_CURVE_FEATURE_COLS:
            if col not in df.columns:
                df[col] = 0
        return df
```

### Step 4: Apply in pipelines (lines ~971 and ~1038)
```python
# In _prepare_feature_frame (line ~971):
df = _apply_market_priors_safe(df)
df = _apply_holiday_features_safe(df)
df = _apply_booking_curve_features_safe(df)  # ADD THIS
return df

# In _load_backtest_data (line ~1038):
df = _apply_market_priors_safe(df)
df = _apply_holiday_features_safe(df)
df = _apply_booking_curve_features_safe(df)  # ADD THIS
return df
```

### Step 5: Test
```bash
python predict_next_day.py --target price_events --report-start-date 2026-04-10 --report-end-date 2026-04-15
# Check for booking_advance_days column in debug output
```

---

## Priority 3: Route Characteristics Integration (1 hour)

### Location: `predict_next_day.py`

### Step 1: Add import (line ~13)
```python
from core.route_characteristics import add_route_characteristics
```

### Step 2: Define feature columns (line ~60)
```python
ROUTE_CHARACTERISTIC_COLS = [
    "route_distance_km",
    "route_type",
    "is_hub_origin",
    "is_hub_destination",
    "is_hub_route",
    "competition_level"
]
```

### Step 3: Create wrapper function (line ~400)
```python
def _apply_route_characteristics_safe(df: pd.DataFrame) -> pd.DataFrame:
    """Safely apply route characteristics to DataFrame."""
    if df.empty or 'origin' not in df.columns or 'destination' not in df.columns:
        return df
    try:
        return add_route_characteristics(df)
    except Exception as e:
        print(f"Warning: Route characteristics extraction failed: {e}")
        for col in ROUTE_CHARACTERISTIC_COLS:
            if col not in df.columns:
                df[col] = 0
        return df
```

### Step 4: Apply in pipelines (lines ~972 and ~1039)
```python
df = _apply_market_priors_safe(df)
df = _apply_holiday_features_safe(df)
df = _apply_booking_curve_features_safe(df)
df = _apply_route_characteristics_safe(df)  # ADD THIS
return df
```

### Step 5: Test
```bash
python predict_next_day.py --target min_price_bdt --route DAC-DXB --report-start-date 2026-03-20
# Check for route_distance_km in features
```

---

## Priority 4: Transfer Learning Integration (2-3 hours)

### Location: `predict_next_day.py`

### Step 1: Add import (line ~13)
```python
from core.transfer_learning import (
    find_similar_routes,
    apply_transfer_learning_model
)
```

### Step 2: Modify _build_ml_dl_predictions function (line ~570)
**This is complex - requires passing additional context**

```python
def _build_ml_dl_predictions(..., full_dataset_df=None):  # Add parameter
    # ... existing code ...

    # After minimum history check fails (line ~570):
    if len(train_x) < min_history_rows:
        # NEW: Try transfer learning fallback
        if full_dataset_df is not None:
            try:
                similar_routes = find_similar_routes(
                    origin=origin,
                    destination=destination,
                    df_all_routes=full_dataset_df,
                    top_n=3
                )
                if similar_routes and not similar_routes.empty:
                    transfer_pred = apply_transfer_learning_model(
                        sparse_route_data=train_df,
                        similar_routes_data=similar_routes,
                        target=target,
                        model_type=model_name
                    )
                    if transfer_pred is not None:
                        return _clip_to_bounds(float(transfer_pred), lower, upper)
            except Exception as e:
                print(f"Transfer learning failed for {origin}-{destination}: {e}")

        # Original fallback
        return None
```

### Step 3: Update function calls to pass full_dataset_df

### Step 4: Test
```bash
# Test on route with sparse data
python predict_next_day.py --route DAC-JSR --target price_events --report-start-date 2026-03-20
```

---

## Priority 5: Prediction Monitoring Integration (2 hours)

### Location: `predict_next_day.py` + new tool

### Step 1: Add import (line ~15)
```python
from core.prediction_monitor import PredictionMonitor
```

### Step 2: Initialize monitor (after imports)
```python
# Initialize prediction monitor
try:
    prediction_monitor = PredictionMonitor(database_url=DEFAULT_DATABASE_URL)
except Exception:
    prediction_monitor = None  # Graceful fallback
```

### Step 3: Log predictions (line ~1735, after CSV write)
```python
# After: next_day_df.to_csv(...)
if prediction_monitor and not next_day_df.empty:
    try:
        for idx, row in next_day_df.iterrows():
            prediction_monitor.log_prediction(
                route=f"{row['origin']}-{row['destination']}",
                target=target,
                prediction_date=row['report_day'],
                predicted_value=row.get('prediction', row.get('ml_pred')),
                model_type=row.get('chosen_model', 'unknown'),
                confidence=row.get('prediction_confidence', 'medium')
            )
    except Exception as e:
        print(f"Warning: Prediction logging failed: {e}")
```

### Step 4: Create daily performance report tool
**New file**: `tools/daily_performance_report.py`
(See full implementation in NEXT_PHASE_IMPROVEMENTS.md)

### Step 5: Test
```bash
python predict_next_day.py --target price_events --report-start-date 2026-03-20
# Check that predictions are logged to database
```

---

## Testing Strategy

### Unit Tests
```bash
# Test individual modules
pytest tests/test_booking_curve_features.py
pytest tests/test_route_characteristics.py
pytest tests/test_explainability.py
```

### Integration Tests
```bash
# Test full pipeline with all features
python predict_next_day.py --target price_events --report-start-date 2026-03-20 --report-end-date 2026-03-23

# Validate outputs
python -c "
import pandas as pd
df = pd.read_csv('output/predictions/next_day_price_events_*.csv')
print('SHAP columns:', [c for c in df.columns if 'shap' in c])
print('Booking curve columns:', [c for c in df.columns if 'booking' in c])
print('Route char columns:', [c for c in df.columns if 'route' in c])
"
```

### Performance Tests
```bash
# Run backtest to measure accuracy improvement
python predict_next_day.py --run-backtest --target price_events --train-days 28 --val-days 7 --test-days 7

# Compare MAE before and after integrations
```

---

## Rollback Plan

If integrations cause issues:

```bash
# Revert predict_next_day.py
git checkout predict_next_day.py

# Or restore backup
cp predict_next_day.py.backup predict_next_day.py

# Disable specific integrations by commenting out function calls
```

---

## Success Criteria

Before considering integrations complete:

- [ ] All 5 priorities integrated without errors
- [ ] Unit tests pass: `pytest tests/`
- [ ] Pipeline runs end-to-end successfully
- [ ] Output CSVs contain new columns (SHAP, booking curve, route chars)
- [ ] Coverage increased (measure routes with predictions)
- [ ] Accuracy improved (run backtest, compare MAE)
- [ ] Predictions logged to monitor (check database)
- [ ] Documentation updated
- [ ] Git committed with clear messages

---

## Common Issues & Solutions

### Issue: Import errors
**Solution**: Verify dependencies installed: `pip install -r requirements.txt`

### Issue: Feature columns not appearing in output
**Solution**: Check that wrapper functions are called in both pipelines (lines 971 AND 1038)

### Issue: SHAP computation slow
**Solution**: Reduce `max_samples` from 100 to 50 or 25

### Issue: Transfer learning not improving coverage
**Solution**: Check similarity threshold, may need to relax matching criteria

### Issue: Monitor logging fails
**Solution**: Verify database connection, check table schema exists

---

## Quick Commands Reference

```bash
# Install dependencies
pip install -r requirements.txt

# Run predictions
python predict_next_day.py --target price_events --report-start-date YYYY-MM-DD

# Run backtest
python predict_next_day.py --run-backtest --target price_events

# Check output
head -n 5 output/predictions/next_day_*.csv

# Run tests
pytest tests/ -v

# Check coverage
python -c "import pandas as pd; df = pd.read_csv('output/predictions/next_day_*.csv'); print(f'Routes: {df.groupby([\"origin\", \"destination\"]).ngroups}')"
```

---

**Last Updated**: 2026-03-23
**Status**: Ready for Implementation
**Estimated Time**: 5 days (1 developer)
**Expected Impact**: +10-15% accuracy, +15-20% coverage, 100% explainability
