# System Improvement Priorities - Quick Reference

**Last Updated**: 2026-03-18
**For Full Details**: See [SYSTEM_IMPROVEMENT_ROADMAP.md](SYSTEM_IMPROVEMENT_ROADMAP.md)

---

## Top 6 Quick Wins (Start Here)

These improvements deliver maximum impact with minimal effort. Each can be completed in 1-2 days.

### 1. 🎯 Holiday Feature Engineering
**Impact**: High | **Effort**: Medium | **Expected Lift**: 12-18% accuracy

**What**: Add Bangladesh holiday calendar (Eid, Pohela Boishakh, Victory Day, etc.) as prediction features

**Why**: Critical for Bangladesh aviation market where Eid drives massive demand surges

**How**:
```bash
# 1. Create holiday calendar
echo '{"holidays": [{"date": "2026-04-11", "name": "Eid-ul-Fitr", "type": "religious"}, ...]}' > config/holiday_calendar.json

# 2. Add feature engineering in predict_next_day.py
# Add: is_departure_holiday, days_to_next_holiday, is_holiday_week

# 3. Test on historical Eid periods
python predict_next_day.py --target price_events --report-start-date 2026-03-20 --report-end-date 2026-04-20
```

**Files**:
- New: `config/holiday_calendar.json`, `core/holiday_features.py`
- Modify: `predict_next_day.py:345-386` (_ml_feature_frame)

---

### 2. 🔍 Feature Importance in Outputs
**Impact**: High | **Effort**: Low | **Expected Benefit**: Increased user trust

**What**: Export SHAP feature importance with each prediction

**Why**: RM team can understand WHY the model predicts a price change, not just WHAT it predicts

**How**:
```python
# In predict_next_day.py after model training:
import shap
explainer = shap.TreeExplainer(model)  # For CatBoost/LightGBM
shap_values = explainer.shap_values(X_test)
# Export top 5 features per prediction to CSV
```

**Files**:
- Modify: `predict_next_day.py:550-600`
- New: `core/explainability.py`

---

### 3. 📊 Prediction Confidence Bands
**Impact**: High | **Effort**: Low | **Expected Benefit**: Know when to trust predictions

**What**: Add confidence metric (high/medium/low) based on quantile spread and historical accuracy

**Why**: Users need to know prediction uncertainty to make better decisions

**How**:
```python
# After generating quantile predictions:
uncertainty = prediction_q90 - prediction_q10
historical_mae = route_backtest_results['mae']
confidence = 'high' if uncertainty < 0.1 * prediction_q50 and historical_mae < threshold else 'medium' or 'low'
```

**Files**:
- Modify: `predict_next_day.py:600-650`
- New: `core/confidence_metrics.py`

---

### 4. ✅ Data Quality Pre-Prediction Gate
**Impact**: High | **Effort**: Low | **Expected Benefit**: Prevent bad predictions

**What**: Check data freshness, completeness, outliers before running predictions

**Why**: Garbage in = garbage out. Catch data issues before they produce bad forecasts.

**How**:
```python
# Before prediction pipeline:
def validate_data_quality(df, min_rows=100, max_null_pct=0.05, max_age_hours=24):
    assert len(df) >= min_rows, "Insufficient data"
    assert df.isnull().mean().max() < max_null_pct, "Too many nulls"
    assert (datetime.now() - df['captured_at'].max()).hours < max_age_hours, "Stale data"
    return True
```

**Files**:
- New: `tools/data_quality_gates.py`
- Modify: `predict_next_day.py:100-150` (add gate at start)

---

### 5. 🔧 Automated Hyperparameter Tuning
**Impact**: Medium-High | **Effort**: Medium | **Expected Lift**: 8-15% accuracy

**What**: Use Optuna to find best model params per route/target

**Why**: Current params (250 iters, depth=6, lr=0.05) are generic. Route-specific tuning improves accuracy.

**How**:
```python
# Add tools/tune_hyperparameters.py
import optuna

def objective(trial):
    params = {
        'iterations': trial.suggest_int('iterations', 100, 500),
        'depth': trial.suggest_int('depth', 4, 10),
        'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3)
    }
    model = CatBoostRegressor(**params)
    model.fit(X_train, y_train)
    return mean_absolute_error(y_val, model.predict(X_val))

study = optuna.create_study(direction='minimize')
study.optimize(objective, n_trials=50)
```

**Files**:
- New: `tools/tune_hyperparameters.py`, `config/model_config_overrides.json`
- Modify: `predict_next_day.py:461-550` (load overrides if exist)

---

### 6. 📖 Feature Engineering Documentation
**Impact**: Medium | **Effort**: Low | **Expected Benefit**: Faster debugging, better thesis

**What**: Document each feature's rationale, computation, and expected impact

**Why**: Team members (and future thesis reviewers) need to understand feature choices

**How**: Create `docs/FEATURE_ENGINEERING_GUIDE.md` with table:

| Feature | Rationale | Computation | Expected Impact |
|---------|-----------|-------------|-----------------|
| `lag_1` | Previous day's value is strong predictor | `df['target'].shift(1)` | High for smooth trends |
| `days_to_departure` | Booking curve effect | `(departure_date - search_date).days` | Critical for pricing |
| `is_holiday` | Demand surges on holidays | Lookup in holiday_calendar.json | High for Bangladesh routes |

**Files**: New: `docs/FEATURE_ENGINEERING_GUIDE.md`

---

## Implementation Order (Recommended)

### Week 1: Data Quality Foundation
1. ✅ Data Quality Gate (#4) - Prevent bad predictions
2. 📖 Feature Engineering Docs (#6) - Understand current state

### Week 2: Transparency & Trust
3. 🔍 Feature Importance (#2) - Show WHY predictions happen
4. 📊 Confidence Bands (#3) - Show WHEN to trust predictions

### Week 3: Accuracy Boost
5. 🎯 Holiday Features (#1) - Critical for Bangladesh market
6. 🔧 Hyperparameter Tuning (#5) - Optimize model params

### Week 4: Validation & Rollout
- Run backtests with all improvements
- Generate comparison report: old vs new accuracy
- Update thesis pack with results
- Deploy to production pipeline

---

## Expected Cumulative Impact

| Metric | Baseline | After Quick Wins | Improvement |
|--------|----------|------------------|-------------|
| MAE (price_events) | 0.35 | 0.26 | -26% ✓ |
| Directional F1 | 0.62 | 0.74 | +19% ✓ |
| Prediction Coverage | 75% routes | 88% routes | +17% ✓ |
| User Trust Score | 3.2/5 | 4.3/5 | +34% ✓ |

---

## Long-Term Improvements (After Quick Wins)

Once Quick Wins are complete, proceed with these Phase 2 enhancements:

### Phase 2A: Core Accuracy (Weeks 5-8)
- Booking curve features (Section 4.1 in roadmap)
- Route characteristics (Section 4.1)
- Transfer learning for sparse routes (Section 1.1)

### Phase 2B: Reliability (Weeks 9-12)
- Robust imputation pipeline (Section 2.1)
- Outlier detection (Section 2.2)
- Exponential backoff retry (Section 3.1)
- Real-time monitoring dashboard (Section 3.2)

### Phase 2C: User Experience (Weeks 13-16)
- SHAP explainability (Section 5.1)
- Actual vs predicted tracking (Section 5.2)
- Prediction change explanations (Section 5.1)

See [SYSTEM_IMPROVEMENT_ROADMAP.md](SYSTEM_IMPROVEMENT_ROADMAP.md) for full details on Phases 2A-2D.

---

## Success Criteria

Before considering improvements "complete", validate:

✅ **Accuracy**: MAE reduced by ≥20% on validation set
✅ **Coverage**: Predictions available for ≥85% of routes
✅ **Confidence**: User trust score ≥4.0/5 (survey RM team)
✅ **Reliability**: Prediction failure rate <5%
✅ **Documentation**: All features documented with rationale

---

## Resource Requirements

**Time**: 4 weeks for Quick Wins + 12 weeks for Phase 2 = 4 months total
**People**: 1 FTE (can be part-time for thesis work)
**Compute**: Current laptop sufficient for Quick Wins; cloud optional for Phase 2B tuning
**Storage**: Monitor PostgreSQL growth, may need partitioning in Phase 2B

---

## Risk Mitigation

**Risk**: Improvements don't actually increase accuracy
**Mitigation**: Run rigorous backtest on holdout dates before deploying. If no improvement, rollback.

**Risk**: Thesis timeline pressure
**Mitigation**: Quick Wins deliverable in 4 weeks. Can complete thesis with just these 6 items.

**Risk**: Data quality issues
**Mitigation**: Gate #4 catches issues early. Always validate before training.

---

## Getting Started

```bash
# 1. Review full roadmap
cat SYSTEM_IMPROVEMENT_ROADMAP.md

# 2. Start with Gate #4 (Data Quality)
python tools/data_quality_gates.py --check-all

# 3. Then Feature Importance #2
python predict_next_day.py --export-feature-importance --target price_events

# 4. Weekly review
# Check PROJECT_DECISIONS.md Section 11 for tracking
```

---

**Questions?** See [SYSTEM_IMPROVEMENT_ROADMAP.md](SYSTEM_IMPROVEMENT_ROADMAP.md) for detailed explanations, code examples, and files to modify.

**Next Review**: 2026-04-01 (track progress, adjust priorities)
