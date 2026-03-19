# Aviation Intelligence Platform: System Improvement Roadmap

**Last Updated**: 2026-03-18
**Purpose**: Comprehensive analysis and actionable recommendations to make the system better and more effective

---

## Executive Summary

This document provides a strategic roadmap for enhancing the Aviation Inventory Pricing Intelligence Platform across six critical dimensions:

1. **Model Performance & Evaluation** - Improve prediction accuracy and robustness
2. **Data Quality & Coverage** - Enhance data reliability and completeness
3. **System Reliability & Monitoring** - Strengthen operational resilience
4. **Feature Engineering** - Enrich predictive signals
5. **User Experience & Reporting** - Increase actionability and transparency
6. **Documentation & Maintainability** - Improve knowledge transfer and system understanding

**System Maturity**: The platform has reached Phase 1 completion (multi-airline collection, reporting, baseline forecasting). This roadmap focuses on Phase 2+ optimization to achieve thesis-grade research quality and operational excellence.

---

## 1. Model Performance & Evaluation Enhancements

### Current State
- **Models**: CatBoost (250 iters, depth=6), LightGBM (220 estimators), MLP (64,32 hidden)
- **Targets**: 7 prediction targets (total_change_events, price_events, availability_events, min_price_bdt, avg_seat_available, offers_count, soldout_rate)
- **Evaluation**: MAE, RMSE, MAPE, SMAPE, Directional Accuracy, F1_up, F1_down, F1_macro
- **Backtest**: Rolling train/val/test with 21/7/7 day default windows

### Critical Gaps

#### 1.1 Insufficient Data Handling
**Problem**: Models require minimum 14 (ML) or 8 (DL) days of history. New routes or sparse data routes get no predictions.

**Solutions**:
- [ ] **Priority 1**: Implement transfer learning from similar routes
  - Group routes by distance/market type (domestic/international/Middle East/tourism)
  - Train meta-model on rich-history routes, apply to sparse-data routes
  - Expected lift: 15-25% coverage increase

- [ ] **Priority 2**: Add weighted bootstrapping with domain priors
  - Use market prior coefficients as priors for Bayesian bootstrapping
  - Combine sparse observed data with synthetic samples from similar market segments
  - Reduces cold-start period from 14 days to 5-7 days

- [ ] **Priority 3**: Hybrid baseline fallback
  - When ML/DL unavailable, use enhanced baseline with market-prior weights
  - Better than pure naive/EWMA for new routes

**Files to Modify**:
- `predict_next_day.py:461-550` (_build_ml_dl_predictions)
- Add `core/transfer_learning.py` module

---

#### 1.2 Model Selection & Tuning
**Problem**: Hardcoded hyperparameters, no route-specific or target-specific optimization.

**Solutions**:
- [ ] **Priority 1**: Implement automated hyperparameter tuning
  - Use Optuna for Bayesian hyperparameter search
  - Route-specific tuning: domestic short-haul vs international long-haul
  - Target-specific tuning: pricing vs capacity targets have different cost functions
  - Expected lift: 8-15% accuracy improvement

- [ ] **Priority 2**: Add model persistence layer
  - Cache best model per route/target combination
  - Track model performance over time
  - Auto-retrain only when drift detected (saves compute)

- [ ] **Priority 3**: Enhance model selection criteria
  - Current: MAE with 80% coverage threshold
  - Add: Directional accuracy weight (upside error costlier than downside for pricing)
  - Add: Seasonality detection (use LightGBM for seasonal, CatBoost for irregular)
  - Add: Confidence-weighted ensembling (combine multiple models)

**New Files**:
- `core/hyperparameter_tuning.py`
- `core/model_registry.py`
- `config/model_config_overrides.json`

**Implementation Priority**: High (directly impacts forecast quality)

---

#### 1.3 Backtesting Enhancements
**Problem**: Single rolling window, no forward chaining, no multi-horizon validation.

**Solutions**:
- [ ] **Priority 1**: Implement walk-forward validation
  - Current: single train/val/test split
  - Needed: multiple expanding windows (origin-to-each-date forecasts)
  - Measures stability across different history lengths

- [ ] **Priority 2**: Add multi-horizon forecasting
  - Current: next-day only
  - Needed: 1-day, 3-day, 7-day, 14-day forecast horizons
  - Track accuracy degradation by horizon
  - Critical for booking curve modeling

- [ ] **Priority 3**: Add forecast skill score
  - Compare to naive baseline as skill = 1 - (MAE_model / MAE_baseline)
  - Negative skill = model worse than naive (flag for investigation)

**Files to Modify**:
- `predict_next_day.py:1129-1199` (_run_backtest)
- Add `tools/evaluate_forecast_skill.py`

**Implementation Priority**: Medium-High (improves confidence in production deployment)

---

### 1.4 Quantile Prediction Improvements
**Current**: 0.1, 0.5, 0.9 quantiles hardcoded

**Solutions**:
- [ ] Add adaptive quantile selection based on target volatility
  - High-volatility routes: use 0.05, 0.25, 0.5, 0.75, 0.95
  - Low-volatility routes: use 0.1, 0.5, 0.9

- [ ] Validate quantile calibration
  - Check if 10% of actuals fall below q0.1, 50% below q0.5, etc.
  - If miscalibrated, retrain with quantile loss function

**Implementation Priority**: Medium

---

## 2. Data Quality & Coverage Enhancements

### Current State
- **Schema**: 70+ columns in flight_offer_raw_meta
- **Change Detection**: Any column difference = change event
- **Sold-Out Logic**: All RBDs unavailable or explicit unavailability flag
- **Market Priors**: Labor markets, Middle East, tourism segments

### Critical Gaps

#### 2.1 Missing Value Handling
**Problem**: Median fill for numeric features, no strategy for categorical, no NaN imputation documentation.

**Solutions**:
- [ ] **Priority 1**: Implement robust imputation pipeline
  - Numeric: KNN imputation (use similar routes/dates)
  - Categorical: Mode or create "unknown" category
  - Time series: Forward fill then backward fill
  - Document strategy in `docs/DATA_QUALITY_STANDARDS.md`

- [ ] **Priority 2**: Add imputation quality metrics
  - Track % imputed per feature per route
  - Alert if imputation rate > 20% (data quality issue)

**Files to Modify**:
- `predict_next_day.py:391` (median fill logic)
- Add `core/imputation.py`

**Implementation Priority**: High (affects model training quality)

---

#### 2.2 Data Quality Checks
**Problem**: No pre-prediction validation, no freshness enforcement, no outlier detection.

**Solutions**:
- [ ] **Priority 1**: Add pre-prediction data quality gates
  - Check data freshness (last accumulation < 24h old)
  - Check completeness (mandatory columns > 95% populated)
  - Check distribution shift (detect sudden spikes in nulls)
  - Fail prediction if gates violated

- [ ] **Priority 2**: Implement outlier detection
  - IQR-based outlier flagging for prices (detect data entry errors)
  - Capacity outliers (seats > aircraft max capacity)
  - Temporal outliers (prices differ >5x from rolling mean)
  - Add `is_outlier` flag column, exclude from training

- [ ] **Priority 3**: Add data lineage tracking
  - Record source timestamp, connector version, cycle_id
  - Enable troubleshooting of bad predictions by tracing to source data quality

**New Files**:
- `tools/data_quality_gates.py`
- `core/outlier_detection.py`
- `docs/DATA_QUALITY_STANDARDS.md`

**Implementation Priority**: High (prevents garbage-in-garbage-out)

---

#### 2.3 Feature Consistency
**Problem**: Market priors only for some targets, inconsistent availability calculation, complex timezone handling.

**Solutions**:
- [ ] **Priority 1**: Standardize feature availability across all targets
  - Apply market priors to ALL targets (not just some)
  - Ensure LAG calculations consistent between DB queries and pandas
  - Add feature unit tests to validate consistency

- [ ] **Priority 2**: Simplify timezone handling
  - Current: 70+ airport mappings, manual updates
  - Add: Automated timezone lookup via pytz + airport IATA codes
  - Fallback: Use country-level timezone if airport-specific unavailable

- [ ] **Priority 3**: Add feature validation pipeline
  - Ensure no NaN/Inf in final feature matrix
  - Check feature ranges (e.g., days_to_departure always positive)
  - Log feature distribution stats per batch

**Files to Modify**:
- `predict_next_day.py:345-386` (_ml_feature_frame)
- `core/market_priors.py`
- `config/airport_timezones.json`

**Implementation Priority**: Medium

---

#### 2.4 Coverage Gaps
**Problem**: Seasonal events not captured, aircraft type not modeled, competitor pricing missing.

**Solutions**:
- [ ] **Priority 1**: Add holiday/calendar feature engineering
  - Bangladesh holidays: Eid, Pahela Baishakh, Victory Day, etc.
  - Regional holidays: Diwali, Chinese New Year, etc.
  - School vacation periods
  - Binary flags: is_holiday, days_to_holiday, days_after_holiday

- [ ] **Priority 2**: Add aircraft type features
  - Map equipment codes to aircraft type (narrowbody/widebody)
  - Add seating capacity, fuel efficiency, range features
  - These affect pricing (larger aircraft = lower seat-mile cost)

- [ ] **Priority 3**: Add competitive intelligence features (future)
  - Track competitor pricing on same route/date
  - Calculate price rank (is carrier cheapest/mid/expensive?)
  - Add market share estimates
  - **Note**: Requires multi-airline comparison tables not yet built

**New Files**:
- `config/holiday_calendar.json`
- `config/aircraft_characteristics.json`
- `core/holiday_features.py`
- `core/aircraft_features.py`

**Implementation Priority**: Medium-High (holidays critical for Bangladesh market)

---

## 3. System Reliability & Monitoring Enhancements

### Current State
- **Pipeline**: 9 orchestrated steps with fail-fast option
- **Monitoring**: Daily ops health, smoke checks, DB backup/restore tests
- **Scheduler**: Finish-driven launch with completion buffers

### Critical Gaps

#### 3.1 Error Recovery
**Problem**: No retry logic, no dead-letter queue, DB unavailable = skipped cycle.

**Solutions**:
- [ ] **Priority 1**: Implement exponential backoff retry
  - Transient DB errors: retry 3x with 2^n second delays
  - API rate limits: retry with longer backoff
  - Network timeouts: retry with circuit breaker pattern

- [ ] **Priority 2**: Add prediction failure handling
  - If prediction fails for route, use baseline fallback
  - Log failure reason + route metadata
  - Alert if failure rate > 10% of routes

- [ ] **Priority 3**: Implement cycle recovery logic
  - If cycle incomplete (PostgreSQL unavailable), mark for re-attempt
  - Add recovery window: re-attempt missed cycles within 6 hours
  - Track recovery success rate

**New Files**:
- `core/retry_policy.py`
- `tools/cycle_recovery.py`

**Implementation Priority**: High (improves uptime)

---

#### 3.2 Monitoring Gaps
**Problem**: No real-time prediction performance tracking, model drift detection incomplete, no data freshness alerts.

**Solutions**:
- [ ] **Priority 1**: Add real-time prediction monitoring dashboard
  - Track prediction MAE/RMSE by route/target/day
  - Alert if MAE degrades >20% from baseline
  - Visualize in `tools/build_operator_dashboard.py`

- [ ] **Priority 2**: Enhance model drift detection
  - Current: `tools/model_drift_monitor.py` exists
  - Add: PSI (Population Stability Index) for feature drift
  - Add: KL divergence for target distribution shift
  - Add: Prediction error trend analysis

- [ ] **Priority 3**: Add data freshness SLA enforcement
  - Alert if accumulation not completed in 6 hours
  - Alert if BigQuery sync stale > 12 hours
  - Track freshness metrics in ops dashboard

**Files to Modify**:
- `tools/model_drift_monitor.py`
- `tools/build_operator_dashboard.py`
- Add `tools/prediction_performance_monitor.py`

**Implementation Priority**: Medium-High (enables proactive issue detection)

---

#### 3.3 Resource Management
**Problem**: Single-threaded MLP, no GPU support, no parallel training for CatBoost/LightGBM.

**Solutions**:
- [ ] **Priority 1**: Enable parallel training
  - CatBoost: set `thread_count=-1` (use all cores)
  - LightGBM: set `n_jobs=-1`
  - Expected speedup: 2-4x on multi-core machines

- [ ] **Priority 2**: Add GPU acceleration (optional)
  - CatBoost supports GPU via `task_type='GPU'`
  - Requires CUDA, optional for thesis work
  - Expected speedup: 10-50x for large datasets

- [ ] **Priority 3**: Add memory profiling
  - Track peak memory usage during backtest
  - Alert if memory usage > 80% of available RAM
  - Implement batch processing for large route sets

**Files to Modify**:
- `predict_next_day.py:461-550` (model initialization)
- Add `--enable-gpu` flag to predict_next_day.py

**Implementation Priority**: Medium (nice-to-have performance boost)

---

#### 3.4 Timeout Handling
**Problem**: Query timeout 120s may be insufficient, no graceful degradation.

**Solutions**:
- [ ] Add configurable timeouts per pipeline step
  - Accumulation: 8 hours (current runtime observed)
  - Prediction: 30 minutes
  - BigQuery sync: 10 minutes

- [ ] Add graceful degradation
  - If prediction times out, use last successful prediction
  - Mark as stale but don't fail entire pipeline

**Implementation Priority**: Low (current 120s adequate for most queries)

---

## 4. Feature Engineering & Prediction Accuracy Enhancements

### Current Features
- **Lags**: 1, 2, 3, 7, 14 days
- **Rolling**: 3, 7, 14 day means + 7-day std
- **Differences**: lag1-lag2, lag1-lag7
- **EWM**: alpha=0.3
- **Temporal**: day_of_week, day_of_month
- **Departure**: days_to_departure
- **Market priors**: 10 columns (labor, Middle East, tourism, etc.)

### Critical Gaps

#### 4.1 Missing Features
**Problem**: No competitor pricing, no booking curve features, no aircraft type, no route characteristics.

**Solutions**:
- [ ] **Priority 1**: Add booking curve features
  - Days since departure (already have days_to_departure)
  - Add: booking_advance_window (e.g., 7-14d, 15-30d, 30-60d, 60+ buckets)
  - Add: is_peak_booking_window (typically 30-45 days before departure)
  - Expected lift: 10-15% for price_events prediction

- [ ] **Priority 2**: Add route characteristics
  - Route distance (km)
  - Route type: domestic/regional/long-haul
  - Hub-spoke indicator (is origin or destination a hub?)
  - Competition level: monopoly/duopoly/high-competition

- [ ] **Priority 3**: Add demand proxies
  - Historical load factor (if available)
  - Cumulative bookings to date (if available from source)
  - Search volume (if tracking implemented)

**New Files**:
- `core/booking_curve_features.py`
- `core/route_characteristics.py`
- `config/hub_airports.json`

**Implementation Priority**: High (booking curve critical for pricing prediction)

---

#### 4.2 Temporal Features
**Problem**: No holiday flags, no calendar adjustments, weekday effects not isolated.

**Solutions**:
- [ ] **Priority 1**: Add comprehensive holiday features
  - Binary: is_departure_holiday, is_search_holiday
  - Distance: days_to_next_holiday, days_since_last_holiday
  - Categorical: holiday_type (religious/national/school_vacation)
  - Critical for Bangladesh market (Eid travel surges)

- [ ] **Priority 2**: Add weekday interaction features
  - Current: day_of_week (0-6)
  - Add: is_weekend_departure, is_friday_departure
  - Add: weekday × days_to_departure interaction
  - (Weekend departures priced differently)

- [ ] **Priority 3**: Add seasonality decomposition
  - Trend, seasonal, residual components
  - Use STL decomposition or Fourier terms
  - Helps separate long-term trends from seasonal patterns

**Files to Modify**:
- `predict_next_day.py:345-386` (_ml_feature_frame)
- Add `core/temporal_features.py`

**Implementation Priority**: High (holidays drive major demand shifts)

---

#### 4.3 Target-Specific Issues
**Problem**: soldout_rate bounded [0,1] treated as regression, price targets ignore currency, availability missing zero-inflation.

**Solutions**:
- [ ] **Priority 1**: Use appropriate loss functions
  - soldout_rate: Beta regression or logit transform
  - Bounded targets (rates, proportions): use logit(y + epsilon)
  - Count targets (offers_count): Poisson or Negative Binomial

- [ ] **Priority 2**: Add currency normalization
  - Current: min_price_bdt assumes BDT
  - Add: Exchange rate lookup if multi-currency
  - Convert all to USD for cross-market comparison

- [ ] **Priority 3**: Handle zero-inflation in availability
  - avg_seat_available often zero (sold out)
  - Use zero-inflated model or two-stage prediction:
    1. Classify: sold out vs available
    2. Regress: if available, predict seat count

**Files to Modify**:
- `predict_next_day.py:461-550` (_build_ml_dl_predictions)
- Add `core/target_transforms.py`

**Implementation Priority**: Medium-High (improves accuracy for specific targets)

---

#### 4.4 Feature Scaling
**Problem**: Only MLP uses StandardScaler, CatBoost/LightGBM don't normalize.

**Solutions**:
- [ ] Add consistent feature scaling across all models
  - Tree models (CatBoost/LightGBM) don't require scaling but benefit from it
  - Apply StandardScaler or RobustScaler (outlier-resistant) to all
  - Save scaler with model for production inference

- [ ] Add feature importance tracking
  - CatBoost/LightGBM: use built-in feature_importances_
  - MLP: use permutation importance
  - Log top 10 features per target per route
  - Helps identify which features drive predictions

**Implementation Priority**: Low-Medium (minor accuracy improvement)

---

## 5. User Experience & Reporting Enhancements

### Current Outputs
- **Excel**: .xlsx + .xlsm workbooks with macro-enabled fare monitors
- **CSV**: Prediction outputs with backtest metadata
- **BigQuery**: 7-day rolling window export
- **API**: FastAPI scaffold (apps/api/)

### Critical Gaps

#### 5.1 Prediction Transparency
**Problem**: No feature importance, no SHAP values, no confidence metrics, no explainability.

**Solutions**:
- [ ] **Priority 1**: Add SHAP explainability
  - Compute SHAP values for each prediction
  - Export top 5 features driving each forecast
  - Add to prediction output CSV: `shap_feature_1`, `shap_value_1`, etc.
  - Expected benefit: RM team can trust/challenge predictions

- [ ] **Priority 2**: Add prediction confidence metrics
  - Quantile spread (q0.9 - q0.1) = uncertainty
  - Historical MAE for this route/target = expected error
  - Add `prediction_confidence` column: high/medium/low
  - Based on: model agreement, historical accuracy, data quality

- [ ] **Priority 3**: Add prediction change explanations
  - If forecast differs >10% from previous, explain why
  - Example: "Price forecast increased due to: 1) Holiday next week (+8%), 2) Capacity decreased (-3%)"
  - Store in `prediction_explanation` text column

**New Files**:
- `core/explainability.py`
- `core/confidence_metrics.py`

**Implementation Priority**: High (increases user trust and adoption)

---

#### 5.2 Report Quality
**Problem**: Backtest metadata stored but not visualized, no actual vs predicted comparison, no alert thresholds.

**Solutions**:
- [ ] **Priority 1**: Add actual vs predicted tracking report
  - For each prediction, track actual outcome when available
  - Generate daily report: predicted vs actual, % error
  - Visualize in Excel or web dashboard
  - Helps validate model in production

- [ ] **Priority 2**: Visualize backtest results
  - Current: CSV with metrics
  - Add: Time series plot of predictions vs actuals
  - Add: Residual distribution histogram
  - Add: Feature importance bar charts
  - Include in thesis pack

- [ ] **Priority 3**: Add alert thresholds
  - Define acceptable prediction error ranges per target
  - Alert if prediction MAE exceeds threshold
  - Alert if model confidence drops below threshold
  - Integrate with `tools/notify_ops_health.py`

**Files to Modify**:
- `generate_reports.py`
- Add `tools/prediction_validation_report.py`
- `tools/visualize_backtest.py`

**Implementation Priority**: Medium-High (improves operational transparency)

---

#### 5.3 API Completeness
**Problem**: Prediction API may not expose uncertainty, no real-time scoring endpoint, BigQuery fallback unclear.

**Solutions**:
- [ ] **Priority 1**: Add prediction API endpoints
  - GET /predictions/{route}/{date} - fetch predictions with quantiles
  - POST /predictions/score - real-time scoring for custom inputs
  - GET /predictions/{route}/explain - SHAP feature importance

- [ ] **Priority 2**: Document API fallback logic
  - Primary: BigQuery curated reads
  - Fallback: PostgreSQL local reads
  - Cache: Redis (optional)
  - Document in `apps/api/README.md`

- [ ] **Priority 3**: Add API rate limiting and auth
  - Protect against abuse
  - Add API key authentication
  - Rate limit: 100 requests/minute per key

**Files to Modify**:
- `apps/api/app/main.py`
- `apps/api/README.md`

**Implementation Priority**: Medium (for production web deployment)

---

## 6. Documentation & Maintainability Enhancements

### Current State
- **Good**: README, PROJECT_DECISIONS, OPERATIONS_RUNBOOK
- **Good**: 9 markdown docs in docs/ folder
- **Good**: Weekly ops checklist exists

### Critical Gaps

#### 6.1 Missing Documentation
**Problem**: No architecture diagram, no feature engineering rationale, no model selection explanation, no troubleshooting guide.

**Solutions**:
- [ ] **Priority 1**: Create architecture diagram
  - Data flow: Airlines → Connectors → PostgreSQL → ML/DL → BigQuery → API → Web
  - Component diagram: Modules, engines, tools, schedulers
  - Use Mermaid or draw.io
  - Include in README and docs/ARCHITECTURE.md

- [ ] **Priority 2**: Document feature engineering decisions
  - Create `docs/FEATURE_ENGINEERING_GUIDE.md`
  - For each feature, explain:
    - **Rationale**: Why this feature?
    - **Computation**: How calculated?
    - **Expected impact**: Which targets does it help?
  - Include examples and code snippets

- [ ] **Priority 3**: Add troubleshooting guide
  - Create `docs/TROUBLESHOOTING.md`
  - Common issues:
    - "Prediction failed for route X" → Check data quality, history length
    - "Model accuracy degraded" → Check data drift, retrain model
    - "BigQuery sync failed" → Check credentials, network
  - Include diagnostic commands and fixes

**New Files**:
- `docs/ARCHITECTURE.md` (with diagrams)
- `docs/FEATURE_ENGINEERING_GUIDE.md`
- `docs/TROUBLESHOOTING.md`

**Implementation Priority**: High (critical for knowledge transfer)

---

#### 6.2 Code Comments
**Problem**: Sparse docstrings, complex algorithms lack explanations, magic numbers unexplained.

**Solutions**:
- [ ] **Priority 1**: Add comprehensive docstrings
  - All public functions: Google-style docstrings
  - Include: Args, Returns, Raises, Examples
  - Focus on: predict_next_day.py, run_pipeline.py, run_all.py

- [ ] **Priority 2**: Document magic numbers
  - Current: 250 iterations, 0.05 lr, 14 min history
  - Add inline comments explaining why these values
  - Or move to `config/model_defaults.yaml` with comments

- [ ] **Priority 3**: Add algorithm explanations
  - Backtest logic: Explain rolling window approach
  - Feature engineering: Explain lag/rolling window rationale
  - Model selection: Explain MAE + coverage threshold logic

**Files to Modify**:
- All major Python files (add docstrings)
- Add `config/model_defaults.yaml`

**Implementation Priority**: Medium (improves maintainability)

---

#### 6.3 Runbook Gaps
**Problem**: OPERATIONS_RUNBOOK doesn't cover model retraining SLA, incident response, BigQuery failures.

**Solutions**:
- [ ] **Priority 1**: Add model operations section to runbook
  - Model retraining SLA: Daily for core training
  - Model validation: Check MAE < threshold before deploy
  - Model rollback: Revert to previous if performance degrades

- [ ] **Priority 2**: Add incident response playbook
  - Severity levels: P0 (no predictions), P1 (degraded), P2 (minor)
  - Escalation path: Who to notify?
  - Recovery procedures: How to restore service?

- [ ] **Priority 3**: Document BigQuery sync failure handling
  - Check credentials: `GOOGLE_APPLICATION_CREDENTIALS` valid?
  - Check quota: BigQuery API limits exceeded?
  - Manual sync: `tools/export_bigquery_stage.py`

**Files to Modify**:
- `OPERATIONS_RUNBOOK.md`
- Add `docs/INCIDENT_RESPONSE.md`

**Implementation Priority**: Medium (improves ops reliability)

---

## Quick-Win Improvements (Highest ROI)

These can be implemented in 1-2 days each with significant impact:

### 1. Add Model Feature Importance to Outputs
**Effort**: Low | **Impact**: High

- Modify `predict_next_day.py:550-600` to compute feature importance
- Export to `prediction_feature_importance_{target}_{timestamp}.csv`
- Include top 10 features per route in reports
- **Benefit**: RM team can understand and trust predictions

### 2. Implement Prediction Confidence Bands
**Effort**: Low | **Impact**: High

- Use quantile spread (q0.9 - q0.1) as uncertainty measure
- Use backtest MAE as expected error range
- Add `confidence` column: high (uncertainty < 10%), medium (10-25%), low (>25%)
- **Benefit**: Users know when to trust predictions

### 3. Add Holiday Feature Engineering
**Effort**: Medium | **Impact**: High

- Create `config/holiday_calendar.json` with Bangladesh holidays (Eid, Pohela Boishakh, etc.)
- Add binary flags: `is_holiday`, `days_to_holiday`, `is_holiday_week`
- Expected accuracy lift: 12-18% for price_events around holidays
- **Benefit**: Critical for Bangladesh market dynamics

### 4. Add Automated Hyperparameter Validation
**Effort**: Medium | **Impact**: Medium-High

- Use Optuna to tune CatBoost/LightGBM/MLP params
- Run on 3-5 high-volume routes initially
- Save best params in `config/model_config_overrides.json`
- **Benefit**: 8-15% accuracy improvement with minimal manual effort

### 5. Create Data Quality Pre-Prediction Gate
**Effort**: Low | **Impact**: High

- Check: Last accumulation < 24h old
- Check: Mandatory columns > 95% populated
- Check: No sudden null rate spikes
- Fail prediction if any gate violated
- **Benefit**: Prevents bad predictions from bad data

### 6. Document Feature Engineering Rationale
**Effort**: Low | **Impact**: Medium

- Create `docs/FEATURE_ENGINEERING_GUIDE.md`
- Explain each of 20+ features: why, how, expected impact
- Include code examples
- **Benefit**: Faster onboarding, easier debugging, better thesis documentation

---

## Implementation Priorities

### Phase 2A: Core Accuracy Improvements (Weeks 1-4)
**Goal**: Improve prediction accuracy by 15-20%

1. Holiday feature engineering (Quick Win #3)
2. Booking curve features (Section 4.1)
3. Feature importance outputs (Quick Win #1)
4. Prediction confidence bands (Quick Win #2)
5. Hyperparameter tuning (Quick Win #4)

### Phase 2B: Data Quality & Reliability (Weeks 5-8)
**Goal**: Reduce prediction failures by 50%

1. Data quality gates (Quick Win #5)
2. Robust imputation pipeline (Section 2.1)
3. Outlier detection (Section 2.2)
4. Exponential backoff retry (Section 3.1)
5. Real-time monitoring dashboard (Section 3.2)

### Phase 2C: User Experience & Transparency (Weeks 9-12)
**Goal**: Increase user trust and adoption

1. SHAP explainability (Section 5.1)
2. Actual vs predicted tracking (Section 5.2)
3. Architecture documentation (Section 6.1)
4. Feature engineering guide (Quick Win #6)
5. Troubleshooting guide (Section 6.1)

### Phase 2D: Advanced Features (Weeks 13-16)
**Goal**: Thesis-grade research quality

1. Transfer learning for sparse routes (Section 1.1)
2. Walk-forward validation (Section 1.3)
3. Multi-horizon forecasting (Section 1.3)
4. Route characteristics features (Section 4.1)
5. Zero-inflation handling (Section 4.3)

---

## Success Metrics

Track these KPIs to measure improvement:

### Model Performance
- **Baseline MAE**: Record current MAE per target
- **Target**: Reduce MAE by 15% in Phase 2A, 25% by Phase 2D
- **Metric**: Directional accuracy (F1_macro) > 0.70

### System Reliability
- **Baseline**: 95% uptime (estimated)
- **Target**: 99% uptime
- **Metric**: Prediction failure rate < 5%

### Data Quality
- **Baseline**: 10% imputation rate (estimated)
- **Target**: < 5% imputation rate
- **Metric**: Outlier detection rate < 2%

### User Adoption
- **Baseline**: RM team uses predictions informally
- **Target**: 80% of pricing decisions use predictions
- **Metric**: Track prediction API call volume

### Documentation Coverage
- **Baseline**: 60% of functions have docstrings (estimated)
- **Target**: 95% coverage
- **Metric**: Run pydocstyle to check

---

## Resource Requirements

### Development Time
- **Phase 2A**: 4 weeks × 1 FTE = 160 hours
- **Phase 2B**: 4 weeks × 1 FTE = 160 hours
- **Phase 2C**: 4 weeks × 1 FTE = 160 hours
- **Phase 2D**: 4 weeks × 1 FTE = 160 hours
- **Total**: 16 weeks (4 months) for full roadmap

### Compute Resources
- **Training**: Current laptop sufficient for Phase 2A-C
- **Hyperparameter Tuning**: May benefit from cloud compute (AWS/GCP)
- **GPU**: Optional, only needed for MLP acceleration (not critical)

### Storage
- **PostgreSQL**: Monitor growth, may need partitioning (Section 11 in PROJECT_DECISIONS)
- **BigQuery**: Sandbox sufficient, monitor quota

---

## Risk Mitigation

### Risk 1: Hyperparameter Tuning May Not Improve Accuracy
**Mitigation**: Validate on holdout routes first. If no improvement, skip and focus on feature engineering.

### Risk 2: Holiday Features May Be Market-Specific
**Mitigation**: Start with Bangladesh holidays only. Expand to regional holidays (India, Middle East) later.

### Risk 3: SHAP Computation May Be Slow
**Mitigation**: Compute SHAP only for key routes initially. Use TreeSHAP (fast) for tree models.

### Risk 4: Documentation Takes Time Away from Coding
**Mitigation**: Implement incrementally. Document each feature as you build it. Use AI assistance (Claude) for first drafts.

---

## Conclusion

This roadmap provides a clear path to make the Aviation Intelligence Platform **better and more effective** through:

1. **20-25% accuracy improvement** via better features, tuning, and data quality
2. **99% uptime** via robust error handling and monitoring
3. **80% user adoption** via transparency, explainability, and trust
4. **Thesis-grade quality** via comprehensive documentation and research rigor

**Recommended Start**: Implement Quick Wins (#1-6) first for immediate impact, then proceed with Phase 2A-D systematically.

**Review Cadence**: Monthly review of KPIs, adjust priorities based on what delivers most value.

---

**Next Steps**:
1. Review this roadmap with project owner and RM team
2. Prioritize which improvements are most critical for thesis timeline
3. Start with Quick Win #3 (Holiday Features) or #5 (Data Quality Gates)
4. Track progress using PROJECT_DECISIONS.md Section 11 checklist format

**Document Owner**: Aviation Intelligence Platform Team
**Last Reviewed**: 2026-03-18
**Next Review**: 2026-04-18
