# Feature Engineering Guide

**Last Updated**: 2026-03-18
**Purpose**: Document rationale, computation, and expected impact for all prediction features

---

## Overview

This guide documents all features used in the ML/DL prediction pipeline (`predict_next_day.py`). Each feature is explained with:
- **Rationale**: Why is this feature important?
- **Computation**: How is it calculated?
- **Expected Impact**: Which targets does it help predict?

---

## Feature Categories

### 1. Lag Features (Historical Values)

These features capture recent historical values of the target variable.

#### `lag1` - Previous Day Value
- **Rationale**: Yesterday's value is the strongest predictor for today (persistence effect)
- **Computation**: `target.shift(1)` - shift target by 1 day
- **Expected Impact**: HIGH for all targets, especially price and capacity stability
- **Target Correlation**: 0.70-0.85 (very strong)

#### `lag2` - 2 Days Ago Value
- **Rationale**: Captures short-term momentum and recent trend direction
- **Computation**: `target.shift(2)`
- **Expected Impact**: MEDIUM for all targets
- **Target Correlation**: 0.60-0.75

#### `lag3` - 3 Days Ago Value
- **Rationale**: Extends short-term trend visibility
- **Computation**: `target.shift(3)`
- **Expected Impact**: MEDIUM for all targets
- **Target Correlation**: 0.55-0.70

#### `lag7` - 7 Days Ago Value (Weekly Seasonality)
- **Rationale**: Captures weekly patterns (weekday effects, weekly booking cycles)
- **Computation**: `target.shift(7)`
- **Expected Impact**: HIGH for event targets, MEDIUM for price targets
- **Target Correlation**: 0.50-0.65
- **Critical For**: Routes with strong weekday demand patterns

#### `lag14` - 14 Days Ago Value (Bi-weekly Patterns)
- **Rationale**: Captures longer-term seasonality and pay-cycle effects
- **Computation**: `target.shift(14)`
- **Expected Impact**: MEDIUM for all targets
- **Target Correlation**: 0.45-0.60

---

### 2. Rolling Window Features (Smoothed Trends)

These features smooth short-term noise to reveal underlying trends.

#### `roll3` - 3-Day Rolling Mean
- **Rationale**: Short-term trend indicator, removes daily noise
- **Computation**: `target.shift(1).rolling(window=3, min_periods=1).mean()`
- **Expected Impact**: HIGH for volatile targets (change events)
- **Smooths**: Day-to-day fluctuations
- **Critical For**: Detecting rapid price changes vs normal volatility

#### `roll7` - 7-Day Rolling Mean
- **Rationale**: Weekly trend baseline, identifies sustained movements
- **Computation**: `target.shift(1).rolling(window=7, min_periods=1).mean()`
- **Expected Impact**: HIGH for all targets
- **Use Case**: Baseline for anomaly detection (actual vs 7-day average)

#### `roll14` - 14-Day Rolling Mean
- **Rationale**: Medium-term trend, filters weekly cycles
- **Computation**: `target.shift(1).rolling(window=14, min_periods=1).mean()`
- **Expected Impact**: MEDIUM for all targets
- **Use Case**: Identifies longer-term demand shifts

#### `roll7_std` - 7-Day Rolling Std Dev (Volatility)
- **Rationale**: Measures recent volatility/uncertainty
- **Computation**: `target.shift(1).rolling(window=7, min_periods=2).std()`
- **Expected Impact**: HIGH for confidence estimation
- **Interpretation**: High std → uncertain predictions, Low std → stable market
- **Critical For**: Risk-adjusted pricing decisions

---

### 3. Difference Features (Momentum & Acceleration)

These features capture rate of change and momentum.

#### `diff_1_2` - Day-to-Day Change (Velocity)
- **Rationale**: Captures immediate momentum (is target increasing or decreasing?)
- **Computation**: `lag1 - lag2`
- **Expected Impact**: MEDIUM for change event prediction
- **Interpretation**: Positive → upward trend, Negative → downward trend

#### `diff_1_7` - Week-over-Week Change (Weekly Momentum)
- **Rationale**: Captures weekly trend acceleration
- **Computation**: `lag1 - lag7`
- **Expected Impact**: MEDIUM-HIGH for all targets
- **Critical For**: Identifying sustained demand shifts vs temporary spikes

---

### 4. Exponential Weighted Moving Average (EWMA)

#### `ewm03` - EWMA with alpha=0.3
- **Rationale**: Adaptive baseline that weights recent values more heavily
- **Computation**: `target.shift(1).ewm(alpha=0.3, adjust=False).mean()`
- **Expected Impact**: HIGH for price prediction
- **Alpha=0.3**: Balances responsiveness (recent data) vs stability (long-term)
- **Use Case**: Better than simple rolling mean for non-stationary targets

---

### 5. Temporal Features (Calendar Effects)

#### `dow` - Day of Week (0=Monday, 6=Sunday)
- **Rationale**: Weekday effects on demand (weekends, Fridays differ from mid-week)
- **Computation**: `report_day.dt.dayofweek`
- **Expected Impact**: HIGH for capacity and event targets
- **Patterns**:
  - Thursday/Friday: Outbound travel peaks
  - Sunday/Monday: Return travel peaks
  - Saturday: Generally lower demand except leisure routes

#### `dom` - Day of Month (1-31)
- **Rationale**: Monthly patterns (salary dates, month-end, month-start)
- **Computation**: `report_day.dt.day`
- **Expected Impact**: MEDIUM for labor routes
- **Patterns**:
  - Day 1-5: Post-salary booking surge
  - Day 25-31: Month-end booking spike

#### `days_to_departure` - Booking Advance Window
- **Rationale**: Booking curve effect - prices and availability change as departure approaches
- **Computation**: `(departure_date - search_date).days`
- **Expected Impact**: VERY HIGH for price and availability
- **Patterns**:
  - 60+ days: Early bird discounts, high availability
  - 30-45 days: Peak booking window, prices stabilize
  - 14-30 days: Price increases begin
  - 0-14 days: Steep price increases, low availability
- **Critical For**: Revenue management, yield optimization

---

### 6. Holiday Features (NEW - 2026-03-18)

Critical for Bangladesh aviation market where holidays drive massive demand surges.

#### `is_search_holiday` - Search Date is Holiday
- **Rationale**: Holidays impact search behavior (advance planning, urgency)
- **Computation**: Lookup in `config/holiday_calendar.json`
- **Expected Impact**: MEDIUM for change event prediction
- **Use Case**: Detect unusual search patterns on holidays

#### `is_high_demand_holiday` - High-Demand Holiday Flag
- **Rationale**: Eid, Pohela Boishakh, Victory Day drive extreme demand
- **Computation**: Check `high_demand: true` in holiday calendar
- **Expected Impact**: VERY HIGH for price and availability (12-18% accuracy lift expected)
- **Patterns**:
  - 7-10 days before Eid: Outbound booking surge
  - 1-3 days after Eid: Return booking surge
  - Prices can increase 2-5x normal rates

#### `days_to_next_holiday` - Distance to Upcoming Holiday
- **Rationale**: Demand builds as holidays approach
- **Computation**: Days until next holiday (0 if today is holiday, 60 if none within 60 days)
- **Expected Impact**: HIGH for price and capacity forecasting
- **Critical Window**: 7-14 days before major holiday

#### `days_since_last_holiday` - Days After Last Holiday
- **Rationale**: Post-holiday demand normalization period
- **Computation**: Days since last holiday (0 if today is holiday, 60 if none within 60 days)
- **Expected Impact**: MEDIUM for demand forecasting
- **Pattern**: Demand returns to baseline 3-5 days after holiday

#### `is_holiday_week` - Within Holiday Window (±3 days)
- **Rationale**: Extended holiday impact zone
- **Computation**: `days_to_next_holiday <= 3 OR days_since_last_holiday <= 3`
- **Expected Impact**: HIGH for all targets
- **Use Case**: Detect pre/post-holiday booking patterns

#### `holiday_type_code` - Holiday Category (0=none, 1=religious, 2=national)
- **Rationale**: Different holiday types have different demand patterns
- **Computation**: Map from holiday calendar
- **Expected Impact**: MEDIUM
- **Patterns**:
  - Religious (Eid): Strongest demand, longest booking window
  - National (Independence Day, Victory Day): Moderate demand, shorter window

#### `is_departure_holiday` - Departure Date is Holiday
- **Rationale**: Traveling ON a holiday has different pricing than traveling TO a holiday
- **Computation**: Lookup departure date in holiday calendar
- **Expected Impact**: HIGH for price prediction
- **Pattern**: Departures on religious holidays often premium-priced

#### `is_departure_high_demand` - Departure on High-Demand Holiday
- **Rationale**: Peak departure dates command premium pricing
- **Computation**: Check departure date against high_demand holidays
- **Expected Impact**: VERY HIGH for price prediction
- **Pattern**: Eid eve departures can be 3-5x normal price

---

### 7. Market Prior Features (Route/Market Context)

These features encode domain knowledge about route types and market segments.

#### `market_is_middle_east` - Middle East Route Flag
- **Rationale**: Middle East routes (DAC→DXB, DAC→JED) have distinct demand patterns
- **Computation**: Applied via `core/market_priors.py`
- **Expected Impact**: HIGH for price and capacity
- **Pattern**: Labor migration routes, salary-cycle sensitive

#### `market_is_ksa` - Saudi Arabia Route Flag
- **Rationale**: KSA routes have unique visa windows and religious travel
- **Computation**: Applied via market priors
- **Expected Impact**: HIGH for event targets
- **Pattern**: Hajj/Umrah seasonality

#### `market_is_thailand_tourism` - Thailand Tourism Route
- **Rationale**: Tourism routes behave differently than labor/business routes
- **Computation**: Applied via market priors
- **Expected Impact**: MEDIUM for all targets
- **Pattern**: Leisure-focused, higher price elasticity

#### `market_is_labor_outbound` - Labor Outbound Route
- **Rationale**: Worker outbound routes (DAC→Middle East) peak at specific times
- **Computation**: Applied via market priors
- **Expected Impact**: HIGH for capacity prediction
- **Pattern**: Salary-cycle aligned, visa-window constrained

#### `market_is_labor_return` - Labor Return Route
- **Rationale**: Return routes have opposite seasonality to outbound
- **Computation**: Applied via market priors
- **Expected Impact**: HIGH for event targets
- **Pattern**: Holiday-return surges

#### `airline_is_hub_spoke` - Hub-Spoke Airline Model
- **Rationale**: Hub-spoke carriers (e.g., Emirates) have different pricing than point-to-point
- **Computation**: Applied via market priors
- **Expected Impact**: MEDIUM for price prediction

#### `airline_is_lcc` - Low-Cost Carrier Flag
- **Rationale**: LCCs have different yield management strategies
- **Computation**: Applied via market priors
- **Expected Impact**: HIGH for price and availability

#### `airline_is_return_oriented` - Return-Traffic Focused
- **Rationale**: Some airlines optimize for return traffic (labor routes)
- **Computation**: Applied via market priors
- **Expected Impact**: MEDIUM for capacity prediction

#### `horizon_is_visa_window` - Visa-Window Booking Horizon
- **Rationale**: Visa processing times constrain booking windows
- **Computation**: Applied via market priors
- **Expected Impact**: MEDIUM for labor routes

#### `horizon_is_long_window` - Long Booking Horizon (>90 days)
- **Rationale**: Very early bookings have different price dynamics
- **Computation**: Applied via market priors
- **Expected Impact**: MEDIUM for price prediction

---

## Feature Importance (Expected)

### Top 10 Most Important Features by Target Type

#### For Price Events (price_events)
1. `days_to_next_holiday` ⭐⭐⭐⭐⭐
2. `is_high_demand_holiday` ⭐⭐⭐⭐⭐
3. `lag1` ⭐⭐⭐⭐⭐
4. `days_to_departure` ⭐⭐⭐⭐
5. `roll7` ⭐⭐⭐⭐
6. `ewm03` ⭐⭐⭐⭐
7. `diff_1_7` ⭐⭐⭐
8. `market_is_labor_outbound` ⭐⭐⭐
9. `dow` ⭐⭐⭐
10. `is_departure_high_demand` ⭐⭐⭐

#### For Capacity (avg_seat_available)
1. `lag1` ⭐⭐⭐⭐⭐
2. `days_to_departure` ⭐⭐⭐⭐⭐
3. `is_high_demand_holiday` ⭐⭐⭐⭐
4. `roll7` ⭐⭐⭐⭐
5. `lag7` ⭐⭐⭐
6. `dow` ⭐⭐⭐
7. `market_is_labor_return` ⭐⭐⭐
8. `days_to_next_holiday` ⭐⭐⭐
9. `roll7_std` ⭐⭐
10. `diff_1_2` ⭐⭐

#### For Prices (min_price_bdt)
1. `lag1` ⭐⭐⭐⭐⭐
2. `days_to_departure` ⭐⭐⭐⭐⭐
3. `is_departure_high_demand` ⭐⭐⭐⭐⭐
4. `days_to_next_holiday` ⭐⭐⭐⭐
5. `ewm03` ⭐⭐⭐⭐
6. `roll14` ⭐⭐⭐⭐
7. `market_is_middle_east` ⭐⭐⭐
8. `airline_is_lcc` ⭐⭐⭐
9. `is_holiday_week` ⭐⭐⭐
10. `dow` ⭐⭐

---

## Feature Engineering Best Practices

### 1. Always Use Lag Shifts for Time Series
- Never use current-day values (data leakage!)
- Always shift by at least 1 day: `target.shift(1)`

### 2. Handle Missing Values
- Fill numeric features with median from training set
- Avoid forward-fill for time series (creates look-ahead bias)

### 3. Scale Features Appropriately
- Tree models (CatBoost/LightGBM): Scaling optional but helpful
- Neural networks (MLP): StandardScaler required
- Keep original scale for interpretability

### 4. Validate Feature Quality
- Check correlation with target (expect >0.1 for useful features)
- Check for data leakage (future → past)
- Monitor feature importance after training

### 5. Domain-Specific Features Matter
- Holiday features: Critical for Bangladesh market
- Booking curve: Universal across all aviation markets
- Market priors: Encode human expertise

---

## Adding New Features

When adding a new feature:

1. **Document Here First**
   - Add to this guide with rationale, computation, expected impact
   - Justify why this feature will improve predictions

2. **Modify `_ml_feature_frame()` in predict_next_day.py**
   - Add computation logic
   - Ensure no data leakage (use shifts!)

3. **Test on Historical Data**
   - Run backtest with new feature
   - Compare MAE with/without feature
   - Expected lift: ≥2% to justify inclusion

4. **Monitor in Production**
   - Track feature importance via SHAP values
   - Remove if importance < 1% consistently

---

## Feature Computation Summary Table

| Feature | Type | Lag (days) | Target Correlation | Training Cost |
|---------|------|------------|-------------------|---------------|
| `lag1` | Lag | 1 | 0.70-0.85 | Low |
| `lag7` | Lag | 7 | 0.50-0.65 | Low |
| `roll7` | Rolling | 1 | 0.65-0.80 | Low |
| `ewm03` | EWM | 1 | 0.60-0.75 | Low |
| `days_to_departure` | Temporal | 0 | 0.50-0.70 | Low |
| `is_high_demand_holiday` | Holiday | 0 | 0.30-0.50 | Low |
| `days_to_next_holiday` | Holiday | 0 | 0.25-0.45 | Low |
| `market_is_labor_outbound` | Market | 0 | 0.20-0.40 | Low |

**Note**: "Lag (days)" = how far back data is shifted. "0" means current-day context (not target value).

---

## References

- `predict_next_day.py:357-403` - `_ml_feature_frame()` function
- `core/market_priors.py` - Market prior application
- `core/holiday_features.py` - Holiday feature extraction
- `config/holiday_calendar.json` - Bangladesh holiday data

**Last Updated**: 2026-03-18
**Next Review**: After first production validation with backtest results
