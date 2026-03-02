# Weakness Assessment & Actionable Remediation Plan

**Last Updated:** 2026-02-23
**Assessment Scope:** Complete codebase review of airline scraper system

---

## EXECUTIVE SUMMARY

The project has **solid architectural foundations** (70% complete) but suffers from **critical operational gaps** in 5 key areas:

1. **Data Validation** (CRITICAL) - Missing input validation + identity tracking
2. **Airline Parser Inconsistency** (HIGH) - Empty implementations + timezone/soldout gaps
3. **Session Management** (HIGH) - Multiple disconnected cookie/session systems
4. **Data Quality Reporting** (MEDIUM) - Framework exists but no automated quality gates
5. **Prediction Integration** (MEDIUM) - Advanced ML framework exists but disconnected

---

## 🔴 CRITICAL ISSUES (Fix Within 24 Hours)

### 1. DATA VALIDATION - No Pre-Insert Validation Framework

**Current State:**
- `run_all.py` has `is_valid_core_offer()` check but it only validates field existence, not values
- No validation of airline codes, date formats, prices (negative?), seat counts
- No rejection of malformed records before DB insertion
- Field type mismatches not caught (e.g., string in numeric field)

**Impact:**
- Garbage data pollutes database → breaks reporting/predictions
- Silent failures (no error logging)
- Identity key conflicts go undetected

**Actionable Steps:**

| # | Task | Time | File(s) |
|---|------|------|---------|
| 1a | Create `validation/flight_offer_validator.py` with strict schema | 30min | NEW |
| 1b | Add `validate_identity_key()` function checking all identity cols | 20min | NEW |
| 1c | Add `validate_price_sanity()` (reject negative/zero prices, outliers) | 20min | NEW |
| 1d | Integrate validator into `db.py::bulk_insert_offers()` | 15min | `db.py` |
| 1e | Add validation logging + rejection report to `run_all.py` | 20min | `run_all.py` |

**Success Criteria:**
- ✅ No rows with missing identity cols inserted
- ✅ No negative/zero prices inserted
- ✅ Daily rejection report in `output/validation_rejections_YYYYMMDD.json`
- ✅ Pytest passes: `pytest validation/test_flight_offer_validator.py`

---

### 2. IDENTITY TRACKING - `identity_valid` Flag Never Checked

**Current State:**
- `identity_valid` column exists in models but is never set or used
- Comparison engine has no guard against invalid identity records
- No way to exclude bad records from change detection

**Impact:**
- Invalid records producing spurious change events
- Reports include noise from malformed flights
- Forecast models training on garbage data

**Actionable Steps:**

| # | Task | Time | File(s) |
|---|------|------|---------|
| 2a | Update validator to set `identity_valid=True/False` on each row | 15min | `validation/flight_offer_validator.py` |
| 2b | Add `identity_valid` guard to `comparison_engine.py::_index()` | 10min | `comparison_engine.py` |
| 2c | Add filtering to `generate_reports.py::_fetch_data_quality_report()` | 15min | `generate_reports.py` |
| 2d | Unit test: verify invalid records excluded from diffs | 15min | `tests/test_identity_validation.py` |

**Success Criteria:**
- ✅ `comparison_engine.compare()` filters out `identity_valid=False`
- ✅ Data quality report shows `identity_valid` rates per airline
- ✅ No invalid records in change events

---

## 🟠 HIGH PRIORITY (Fix Within 48 Hours)

### 3. AIRLINE PARSERS - Incomplete/Empty Implementations

**Current State:**
- `airlines/us-bangla.py` - **EMPTY**
- `airlines/novoair.py` - **EMPTY**
- `airlines/emirates.py`, `qatar.py`, `singapore.py`, `malaysia.py` - Status unclear
- No timezone handling across any parser
- No soldout logic (capacity comparison)

**Impact:**
- 50% of airlines can't scrape → incomplete competitor monitoring
- Manual data only for disabled airlines
- Missing crucial pricing signals

**Actionable Steps:**

| # | Task | Time | File(s) |
|---|------|------|---------|
| 3a | Audit each airline module: empty vs. partial vs. complete | 2h | All `airlines/*.py` |
| 3b | For US Bangla: reverse-engineer API/GraphQL from Codex work | 4h | `airlines/us-bangla.py` |
| 3c | For Novo Air: reverse-engineer API from Codex work | 4h | `airlines/novoair.py` |
| 3d | Implement `get_timezone_offset(airport_code)` helper | 1h | NEW: `airlines/timezone_helper.py` |
| 3e | Add soldout detection: `seat_available=0 OR all_rbds_sold` for each parser | 1h each | `airlines/*.py` (update all) |
| 3f | Add tests for each airline with fixture responses | 2h | `tests/fixtures/` + `tests/test_*_parser.py` |

**Success Criteria:**
- ✅ 100% of top-5 airlines have working parsers
- ✅ Each parser includes timezone-aware departure/arrival times
- ✅ Soldout logic: `seat_available == 0 OR all_booking_classes_sold_out`
- ✅ Config file specifies airline status: `"complete"` vs `"stub"`
- ✅ Pytest: `pytest airlines/ tests/` passes 100%

---

### 4. SESSION MANAGEMENT - Fragmented Cookie Systems

**Current State:**
- `state.json` for Playwright cookies (manual capture via `refresh_cookies.py`)
- `cookies.json` exists but unused in main flow
- Multiple session paths in modules (inconsistent)
- No automatic refresh on 401/403
- Manual Cloudflare bypass required

**Impact:**
- Frequent scrape failures due to expired sessions
- Operator has to manually refresh cookies every 3-7 days
- No centralized session diagnostics

**Actionable Steps:**

| # | Task | Time | File(s) |
|---|------|------|---------|
| 4a | Create `core/session_manager.py` with unified cookie interface | 1h | NEW |
| 4b | Implement `SessionManager.load_cookies(airline)` | 30min | NEW |
| 4c | Implement auto-refresh on HTTP 401 with retry loop (max 2 attempts) | 1h | NEW |
| 4d | Consolidate state.json → single `cookies/{airline}_cookies.json` per airline | 1h | NEW |
| 4e | Add session lifetime validation (warn if > 7 days old) | 30min | NEW |
| 4f | Update all airline modules to use SessionManager | 1h | All `airlines/*.py` |
| 4g | Test: mock 401 response → verify auto-refresh + retry | 30min | `tests/test_session_manager.py` |

**Success Criteria:**
- ✅ Single session file per airline in `cookies/`
- ✅ `SessionManager.load_cookies()` returns valid cookies
- ✅ 401 responses trigger auto-refresh + 1 retry
- ✅ Session age logged on startup
- ✅ Manual refresh still possible via `refresh_cookies.py`

---

## 🟡 MEDIUM PRIORITY (Fix Within 72 Hours)

### 5. DATA QUALITY REPORTING - Framework Exists, No Automated Gates

**Current State:**
- `_fetch_data_quality_report()` exists in `generate_reports.py`
- Collects metrics but no pass/fail criteria
- No automated rejection thresholds
- Operators must manually review CSVs

**Impact:**
- No early warning of data degradation
- Garbage data silently enters reports
- Can't trigger alerts on SLA violations

**Actionable Steps:**

| # | Task | Time | File(s) |
|---|------|------|---------|
| 5a | Define DQ thresholds in `config/data_quality_gates.json` | 30min | NEW |
| 5b | Implement `engines/data_quality_evaluator.py` with automated pass/fail | 1h | NEW |
| 5c | Add metrics: null rates, duplicate rates, outlier rates per airline | 1.5h | `generate_reports.py` |
| 5d | Auto-generate DQ report + summary status (`PASS/WARN/FAIL`) | 1h | `generate_reports.py` |
| 5e | Hook into `run_all.py` to abort scrape on FAIL, warn on WARN | 30min | `run_all.py` |
| 5f | Unit test: verify thresholds trigger correctly | 30min | `tests/test_data_quality_gates.py` |

**Success Criteria:**
- ✅ DQ report auto-generated every run
- ✅ Thresholds enforced (e.g., <5% null rate, <1% duplicates)
- ✅ Run aborts with warning if FAIL
- ✅ Operators receive DQ summary email/dashboard
- ✅ Output: `output/reports/data_quality_YYYYMMDD_HHMMSS.json`

---

### 6. PREDICTION INTEGRATION - Advanced Features Disconnected

**Current State:**
- `predict_next_day.py` is feature-complete (1183 lines) with CatBoost/LightGBM
- NOT integrated into main `run_all.py` pipeline
- Manual invocation only
- No forecast evaluation or drift monitoring

**Impact:**
- Expensive ML infrastructure unused
- Forecasts not part of daily reporting
- Can't detect model degradation

**Actionable Steps:**

| # | Task | Time | File(s) |
|---|------|------|---------|
| 6a | Add `--run-prediction` flag to `run_all.py` (disabled by default) | 30min | `run_all.py` |
| 6b | Integrate `predict_next_day.py::main()` call at pipeline end | 1h | `run_all.py` |
| 6c | Capture forecast outputs in `output/reports/predictions/` | 30min | `run_all.py` + `predict_next_day.py` |
| 6d | Create `engines/forecast_evaluator.py` for MAE/MAPE metrics | 1h | NEW |
| 6e | Add forecast outputs to `hourly_change_report_*.xlsx` (optional sheet) | 1h | `generate_reports.py` |
| 6f | Document: when to enable/disable prediction in OPERATIONS_RUNBOOK.md | 30min | `OPERATIONS_RUNBOOK.md` |

**Success Criteria:**
- ✅ `--run-prediction` flag works in `run_all.py`
- ✅ Predictions saved to `output/reports/predictions/YYYYMMDD_HHMMSS/`
- ✅ Forecast evaluation metrics (MAE, MAPE) computed
- ✅ No 401/403 errors from DB during prediction phase
- ✅ Documentation updated in runbook

---

### 7. TIMEZONE HANDLING - Inconsistent Across Parsers

**Current State:**
- Each airline has different timestamp formats (ISO8601, custom formats)
- `departure_local` vs `departure_utc` not consistently computed
- Airport timezone mapping exists (`config/airport_timezones.json`) but not used
- Comparison engine assumes UTC

**Impact:**
- Change detection may compare times at different offsets
- Reports show wrong local times
- Forecasts trained on misaligned timestamps

**Actionable Steps:**

| # | Task | Time | File(s) |
|---|------|------|---------|
| 7a | Create `airlines/timezone_helper.py` with `apply_timezone_offsets()` | 1h | NEW |
| 7b | Standardize output: all parsers return `departure_utc` + `departure_local` | 1.5h | All `airlines/*.py` |
| 7c | Update schema: ensure `departure`, `arrival` stored as UTC in DB | 1h | `models.py` + migrations |
| 7d | Update comparison engine to always compare UTC timestamps | 30min | `comparison_engine.py` |
| 7e | Test: verify DAC/CXB/JSD flights compute correct local times | 30min | `tests/test_timezone_handling.py` |

**Success Criteria:**
- ✅ All parsers return ISO8601 UTC timestamps + local times
- ✅ DB stores only UTC in `departure`/`arrival` columns
- ✅ Reports display local times correctly per airport
- ✅ Change events compare UTC times

---

## 🟢 LOW PRIORITY (Fix Within Week)

### 8. TEST COVERAGE - Partial Unit Tests, No E2E

**Current State:**
- 9 test files exist
- Mostly unit tests (connector contracts, parser specs)
- No E2E pipeline tests
- No integration tests for comparison_engine + alerting chain

**Actionable Steps:**

| # | Task | Time | File(s) |
|---|------|------|---------|
| 8a | Create `tests/test_e2e_pipeline.py` with mock scrape → report flow | 2h | NEW |
| 8b | Add tests for invalid/edge-case data handling | 1h | `tests/` |
| 8c | Add CI/CD config (GitHub Actions) to run pytest on push | 1h | `.github/workflows/pytest.yml` |
| 8d | Achieve 80%+ line coverage on critical files | 2h | All modules |

---

## 🔵 REFERENCE CHECKLIST

Use this to track remediation progress:

```text
DATA VALIDATION (Critical)
  ☐ 1a: validation/flight_offer_validator.py created
  ☐ 1b: validate_identity_key() implemented
  ☐ 1c: validate_price_sanity() implemented
  ☐ 1d: Validator integrated into bulk_insert_offers()
  ☐ 1e: Rejection reporting added to run_all.py

IDENTITY TRACKING (Critical)
  ☐ 2a: identity_valid flag set by validator
  ☐ 2b: comparison_engine filters invalid records
  ☐ 2c: generate_reports includes identity_valid metrics
  ☐ 2d: Unit test passing

AIRLINE PARSERS (High)
  ☐ 3a: Audit complete (status doc)
  ☐ 3b: US Bangla parser complete
  ☐ 3c: Novo Air parser complete
  ☐ 3d: timezone_helper.py created
  ☐ 3e: Soldout logic added to all parsers
  ☐ 3f: Fixture tests 100% passing

SESSION MANAGEMENT (High)
  ☐ 4a: SessionManager created
  ☐ 4b: load_cookies() working
  ☐ 4c: Auto-refresh on 401 implemented
  ☐ 4d: Cookies consolidated to cookies/ folder
  ☐ 4e: Session age validation added
  ☐ 4f: All airlines use SessionManager
  ☐ 4g: Test passing

DATA QUALITY (Medium)
  ☐ 5a: DQ gates config created
  ☐ 5b: data_quality_evaluator.py created
  ☐ 5c: Metrics computed (null, duplicate, outlier rates)
  ☐ 5d: Auto report generation working
  ☐ 5e: run_all.py respects DQ gates
  ☐ 5f: Test passing

PREDICTION INTEGRATION (Medium)
  ☐ 6a: --run-prediction flag added
  ☐ 6b: predict_next_day.py integrated
  ☐ 6c: Outputs saved to reports/predictions/
  ☐ 6d: forecast_evaluator.py created
  ☐ 6e: Predictions in Excel reports
  ☐ 6f: Runbook updated

TIMEZONE HANDLING (Medium)
  ☐ 7a: timezone_helper.py created
  ☐ 7b: All parsers return UTC + local times
  ☐ 7c: Schema standardized to UTC
  ☐ 7d: Comparison engine uses UTC
  ☐ 7e: Timezone tests passing

TEST COVERAGE (Low)
  ☐ 8a: E2E pipeline test created
  ☐ 8b: Edge case tests added
  ☐ 8c: CI/CD config added
  ☐ 8d: 80%+ coverage achieved
```

---

## 📋 RECOMMENDED EXECUTION ORDER

**Phase 1 (Stabilization) - Day 1-2:**
1. **Data Validation** (1a-1e) - Prevent bad data from entering system
2. **Identity Tracking** (2a-2d) - Enable filtering of bad records
3. **Airline Parser Audit** (3a) - Understand what's broken

**Phase 2 (Operationalization) - Day 2-3:**
4. **Airline Parser Fixes** (3b-3f) - Get all 5+ airlines scraping
5. **Session Management** (4a-4g) - Reduce manual intervention
6. **Timezone Handling** (7a-7e) - Fix time comparisons

**Phase 3 (Intelligence) - Day 3-4:**
7. **Data Quality Gates** (5a-5f) - Automated quality monitoring
8. **Prediction Integration** (6a-6f) - Enable forecasting

**Phase 4 (Polish) - Day 5:**
9. **Test Coverage** (8a-8d) - CI/CD + regression safety

---

## 💾 ESTIMATED EFFORT

| Category | Effort | Days |
|----------|--------|------|
| Critical (1-2) | 3-4 hours | 0.5 |
| High (3-4, 7) | 12-16 hours | 1.5-2 |
| Medium (5-6) | 8-10 hours | 1-1.5 |
| Low (8) | 6-8 hours | 1 |
| **TOTAL** | **30-40 hours** | **4-5 days** |

---

## 🎯 SUCCESS DEFINITION

After completing this plan, the system will:

✅ **Reject invalid data before inserting** (no garbage in DB)
✅ **Track identity validity** (can filter bad records from analysis)
✅ **Support 5+ airlines** with complete parsers
✅ **Auto-manage sessions** (minimal manual intervention)
✅ **Monitor data quality** (automated pass/fail gates)
✅ **Generate timezone-correct reports** (local times display correctly)
✅ **Integrate forecasting** (ML models part of standard pipeline)
✅ **Have regression protection** (E2E tests on CI/CD)

---

## 📌 NEXT STEP

Choose one and start:

- **Urgent?** → Start with Phase 1 (Validation + Identity)
- **Want working airlines?** → Start with Phase 2 (Parsers)
- **Want monitoring first?** → Start with Quality Gates (Phase 3)
