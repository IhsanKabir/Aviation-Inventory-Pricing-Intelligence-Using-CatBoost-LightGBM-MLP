# System Weakness Dependency Map

**Purpose:** Understand why we fix issues in a specific order and what blocks what

---

## Dependency Chain Diagram

```text
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│  START HERE:  Validation Framework (Critical)                   │
│  ============  - Reject bad data before insert                  │
│                - Enable identity_valid flag                     │
│                - Prevent garbage from DB                        │
│                  │                                              │
│                  └──→ BLOCKS EVERYTHING THAT FOLLOWS            │
│                       (No point fixing downstream if garbage    │
│                        keeps entering database)                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│  NEXT: Identity Tracking (Critical)                             │
│  =====  - Filter invalid records from comparison                │
│         - Track identity_valid flag through pipeline            │
│         - Report identity coverage metrics                      │
│         │                                                       │
│         └──→ UNBLOCKS: Comparison Engine Clean                  │
│              (Now diff only compares valid flights)             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

        ┌─────────────────────────────────┐
        │   CRITICAL PATH COMPLETE        │
        │ (Can now scrape reliably)       │
        └─────────────────────────────────┘
                      ↓
            (Split: 3 parallel teams)
                      ↓
        ┌─────────────────────────────────┐

┌─────────────────────────────────────┐
│  TRACK A: Airline Parsers (High)    │
│  =========  3 parallel workstreams: │
│                                     │
│  A1. Audit + Complete US Bangla     │
│  A2. Audit + Complete Novo Air      │
│  A3. Create timezone_helper         │
│                                     │
│  Dependencies:                      │
│  - Needs Codex API docs or network  │
│    inspection                       │
│  - timezone_helper MUST complete    │
│    before parser fixes              │
│                                     │
│  Unlocks: Scraping for 5+ airlines  │
│           (vs current 2)            │
└─────────────────────────────────────┘
            │
            └──→ Feeds into validation ✓ (already done)
                So: Scrape → Validate → Insert

┌─────────────────────────────────────┐
│  TRACK B: Session Management (High) │
│  =========                          │
│                                     │
│  B1. Create SessionManager class    │
│  B2. Implement load_cookies()       │
│  B3. Implement 401 auto-refresh     │
│  B4. Consolidate cookies files      │
│  B5. Update all airlines            │
│                                     │
│  Dependencies: None (independent)   │
│                                     │
│  Unlocks: Auto-recovery from auth   │
│           failures; fewer manual    │
│           cookie refreshes          │
└─────────────────────────────────────┘
            │
            └──→ 401 errors trigger retry loop
                (currently silent failure)

┌─────────────────────────────────────┐
│  TRACK C: Timezone Handling (High)  │
│  =========                          │
│                                     │
│  C1. Create timezone_helper         │ ← SHARED with Track A
│  C2. Update all parsers to use it   │
│  C3. Standardize to UTC + local     │
│  C4. Update comparison engine       │
│  C5. Update DB schema               │
│                                     │
│  Dependencies:                      │
│  - Needs timezone_helper complete   │
│    (do with A3)                    │
│  - Must be done BEFORE Track C2     │
│                                     │
│  Unlocks: Correct change detection  │
│           Correct report times      │
└─────────────────────────────────────┘
            │
            └──→ All timestamps UTC in DB
                Comparison engine compares UTC
                Reports show local times

        ┌─────────────────────────────────┐
        │   HIGH PRIORITY COMPLETE        │
        │ (Can run full pipeline)         │
        └─────────────────────────────────┘
                      ↓
            (Split: 2 parallel teams)
                      ↓

┌─────────────────────────────────────┐
│  TRACK D: Data Quality Gates (Med)  │
│  =========                          │
│                                     │
│  D1. Define SLA thresholds in JSON  │
│  D2. Create data_quality_evaluator  │
│  D3. Auto-compute metrics           │
│  D4. Add DQ gates to run_all.py     │
│  D5. Abort/warn on violations       │
│                                     │
│  Dependencies:                      │
│  - NEEDS validation ✓ working       │
│  - NEEDS identity_valid ✓ working   │
│  - Works with comparison engine ✓   │
│                                     │
│  Unlocks: Early warning of bad data │
│           Automated quality checks  │
└─────────────────────────────────────┘
            │
            └──→ Reports show DQ metrics
                Run aborts if thresholds violated

┌─────────────────────────────────────┐
│  TRACK E: Prediction Integration    │
│  =========  (Med)                   │
│                                     │
│  E1. Add --run-prediction flag      │
│  E2. Wire predict_next_day.py       │
│  E3. Create forecast_evaluator      │
│  E4. Add outputs to reports         │
│                                     │
│  Dependencies:                      │
│  - NEEDS validation ✓ working       │
│  - NEEDS good historical data       │
│    (requires 3-7 days of clean     │
│     scrapes from earlier phases)    │
│  - DB schema must be stable         │
│                                     │
│  Unlocks: ML forecasts in daily     │
│           reports; drift detection  │
└─────────────────────────────────────┘
            │
            └──→ Daily: predictions for next day

        ┌─────────────────────────────────┐
        │   MEDIUM PRIORITY COMPLETE      │
        │ (Full intelligence system)      │
        └─────────────────────────────────┘
                      ↓
                  Last Phase:
                      ↓

┌─────────────────────────────────────┐
│  TRACK F: Testing + Hardening       │
│  =========  (Low)                   │
│                                     │
│  F1. Create E2E test               │
│  F2. Add edge case tests            │
│  F3. Set up GitHub Actions          │
│  F4. Achieve 80%+ coverage          │
│                                     │
│  Dependencies:                      │
│  - ALL previous work must be ✓      │
│  - Tests are regression protection  │
│                                     │
│  Unlocks: Confidence in future      │
│           changes; safe refactoring │
└─────────────────────────────────────┘
            │
            └──→ CI/CD runs pytest on every push

        ┌─────────────────────────────────┐
        │   PROJECT COMPLETE              │
        │  & THESIS-READY                 │
        └─────────────────────────────────┘
```

---

## Critical Path vs. Optional Path

```text
CRITICAL PATH (Must do in order, 24-48 hours):
  Validation (1) → Identity (2) → Airline Parsers (3)
                                    ↓
                            (All other work blocked until)
                                    ↓
                            Session Mgmt (4) + Timezone (7)
                                    ↓
              Ready for 3-7 day test run → Prediction (6)
                                    ↓
                          Data Quality Gates (5)
                                    ↓
                              Testing (8)

OPTIONAL BUT RECOMMENDED:
  Data Quality Gates earlier if already have 3+ days of good data
  Testing can run in parallel during test runs

TIME ESTIMATE:
  Validation (1):        2 hours  ⏱
  Identity (2):          1 hour   ⏱
  Subtotal: 3 hours      (0.5 days)

  Airline Parsers (3):  14 hours  ⏱⏱⏱⏱
  Session Mgmt (4):     3 hours   ⏱⏱
  Timezone (7):         2 hours   ⏱
  Subtotal: 19 hours    (2.5 days)

  Data Quality (5):     4 hours   ⏱
  Prediction (6):       3 hours   ⏱
  Subtotal: 7 hours     (1 day)

  Testing (8):          8 hours   ⏱⏱

  TOTAL: 37 hours ≈ 4.5 days (if 1 person serial)
                 ≈ 2-3 days (if 2-3 people parallel)
```

---

## What Breaks Without Each Piece

```text
❌ Missing: Validation Framework
   Impact: Garbage data enters DB
   Symptom: Silent failures, false signals in reports
   Recovery: Delete bad rows, rerun analysis
   Cost: 3-5 days of wasted investigation

❌ Missing: Identity Tracking
   Impact: Can't filter invalid records from diffs
   Symptom: Change events on already-bad flights
   Recovery: Re-run comparison with identity filter
   Cost: Noisy reports for 1-2 weeks

❌ Missing: Airline Parsers
   Impact: Only 2/10 airlines scraping
   Symptom: Incomplete competitor monitoring
   Recovery: Manual API reverse-engineering (days)
   Cost: Missing critical pricing signals

❌ Missing: Session Management
   Impact: 30% of scrapes fail due to expired cookies
   Symptom: Manual cookie refresh every 3-7 days
   Recovery: Operator has to manually refresh
   Cost: ~2 hours/week of manual work

❌ Missing: Timezone Handling
   Impact: Change detection compares times at wrong offsets
   Symptom: False inventory changes (invisible bug)
   Recovery: Recalculate all change events with UTC
   Cost: 1-2 days of re-analysis

❌ Missing: Data Quality Gates
   Impact: Garbage silently pollutes reports
   Symptom: Operators question data reliability
   Recovery: Forensic investigation per report
   Cost: Loss of confidence in system

❌ Missing: Prediction Integration
   Impact: ML infrastructure unused
   Symptom: Expensive feature sits dormant
   Recovery: Full re-integration after system stable
   Cost: Delayed forecast capability by weeks

❌ Missing: Testing
   Impact: Any change breaks something
   Symptom: Frequent emergency patches
   Recovery: Manual regression testing
   Cost: Slow development velocity
```

---

## Recommendation: 2-Person Team, 4 Days

```text
PERSON A (Parsers + Timezone):
  ├─ Day 1:  Complete Phase 1 TOGETHER (Validation + Identity)
  ├─ Day 2:  Airline parser audit + timezone_helper
  ├─ Day 3:  Complete US Bangla + Novo Air parsers
  ├─ Day 4:  Update all parsers + timezone fixes
  └─ Day 5:  Testing + documentation

PERSON B (Session + Quality + Prediction):
  ├─ Day 1:  Complete Phase 1 TOGETHER (Validation + Identity)
  ├─ Day 2:  SessionManager design + implementation
  ├─ Day 3:  SessionManager testing + airline integration
  ├─ Day 4:  Data Quality Gates + Prediction wiring
  └─ Day 5:  Testing + CI/CD setup

DAILY SYNC (15 min):
  - Share blockers
  - Verify Phase 1 completion before splitting
  - Ensure timezone_helper is done before both teams need it
```

---

## Key Success Metrics Per Phase

```text
Phase 1: Validation + Identity
  ✅ Zero rows with missing identity cols inserted
  ✅ Zero negative prices in DB
  ✅ Rejection report created every run
  ✅ 100% of unit tests passing

Phase 2: Airline Parsers + Session + Timezone
  ✅ 5+ airlines scraping successfully
  ✅ No > 0.1ms timezone offset errors
  ✅ 401 errors trigger auto-refresh (mock test)
  ✅ Session files consolidated in cookies/ folder

Phase 3: Quality + Prediction
  ✅ DQ report auto-generated with PASS/WARN/FAIL
  ✅ 3-7 days of clean historical data in DB
  ✅ Forecast outputs saved every run
  ✅ MAE/MAPE metrics computed

Phase 4: Testing
  ✅ E2E test covers scrape → report → predict
  ✅ 80%+ code coverage on critical modules
  ✅ GitHub Actions pytest passing on every commit
```

---

## Decision Tree: Where to Start?

```text
Q: Do you have API docs for US Bangla / Novo Air?
├─ YES → Start with Phase 1, then Parsers
└─ NO  → Start with Phase 1, plan API reverse-engineering

Q: How many people working on this?
├─ 1 person  → Phase 1 → Parsers → Session (serial, 5 days)
├─ 2-3 people → Phase 1 → Split tracks (parallel, 3 days)
└─ 4+ people → Phase 1 → Full parallelization (parallel, 2 days)

Q: Most painful problem TODAY?
├─ "Data is garbage" → Phase 1 (Validation)
├─ "Cookies expire" → Phase 2 Session (but Phase 1 first!)
├─ "Wrong timestamps in reports" → Phase 2 Timezone (but Phase 1 first!)
└─ "Don't know if data is good" → Phase 3 Quality (after 1-2)

Q: Can the system afford downtime?
├─ NO (production) → Phase 1 only (validation + identity)
│                   Then run 3 days validation-only
│                   Then carefully roll out other phases
└─ YES (prototype) → Full parallel attack, 3 days

```

---

## Rollback/Safe Failures Per Phase

```text
Phase 1 (Validation): ✅ SUPER SAFE
  - Validator only REJECTS rows; doesn't modify anything
  - Can disable instantly: just set all rows as identity_valid=true
  - Zero risk of data loss

Phase 2 (Parsers): ⚠️ MEDIUM RISK
  - If parser breaks, just disable airline in config.json
  - Old rows in DB still valid
  - Rollback: revert parser change, re-enable airline

Phase 2 (Session): ✅ SAFE
  - SessionManager can fall back to reload_state()
  - Old session files still exist as backup
  - Rollback: use old session mechanism

Phase 2 (Timezone): ⚠️ CAREFUL
  - Can't easily roll back timestamps already stored
  - Run on fresh DB schema first (test environment)
  - Validate on 1 day before rolling to production

Phase 3 (Quality + Prediction): ✅ SAFE
  - These are passive (monitoring + logging)
  - Don't modify core pipeline
  - Can disable instantly

Phase 4 (Testing): ✅ SAFE
  - Tests don't modify production
  - Can skip E2E tests if necessary
  - Only risk is to development velocity
```

---

## Final Recommendation

**Start with Phase 1 (Validation) TODAY.**

Why?

- ✅ Shortest (2 hours)
- ✅ Safest (zero risk)
- ✅ Biggest impact (prevents garbage)
- ✅ Unblocks everything else
- ✅ Can start immediately (no dependencies)

**Then decide:**
- If parsers are your bottleneck → Phase 2 Parsers next
- If sessions/cookies are your bottleneck → Phase 2 Session next
- If time is tight → Just do Phases 1-2, skip 3-4
- If you have 1 week → Everything (phases 1-4)

**DO NOT SKIP Phase 1.** It's the foundation.
