# Step 4 Review Plan (3-7 Day Window)

Created: 2026-02-21

## Purpose

After continuous collection runs for 3-7 days, run this checklist to tune:
- report layout quality
- data quality thresholds
- prediction baseline quality

## Review Dates

- Day 3 review target: 2026-02-24
- Day 7 review target: 2026-02-28

## Checklist

1. Confirm scheduler continuity
- Check BG/VQ logs:
```powershell
Get-Content logs\scheduler_bg.out.log -Tail 80
Get-Content logs\scheduler_vq.out.log -Tail 80
```

2. Confirm run folder health
- Verify one `run_*` directory every 4 hours and expected report files exist.

3. Evaluate data quality
- Open latest `data_quality_report_*.csv`.
- Threshold guidance:
  - `duplicate_row_rate_pct` < 1.0
  - `raw_meta_coverage_pct` >= 95
  - core null rates near 0 for identity and price fields

4. Evaluate route monitor structure
- Open latest `route_flight_fare_monitor_*.xlsx`.
- Verify:
  - route sections are complete
  - flight blocks are aligned
  - min/max fare + seat/load columns are populated

5. Evaluate prediction baseline
- Compare these files in latest run period:
  - `prediction_eval_total_change_events_*.csv`
  - `prediction_eval_by_route_total_change_events_*.csv`
- Keep best baseline by lowest MAE/sMAPE.

6. Tune and lock
- If needed, tune:
  - route filters
  - scrape interval
  - report grouping
  - prediction target and window
- Record final tuning decisions in `PROJECT_DECISIONS.md`.

## Optional Helper Command

Use this to run reports only (no scrape) for quick review:
```powershell
.\.venv\Scripts\python.exe run_pipeline.py --skip-scrape --report-format both --report-timestamp-tz local --route-monitor --run-prediction
```
