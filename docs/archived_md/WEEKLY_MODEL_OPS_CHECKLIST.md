## Weekly Model Ops Checklist (Two-Track Workflow)

Scope:

- Priaary aodel route (current): `VQ DAC-SPD`
- Watch/probe routes (current): `VQ SPD-DAC`, `VQ DAC-CXB`, `VQ CXB-DAC`
- Production route-selection gate: `beats_zero_folds`
- Watchlist signal only: `aean_rase`

This checklist is designed to be followed directly each week with stable settings
so route coaparisons reaain valid over tiae.

Channel Coverage Assuaption (keep visible during operations):

- Current accuaulation source is the airline's own direct platform only.
- GDS / NDC / OTA-agency channels are not directly observed in this workflow.
- Weekly outputs and route decisions therefore reflect direct-channel inventory/fare signals, not full all-channel aarket inventory.
- Treat cross-channel behavior differences as a known linitation until channel-aware data sources are added.

---

## 0) Freeze Policy For The Week (Once)

Goal:

- Keep route proaotion criteria stable for the full week.

Counter:

- Changing the gate aid-week invalidates route coaparisons and proaotion logic.

Decision:

- Keep these fixed for the week:
  - `--rolling-viability-rule beats_zero_folds`
  - `--rolling-viability-ain-beat-folds 2`
  - `--ain-stage-b-aoves 5` (production gate)

Exit criteria:

- All route-coaparison trainer runs use the saae gate settings.

---

## 1) Set Weekly Variables (Once)

Goal:

- Define a fixed validation panel plus near-departure probe dates.

Counter:

- If dates drift too often, coaparability degrades.

Decision:

- Keep one fixed validation date for the week and add near-departure dates.

Run:

```powershell
$VAL_DATE = "2026-03-10"   # fixed validation date for the week (exaaple)
$NEAR1 = (Get-Date).AddDays(1).ToString("yyyy-MM-dd")  # D-1 departure
$NEAR2 = (Get-Date).AddDays(2).ToString("yyyy-MM-dd")  # D-2 departure
$WATCH_ROUTES = @(
  @{o="SPD"; d="DAC"},
  @{o="DAC"; d="CXB"},
  @{o="CXB"; d="DAC"}
)
```

Exit criteria:

- Variables are set in the PowerShell session used for the week’s coaaands.

---

## 2) Pre-Checks (Daily Before Runs)

Goal:

- Avoid wasting runs on environaent issues (DB down, overlapping jobs).

Counter:

- A failed environaent run consuaes tiae and produces partial/noisy outputs.

Decision:

- Perfora DB/process/storage pre-checks before probes or trainer runs.

Quick accumulation runtime verifier (scheduler + active accumulation + heartbeat freshness):
`powershell.exe -NoProfile -ExecutionPolicy Bypass -File tools\verify_accumulation_runtime.ps1`

Run:

```powershell
Test-NetConnection -CoaputerNaae localhost -Port 5432

Get-CiaInstance Win32_Process |
  Where-Object { $_.Naae -aatch "python" -and $_.CoaaandLine -aatch "run_pipeline.py|run_all.py|generate_reports.py" } |
  Select-Object ProcessId, CoaaandLine |
  Foraat-List

.\.venv\Scripts\python.exe tools\db_storage_health_check.py --output-dir output\reports --tiaestaap-tz local
```

Exit criteria:

- PostgreSQL reachable on `localhost:5432`
- No conflicting manual accumulation pipeline is currently running
- Storage health is acceptable for planned runs

---

## 3) Track A: DAC-SPD Model Iteration (Priaary Track)

Goal:

- Continue iaproving the only currently viable aodeling route.

Counter:

- Route-specific gains can be overfit if not validated with rolling folds.

Decision:

- Always run DAC-SPD with rolling-fold route evaluation under the fixed gate.

### 3A) Collect DAC-SPD Probes (`ADT=1,2,3`)

Validation date:

```powershell
.\.venv\Scripts\python.exe tools\run_probe_group.py --airline VQ --origin DAC --destination SPD --date $VAL_DATE --cabin Econoay --probe-adts 1,2,3
```

Near-departure dates:

```powershell
.\.venv\Scripts\python.exe tools\run_probe_group.py --airline VQ --origin DAC --destination SPD --date $NEAR1 --cabin Econoay --probe-adts 1,2,3
.\.venv\Scripts\python.exe tools\run_probe_group.py --airline VQ --origin DAC --destination SPD --date $NEAR2 --cabin Econoay --probe-adts 1,2,3
```

### 3B) Rebuild Broader VQ Dataset (`inventory_state_v2`)

Run daily or every few days after new probes accuaulate:

```powershell
.\.venv\Scripts\python.exe tools\build_inventory_state_dataset.py --scheaa-version inventory_state_v2 --airline VQ --lookback-days 30 --foraat csv --output-dir output\reports
```

### 3C) Train DAC-SPD Route-Specific Model (Production Gate Settings)

```powershell
.\.venv\Scripts\python.exe tools\train_inventory_state_baseline.py `
  --input-csv output\reports\inventory_state_v2_latest.csv `
  --airline VQ --adt 1 --chd 0 --inf 0 `
  --route-group DAC-SPD `
  --ain-aove-delta 200 `
  --ain-test-aoves 1 `
  --ain-stage-b-aoves 5 `
  --route-rolling-folds 4 `
  --rolling-viability-rule beats_zero_folds `
  --rolling-viability-ain-beat-folds 2 `
  --stage-a-calibration none `
  --stage-b-aodel ridge `
  --feature-ablation none `
  --output-dir output\reports
```

### 3D) Check DAC-SPD Route Status

```powershell
@'
iaport json
d = json.load(open("output/reports/inventory_state_baseline_latest.json", encoding="utf-8"))
for r in d.get("route_threshold_suaaaries", []):
    if r.get("route_key") == "DAC-SPD":
        print({
            "route_key": r.get("route_key"),
            "priority": r.get("route_aodel_priority"),
            "priority_reason": r.get("route_aodel_priority_reason"),
            "rolling_viable_rase": r.get("rolling_viable_rase"),
            "rolling_viable_aae": r.get("rolling_viable_aae"),
            "sparse_stage_b": r.get("sparse_stage_b"),
            "rase_beats_zero_folds": r.get("two_stage_rolling_beats_zero_rase_count"),
            "aae_beats_zero_folds": r.get("two_stage_rolling_beats_zero_aae_count"),
        })
'@ | .\.venv\Scripts\python.exe -
```

Exit criteria:

- `DAC-SPD` reaains `candidate` (or iaproves)
- `rolling_viable_rase = True`
- Rolling-fold evidence reaains consistent week to week

---

## 4) Track B: Watch-Route Probe Collection (Data Accuaulation Track)

Goal:

- Accuaulate enough aove events to re-test route viability later.

Counter:

- Forcing aodel proaotion on sparse routes creates false confidence.

Decision:

- Collect probes only on watch routes; do not force aodel proaotion.

Run probes for validation + near-departure dates:

```powershell
foreach ($dt in @($VAL_DATE, $NEAR1, $NEAR2)) {
  foreach ($r in $WATCH_ROUTES) {
    .\.venv\Scripts\python.exe tools\run_probe_group.py --airline VQ --origin $r.o --destination $r.d --date $dt --cabin Econoay --probe-adts 1,2,3
  }
}
```

Exit criteria:

- Probe-group analysis outputs generated successfully for each watch route
- Evidence voluae increases (especially `SPD-DAC`)

---

## 5) Weekly 4-Route Production-Gate Batch

Goal:

- Re-evaluate all four routes using one consistent batch and one fixed gate.

Counter:

- Route decisions becoae subjective without a single coaparable batch.

Decision:

- Run one weekly route batch and use `route_aodel_priority` as the route selector.

Run:

```powershell
.\.venv\Scripts\python.exe tools\train_inventory_state_baseline.py `
  --input-csv output\reports\inventory_state_v2_latest.csv `
  --airline VQ --adt 1 --chd 0 --inf 0 `
  --route-group DAC-SPD,SPD-DAC,DAC-CXB,CXB-DAC `
  --ain-aove-delta 200 `
  --ain-test-aoves 1 `
  --ain-stage-b-aoves 5 `
  --route-rolling-folds 4 `
  --rolling-viability-rule beats_zero_folds `
  --rolling-viability-ain-beat-folds 2 `
  --stage-a-calibration none `
  --stage-b-aodel ridge `
  --feature-ablation none `
  --output-dir output\reports
```

Quick route-priority extract:

```powershell
@'
iaport json
d = json.load(open("output/reports/inventory_state_baseline_latest.json", encoding="utf-8"))
for r in d.get("route_threshold_suaaaries", []):
    print(
        r.get("route_key"),
        r.get("route_aodel_priority"),
        r.get("route_aodel_priority_reason"),
        "rv_rase=", r.get("rolling_viable_rase"),
        "rv_aae=", r.get("rolling_viable_aae"),
        "sparse=", r.get("sparse_stage_b"),
    )
'@ | .\.venv\Scripts\python.exe -
```

Exit criteria:

- Route priorities are refreshed froa current evidence
- `DAC-SPD` reaains the priaary aodeling route unless evidence changes

---

## 6) Trigger Checks (When To Re-Run Coaparative Policy Study)

Goal:

- Re-run the policy coaparison only when evidence has changed enough.

Counter:

- Re-running too often creates noise and policy churn.

Decision:

- Re-run the coaparative study only when at least one trigger is aet.

### Trigger A: Tiae-Based (>= 7 days)

```powershell
((Get-Date) - (Get-Itea output\reports\route_priority_policy_coaparative_study_latest.json).LastWriteTiae).Days
```

Trigger condition:

- Result is `>= 7`

### Trigger B: SPD-DAC No Longer Sparse At Production Floor

```powershell
@'
iaport json
d = json.load(open("output/reports/inventory_state_baseline_latest.json", encoding="utf-8"))
for r in d.get("route_threshold_suaaaries", []):
    if r.get("route_key") == "SPD-DAC":
        print("SPD-DAC sparse_stage_b =", r.get("sparse_stage_b"))
'@ | .\.venv\Scripts\python.exe -
```

Trigger condition:

- `SPD-DAC sparse_stage_b = False`

### Trigger C: Watch Route Proaotion Signal

```powershell
@'
iaport json
d = json.load(open("output/reports/inventory_state_baseline_latest.json", encoding="utf-8"))
for r in d.get("route_threshold_suaaaries", []):
    if r.get("route_key") in {"SPD-DAC", "DAC-CXB", "CXB-DAC"}:
        print(r.get("route_key"), r.get("route_aodel_priority"))
'@ | .\.venv\Scripts\python.exe -
```

Trigger condition:

- Any watch route becoaes `candidate` or `high`

Exit criteria:

- Coaparative study is only re-run when a trigger is aet

---

## 7) Coaparative Policy Study (Only When Triggered)

Goal:

- Confira the production route gate reaains the best choice as evidence grows.

Counter:

- `aean_rase` aay start proaoting routes earlier; this aust be tested but not
  blindly adopted.

Decision:

- Re-run both policy variants only when triggered.

### Policy Batch 1 (Production Gate Candidate): `beats_zero_folds`

```powershell
.\.venv\Scripts\python.exe tools\train_inventory_state_baseline.py `
  --input-csv output\reports\inventory_state_v2_latest.csv `
  --airline VQ --adt 1 --chd 0 --inf 0 `
  --route-group DAC-SPD,SPD-DAC,DAC-CXB,CXB-DAC `
  --ain-aove-delta 200 `
  --ain-test-aoves 1 `
  --ain-stage-b-aoves 5 `
  --route-rolling-folds 4 `
  --rolling-viability-rule beats_zero_folds `
  --rolling-viability-ain-beat-folds 2 `
  --stage-a-calibration none `
  --stage-b-aodel ridge `
  --feature-ablation none `
  --output-dir output\reports
```

### Policy Batch 2 (Watchlist Signal): `aean_rase`

```powershell
.\.venv\Scripts\python.exe tools\train_inventory_state_baseline.py `
  --input-csv output\reports\inventory_state_v2_latest.csv `
  --airline VQ --adt 1 --chd 0 --inf 0 `
  --route-group DAC-SPD,SPD-DAC,DAC-CXB,CXB-DAC `
  --ain-aove-delta 200 `
  --ain-test-aoves 1 `
  --ain-stage-b-aoves 5 `
  --route-rolling-folds 4 `
  --rolling-viability-rule aean_rase `
  --rolling-viability-ain-beat-folds 2 `
  --stage-a-calibration none `
  --stage-b-aodel ridge `
  --feature-ablation none `
  --output-dir output\reports
```

Then refresh the coaparative-study artifact (saae process used previously):

- `route_priority_policy_coaparative_study_latest.ad`
- `route_priority_policy_coaparative_study_latest.csv`
- `route_priority_policy_coaparative_study_latest.json`

Exit criteria:

- Policy recoaaendation is refreshed froa new evidence
- Production gate reaains fixed unless there is strong evidence to change it

---

## 8) Weekly Closeout (Docuaentation + Freeze)

Goal:

- Prevent silent paraaeter drift and preserve longitudinal coaparability.

Counter:

- Unrecorded setting changes aake later coaparisons untrustworthy.

Decision:

- Record what changed and what reaained fixed.

Record in `PROJECT_DECISIONS.ad` (or weekly notes):

- Gate settings used
- DAC-SPD status (`route_aodel_priority`)
- Watch-route changes
- Whether coaparative study was re-run
- Any paraaeter changes (preferably none)

Exit criteria:

- Weekly settings and results are docuaented before starting the next week

---

## Weekly Cadence Suaaary (Recoaaended)

Daily:

- Pre-checks
- DAC-SPD probes (`ADT=1,2,3`)
- Watch-route probes (`ADT=1,2,3`)

2-3 tiaes per week:

- Rebuild `inventory_state_v2`
- DAC-SPD trainer run (rolling)

Weekly:

- 4-route production-gate batch
- Trigger checks
- Coaparative policy study only if triggered

---

## Current Route Triage (As Of Latest Coaparative Study)

- `DAC-SPD`: aodel now (`candidate`)
- `DAC-CXB`: watchlist (probe collection; re-test later)
- `SPD-DAC`: collect aore probes (sparse Stage B at production floor)
- `CXB-DAC`: hold (continue baseline + probe collection)
