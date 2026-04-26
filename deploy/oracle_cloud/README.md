# Oracle Cloud Always Free VM Migration (Ubuntu)

This package migrates the project to an **Oracle Cloud Always Free Compute VM** while keeping the
current accumulation/recovery workflow and route-gated ML operations unchanged.

## Use the right Oracle Cloud category

- Start in **Compute** (Always Free VM Instance)
- Networking is configured during VM creation (VCN/subnet/security rules)
- Oracle Databases / AI & Machine Learning categories are **not** the right entry point for this stack

Current stack hosted on the VM:
- Python accumulation + reporting + probe workflows
- PostgreSQL (local on the VM)
- systemd timers for recurring accumulation and maintenance pulse

## Recommended VM choice

- **Ampere A1 (ARM)** Always Free if available (more free CPU/RAM)
- Ubuntu image (22.04 or 24.04)

## Minimal networking/security rules

- Inbound:
  - `22/tcp` (SSH) from your IP only
- Do **not** expose PostgreSQL (`5432`) publicly
- Application runs locally; reports/artifacts are files on disk

## Migration steps (high level)

1. Create VM in **Compute**
2. SSH into VM
3. Clone this repo
4. Run bootstrap script
5. Configure `.env`
6. Install/start systemd timers
7. Verify accumulation runtime

## 1) Bootstrap the VM

From repo root on the VM:

```bash
chmod +x deploy/oracle_cloud/bootstrap_ubuntu_oracle_free.sh
./deploy/oracle_cloud/bootstrap_ubuntu_oracle_free.sh
```

Optional ML extras (`catboost`, `lightgbm`) can be attempted later; core accumulation does not require them.

## 2) Configure environment

Edit `.env` (created from `.env.example` if missing):

```bash
nano .env
```

Recommended VM-local defaults:
- `DB_HOST=localhost`
- `DB_PORT=5432`
- `DB_NAME=Playwright_API_Calling`
- `DB_USER=airlineintel`
- `DB_PASSWORD=<strong password>`

## 3) Install systemd timers (ingestion + maintenance pulse)

```bash
chmod +x deploy/oracle_cloud/install_systemd_services.sh
./deploy/oracle_cloud/install_systemd_services.sh
```

This installs:
- `airlineintel-ingestion4h.timer` (09:30, 13:30, 17:30, 21:30 local VM time)
- `airlineintel-maintenance-pulse.timer` (every 30 minutes)

Services call:
- `scheduler/run_ingestion_4h_once.sh`
- `scheduler/run_maintenance_pulse_once.sh`

## 4) Verify timers and runtime

```bash
systemctl status airlineintel-ingestion4h.timer --no-pager
systemctl status airlineintel-maintenance-pulse.timer --no-pager
tail -n 50 logs/ingestion_4h.log
tail -n 50 logs/maintenance_pulse.log
```

## Sleep / shutdown behavior (VM context)

VMs do not have laptop sleep mode issues, which is the main reason this migration improves reliability.
If the VM reboots:
- systemd timers resume automatically
- the maintenance pulse can trigger accumulation recovery checks

## Notes on ARM / package compatibility

- `scikit-learn` usually works on ARM with wheels or source build (slower install)
- `catboost` / `lightgbm` may be harder on ARM and are **optional** for current operations
- Start with core accumulation + probes first; add optional ML backends later if needed

## Operational continuity rules (unchanged)

- Keep route-selection gate fixed: `beats_zero_folds`
- Continue weekly model-ops workflow from the current project context and operations runbook.
- Re-run comparative policy study only on trigger conditions
