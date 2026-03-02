#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$ROOT/logs" "$ROOT/output/reports"

PYEXE="$ROOT/.venv/bin/python"
LOGFILE="$ROOT/logs/maintenance_pulse.log"

timestamp() {
  date "+%Y-%m-%d %H:%M:%S"
}

if [[ ! -x "$PYEXE" ]]; then
  echo "[$(timestamp)] python exe not found: $PYEXE" >> "$LOGFILE"
  exit 1
fi

echo "[$(timestamp)] starting maintenance pulse" >> "$LOGFILE"
set +e
"$PYEXE" "$ROOT/scheduler/always_on_maintenance.py" \
  --python-exe "$PYEXE" \
  --reports-dir "$ROOT/output/reports" \
  --state-file "$ROOT/output/reports/always_on_maintenance_state.json" \
  --run-on-start \
  --once >> "$LOGFILE" 2>&1
RC=$?
set -e
echo "[$(timestamp)] maintenance pulse finished rc=$RC" >> "$LOGFILE"
exit "$RC"

