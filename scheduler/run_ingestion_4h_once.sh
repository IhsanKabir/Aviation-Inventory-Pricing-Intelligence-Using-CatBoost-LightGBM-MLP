#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$ROOT/logs" "$ROOT/output/reports"

PYEXE="$ROOT/.venv/bin/python"
LOGFILE="$ROOT/logs/ingestion_4h.log"
RECOVERY_HELPER="$ROOT/tools/recover_interrupted_accumulation.py"
RECOVERY_STATUS="$ROOT/output/reports/accumulation_recovery_latest.json"
CYCLE_STATE="$ROOT/output/reports/accumulation_cycle_latest.json"
ENVFILE="$ROOT/.env"

timestamp() {
  date "+%Y-%m-%d %H:%M:%S"
}

if [[ ! -x "$PYEXE" ]]; then
  echo "[$(timestamp)] python exe not found: $PYEXE" >> "$LOGFILE"
  exit 1
fi

if [[ -f "$ENVFILE" ]]; then
  while IFS='=' read -r key value; do
    [[ -z "${key// }" ]] && continue
    [[ "$key" =~ ^[[:space:]]*# ]] && continue
    case "$key" in
      BIGQUERY_PROJECT_ID|BIGQUERY_DATASET|GOOGLE_APPLICATION_CREDENTIALS|OPERATIONAL_COMPLETION_BUFFER_MINUTES|ACCUMULATION_COMPLETION_BUFFER_MINUTES)
        export "$key"="${value:-}"
        ;;
    esac
  done < "$ENVFILE"
fi

export OPERATIONAL_COMPLETION_BUFFER_MINUTES="${OPERATIONAL_COMPLETION_BUFFER_MINUTES:-${ACCUMULATION_COMPLETION_BUFFER_MINUTES:-90}}"

if [[ -z "${BIGQUERY_PROJECT_ID:-}" ]]; then
  echo "[$(timestamp)] warning: BIGQUERY_PROJECT_ID not set; automatic BigQuery sync will be skipped" >> "$LOGFILE"
fi
if [[ -z "${BIGQUERY_DATASET:-}" ]]; then
  echo "[$(timestamp)] warning: BIGQUERY_DATASET not set; automatic BigQuery sync will be skipped" >> "$LOGFILE"
fi
if [[ -n "${BIGQUERY_PROJECT_ID:-}" && -n "${BIGQUERY_DATASET:-}" && -z "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
  echo "[$(timestamp)] warning: GOOGLE_APPLICATION_CREDENTIALS not set; automatic BigQuery sync requires ADC or an explicit service-account JSON" >> "$LOGFILE"
fi

if [[ -f "$RECOVERY_HELPER" ]]; then
  echo "[$(timestamp)] ingestion cycle launch check" >> "$LOGFILE"
  set +e
  "$PYEXE" "$RECOVERY_HELPER" \
    --mode guarded-run \
    --python-exe "$PYEXE" \
    --root "$ROOT" \
    --reports-dir "$ROOT/output/reports" \
    --min-completed-gap-minutes "$OPERATIONAL_COMPLETION_BUFFER_MINUTES" \
    -- \
    "$PYEXE" "$ROOT/run_pipeline.py" \
    --python-exe "$PYEXE" \
    --skip-reports \
    --report-output-dir "$ROOT/output/reports" \
    --report-timestamp-tz local >> "$LOGFILE" 2>&1
  RC=$?
  set -e
  if [[ -f "$RECOVERY_STATUS" && -f "$CYCLE_STATE" ]]; then
    "$PYEXE" -c "import json, pathlib, datetime; p=json.loads(pathlib.Path(r'$RECOVERY_STATUS').read_text(encoding='utf-8')); c=json.loads(pathlib.Path(r'$CYCLE_STATE').read_text(encoding='utf-8')); print(f'[{datetime.datetime.now().strftime(\"%Y-%m-%d %H:%M:%S\")}] ingestion wrapper summary: state={c.get(\"state\")} action={p.get(\"action\")} reason={p.get(\"reason\")} cycle_id={c.get(\"cycle_id\")} launched={p.get(\"launched\")} db_ok={(p.get(\"db_check\") or {}).get(\"ok\")} rc=$RC')" >> "$LOGFILE" 2>&1
  fi

  if [[ "$RC" -eq 10 ]]; then
    echo "[$(timestamp)] ingestion cycle skipped: wrapper lock or active accumulation already present" >> "$LOGFILE"
    exit 0
  fi
  if [[ "$RC" -eq 11 ]]; then
    echo "[$(timestamp)] ingestion cycle skipped: ${OPERATIONAL_COMPLETION_BUFFER_MINUTES} minute post-completion buffer is active" >> "$LOGFILE"
    exit 0
  fi
  if [[ "$RC" -eq 12 ]]; then
    echo "[$(timestamp)] ingestion cycle skipped: PostgreSQL is unavailable" >> "$LOGFILE"
    exit 0
  fi
  echo "[$(timestamp)] ingestion wrapper finished rc=$RC" >> "$LOGFILE"
  exit "$RC"
fi

echo "[$(timestamp)] starting ingestion cycle" >> "$LOGFILE"
set +e
"$PYEXE" "$ROOT/run_pipeline.py" \
  --python-exe "$PYEXE" \
  --skip-reports \
  --report-output-dir "$ROOT/output/reports" \
  --report-timestamp-tz local >> "$LOGFILE" 2>&1
RC=$?
set -e
echo "[$(timestamp)] ingestion cycle finished rc=$RC" >> "$LOGFILE"
exit "$RC"
