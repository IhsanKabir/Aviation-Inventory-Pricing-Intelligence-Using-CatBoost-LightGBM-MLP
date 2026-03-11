#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$ROOT/logs" "$ROOT/output/reports"

PYEXE="$ROOT/.venv/bin/python"
LOGFILE="$ROOT/logs/ingestion_4h.log"
RECOVERY_HELPER="$ROOT/tools/recover_interrupted_accumulation.py"
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
      BIGQUERY_PROJECT_ID|BIGQUERY_DATASET|GOOGLE_APPLICATION_CREDENTIALS|ACCUMULATION_COMPLETION_BUFFER_MINUTES)
        export "$key"="${value:-}"
        ;;
    esac
  done < "$ENVFILE"
fi

export ACCUMULATION_COMPLETION_BUFFER_MINUTES="${ACCUMULATION_COMPLETION_BUFFER_MINUTES:-72}"

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
  echo "[$(timestamp)] starting ingestion cycle" >> "$LOGFILE"
  set +e
  "$PYEXE" "$RECOVERY_HELPER" \
    --mode guarded-run \
    --python-exe "$PYEXE" \
    --root "$ROOT" \
    --reports-dir "$ROOT/output/reports" \
    --min-completed-gap-minutes "$ACCUMULATION_COMPLETION_BUFFER_MINUTES" \
    -- \
    "$PYEXE" "$ROOT/run_pipeline.py" \
    --python-exe "$PYEXE" \
    --skip-reports \
    --report-output-dir "$ROOT/output/reports" \
    --report-timestamp-tz local >> "$LOGFILE" 2>&1
  RC=$?
  set -e

  if [[ "$RC" -eq 10 ]]; then
    echo "[$(timestamp)] ingestion cycle skipped: wrapper lock or active accumulation already present" >> "$LOGFILE"
    exit 0
  fi
  if [[ "$RC" -eq 11 ]]; then
    echo "[$(timestamp)] ingestion cycle skipped: ${ACCUMULATION_COMPLETION_BUFFER_MINUTES} minute post-completion buffer is active" >> "$LOGFILE"
    exit 0
  fi
  echo "[$(timestamp)] ingestion cycle finished rc=$RC" >> "$LOGFILE"
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
