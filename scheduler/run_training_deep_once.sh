#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$ROOT/logs" "$ROOT/output/reports"

PYEXE="$ROOT/.venv/bin/python"
LOGFILE="$ROOT/logs/training_deep.log"
RECOVERY_HELPER="$ROOT/tools/recover_interrupted_accumulation.py"
ENVFILE="$ROOT/.env"
export RUN_ALL_TRIP_PLAN_MODE=deep

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
      BIGQUERY_PROJECT_ID|BIGQUERY_DATASET|GOOGLE_APPLICATION_CREDENTIALS|DEEP_COMPLETION_BUFFER_MINUTES|TRAINING_COMPLETION_BUFFER_MINUTES|ACCUMULATION_COMPLETION_BUFFER_MINUTES|DEEP_PREDICTION_ML_MODELS|DEEP_PREDICTION_DL_MODELS|DEEP_SKIP_BIGQUERY_SYNC)
        export "$key"="${value:-}"
        ;;
    esac
  done < "$ENVFILE"
fi

export DEEP_COMPLETION_BUFFER_MINUTES="${DEEP_COMPLETION_BUFFER_MINUTES:-${TRAINING_COMPLETION_BUFFER_MINUTES:-${ACCUMULATION_COMPLETION_BUFFER_MINUTES:-120}}}"
export DEEP_PREDICTION_ML_MODELS="${DEEP_PREDICTION_ML_MODELS:-catboost,lightgbm}"
export DEEP_PREDICTION_DL_MODELS="${DEEP_PREDICTION_DL_MODELS:-mlp}"
export DEEP_SKIP_BIGQUERY_SYNC="${DEEP_SKIP_BIGQUERY_SYNC:-0}"

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
  set +e
  "$PYEXE" "$RECOVERY_HELPER" \
    --mode preflight \
    --python-exe "$PYEXE" \
    --root "$ROOT" \
    --reports-dir "$ROOT/output/reports" \
    --min-completed-gap-minutes "$DEEP_COMPLETION_BUFFER_MINUTES" >> "$LOGFILE" 2>&1
  PRE_RC=$?
  set -e

  if [[ "$PRE_RC" -eq 10 ]]; then
    echo "[$(timestamp)] deep training skipped: active or fresh accumulation already present" >> "$LOGFILE"
    exit 0
  fi
  if [[ "$PRE_RC" -eq 11 ]]; then
    echo "[$(timestamp)] deep training skipped: ${DEEP_COMPLETION_BUFFER_MINUTES} minute post-completion buffer is active" >> "$LOGFILE"
    exit 0
  fi
  if [[ "$PRE_RC" -eq 12 ]]; then
    echo "[$(timestamp)] deep training skipped: PostgreSQL is unavailable" >> "$LOGFILE"
    exit 0
  fi
  if [[ "$PRE_RC" -ne 0 ]]; then
    echo "[$(timestamp)] deep training preflight warning rc=$PRE_RC (continuing)" >> "$LOGFILE"
  fi
fi

echo "[$(timestamp)] starting deep training cycle" >> "$LOGFILE"
set +e
cmd=(
  "$PYEXE" "$ROOT/run_pipeline.py"
  --python-exe "$PYEXE"
  --trip-plan-mode deep
  --skip-reports
  --report-output-dir "$ROOT/output/reports"
  --report-timestamp-tz local
  --prediction-ml-models "$DEEP_PREDICTION_ML_MODELS"
  --prediction-dl-models "$DEEP_PREDICTION_DL_MODELS"
)
if [[ "$DEEP_SKIP_BIGQUERY_SYNC" == "1" ]]; then
  cmd+=(--skip-bigquery-sync)
fi
"${cmd[@]}" >> "$LOGFILE" 2>&1
RC=$?
set -e
echo "[$(timestamp)] deep training cycle finished rc=$RC" >> "$LOGFILE"
exit "$RC"
