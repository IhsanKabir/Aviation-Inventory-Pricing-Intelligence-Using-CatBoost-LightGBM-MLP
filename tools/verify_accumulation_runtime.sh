#!/usr/bin/env bash
set -euo pipefail

ROOT_DEFAULT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="${ROOT:-$ROOT_DEFAULT}"
STALE_MINUTES="${STALE_MINUTES:-15}"

if [[ $# -gt 0 ]]; then
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --root)
        ROOT="$2"
        shift 2
        ;;
      --stale-minutes)
        STALE_MINUTES="$2"
        shift 2
        ;;
      *)
        echo "Unknown argument: $1" >&2
        echo "Usage: $0 [--root <repo-root>] [--stale-minutes <minutes>]" >&2
        exit 2
        ;;
    esac
  done
fi

REPORTS_DIR="$ROOT/output/reports"
HB1="$REPORTS_DIR/run_all_accumulation_status_latest.json"
HB2="$REPORTS_DIR/run_all_status_latest.json"
HEARTBEAT_FILE=""
[[ -f "$HB1" ]] && HEARTBEAT_FILE="$HB1"
[[ -z "$HEARTBEAT_FILE" && -f "$HB2" ]] && HEARTBEAT_FILE="$HB2"

PYEXE="python3"
if [[ -x "$ROOT/.venv/bin/python" ]]; then
  PYEXE="$ROOT/.venv/bin/python"
fi

timers=("airlineintel-ingestion4h.timer" "airlineintel-maintenance-pulse.timer")
services=("airlineintel-ingestion4h.service" "airlineintel-maintenance-pulse.service")

echo "=== systemd timers ==="
declare -A TIMER_ACTIVE TIMER_ENABLED TIMER_NEXT TIMER_LAST
for t in "${timers[@]}"; do
  if command -v systemctl >/dev/null 2>&1; then
    TIMER_ACTIVE["$t"]="$(systemctl is-active "$t" 2>/dev/null || true)"
    TIMER_ENABLED["$t"]="$(systemctl is-enabled "$t" 2>/dev/null || true)"
    # Pull a single line from list-timers for human readability if present.
    line="$(systemctl list-timers --all --no-legend 2>/dev/null | grep -F " $t " | tail -n 1 || true)"
    if [[ -n "$line" ]]; then
      # Columns: NEXT LEFT LAST PASSED UNIT ACTIVATES
      # Keep raw line for robustness.
      TIMER_NEXT["$t"]="$(echo "$line" | awk '{print $1" "$2" "$3" "$4}')"
      TIMER_LAST["$t"]="$(echo "$line" | awk '{print $5" "$6" "$7" "$8}')"
      echo "$t"
      echo "  enabled : ${TIMER_ENABLED[$t]}"
      echo "  active  : ${TIMER_ACTIVE[$t]}"
      echo "  list    : $line"
    else
      echo "$t"
      echo "  enabled : ${TIMER_ENABLED[$t]:-unknown}"
      echo "  active  : ${TIMER_ACTIVE[$t]:-unknown}"
      echo "  list    : (not found in systemctl list-timers)"
    fi
  else
    echo "$t"
    echo "  systemctl not available"
  fi
done

echo
echo "=== service runtime limit (execution limit risk equivalent) ==="
INGEST_RUNTIME_LIMIT="unknown"
INGEST_RUNTIME_RISK="false"
if command -v systemctl >/dev/null 2>&1; then
  INGEST_RUNTIME_LIMIT="$(systemctl show airlineintel-ingestion4h.service -p RuntimeMaxUSec --value 2>/dev/null || echo unknown)"
  echo "airlineintel-ingestion4h.service RuntimeMaxUSec: $INGEST_RUNTIME_LIMIT"
  # In systemd, "infinity" means no runtime limit; any finite positive value can be a risk.
  if [[ -n "$INGEST_RUNTIME_LIMIT" && "$INGEST_RUNTIME_LIMIT" != "infinity" && "$INGEST_RUNTIME_LIMIT" != "0" && "$INGEST_RUNTIME_LIMIT" != "unknown" ]]; then
    INGEST_RUNTIME_RISK="true"
    echo "WARNING: ingestion service has a finite runtime limit (possible long-run interruption risk)."
  fi
fi

echo
echo "=== active accumulation processes ==="
PROC_TMP="$(mktemp)"
ps -eo pid=,ppid=,comm=,args= | awk '
  BEGIN{IGNORECASE=1}
  /python/ && /(run_pipeline\.py|run_all\.py|generate_reports\.py)/ {
    print $0
  }' > "$PROC_TMP" || true
if [[ -s "$PROC_TMP" ]]; then
  cat "$PROC_TMP"
  ACTIVE_PROC_COUNT="$(wc -l < "$PROC_TMP" | tr -d ' ')"
else
  echo "No accumulation processes running."
  ACTIVE_PROC_COUNT="0"
fi

echo
echo "=== heartbeat freshness ==="
HB_JSON_TMP="$(mktemp)"
if [[ -z "$HEARTBEAT_FILE" ]]; then
  echo "Heartbeat file not found."
  cat > "$HB_JSON_TMP" <<JSON
{"heartbeat_file": null, "state": null, "accumulation_run_id": null, "heartbeat_age_minutes": null, "heartbeat_status": "MISSING", "progress": null, "current": null}
JSON
else
  "$PYEXE" - "$HEARTBEAT_FILE" "$STALE_MINUTES" <<'PY' | tee "$HB_JSON_TMP"
import json, sys
from datetime import datetime, timezone
from pathlib import Path

hb_file = Path(sys.argv[1])
stale_minutes = float(sys.argv[2])

def p_iso(v):
    if not v:
        return None
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except Exception:
        return None

payload = {}
try:
    payload = json.loads(hb_file.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        payload = {}
except Exception:
    payload = {}

ts = None
for k in (
    "accumulation_written_at_utc",
    "written_at_utc",
    "accumulation_last_query_at_utc",
    "last_query_at_utc",
    "accumulation_started_at_utc",
    "started_at_utc",
):
    ts = p_iso(payload.get(k))
    if ts:
        break

age = None
if ts:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    age = max(0.0, (datetime.now(timezone.utc) - ts.astimezone(timezone.utc)).total_seconds() / 60.0)
    age = round(age, 2)

state = payload.get("state")
acc_id = payload.get("accumulation_run_id") or payload.get("scrape_id")
done = payload.get("overall_query_completed")
total = payload.get("overall_query_total")
cur = f"{payload.get('current_airline','?')} {payload.get('current_origin','?')}->{payload.get('current_destination','?')} {payload.get('current_date','?')} {payload.get('current_cabin','?')}"
hb_status = "STALE" if (age is not None and age > stale_minutes) else "OK"
summary = {
    "heartbeat_file": str(hb_file),
    "state": state,
    "accumulation_run_id": acc_id,
    "heartbeat_age_minutes": age,
    "heartbeat_status": hb_status,
    "progress": f"{done}/{total}" if done is not None or total is not None else None,
    "current": cur,
}
print(json.dumps(summary, ensure_ascii=False))
PY
  echo
fi

HB_STATUS="$("$PYEXE" - "$HB_JSON_TMP" <<'PY'
import json, sys
from pathlib import Path
p = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
print(p.get("heartbeat_status") or "")
PY
)"

echo "=== quick verdict ==="
if [[ "$ACTIVE_PROC_COUNT" != "0" ]]; then
  echo "Accumulation is RUNNING now."
else
  echo "No accumulation process is running now."
fi
if [[ "$INGEST_RUNTIME_RISK" == "true" ]]; then
  echo "Runtime limit risk detected on airlineintel-ingestion4h.service (finite RuntimeMaxUSec)."
fi
if [[ "$HB_STATUS" == "STALE" ]]; then
  echo "Heartbeat is STALE. Maintenance pulse recovery should relaunch only if no active process exists."
fi

echo
echo "=== machine-readable summary (json) ==="
"$PYEXE" - "$ROOT" "$INGEST_RUNTIME_LIMIT" "$INGEST_RUNTIME_RISK" "$ACTIVE_PROC_COUNT" "$HB_JSON_TMP" <<'PY'
import json, sys
from datetime import datetime
from pathlib import Path

root, runtime_limit, runtime_risk, proc_count, hb_json_path = sys.argv[1:]
hb = json.loads(Path(hb_json_path).read_text(encoding="utf-8"))

summary = {
    "checked_at_local": datetime.now().astimezone().isoformat(),
    "root": root,
    "ingestion_service_runtime_max_usec": runtime_limit,
    "ingestion_runtime_limit_risk": runtime_risk.lower() == "true",
    "active_accumulation_process_count": int(proc_count),
    "heartbeat": hb,
}
print(json.dumps(summary, indent=2, ensure_ascii=False))
PY

rm -f "$PROC_TMP" "$HB_JSON_TMP"

