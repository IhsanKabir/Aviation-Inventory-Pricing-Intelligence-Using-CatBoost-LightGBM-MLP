#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TPL_DIR="$ROOT/deploy/oracle_cloud/systemd"
OUT_DIR="$(mktemp -d)"
APP_USER="${APP_USER:-$USER}"
APP_GROUP="${APP_GROUP:-$(id -gn "$APP_USER")}"

if [[ ! -x "$ROOT/.venv/bin/python" ]]; then
  echo "Missing venv python at $ROOT/.venv/bin/python"
  echo "Run bootstrap first: ./deploy/oracle_cloud/bootstrap_ubuntu_oracle_free.sh"
  exit 1
fi

if [[ ! -f "$ROOT/scheduler/run_ingestion_4h_once.sh" ]]; then
  echo "Missing Linux ingestion wrapper: $ROOT/scheduler/run_ingestion_4h_once.sh"
  exit 1
fi

render() {
  local src="$1"
  local dst="$2"
  sed \
    -e "s|__APP_DIR__|$ROOT|g" \
    -e "s|__APP_USER__|$APP_USER|g" \
    -e "s|__APP_GROUP__|$APP_GROUP|g" \
    "$src" > "$dst"
}

chmod +x "$ROOT/scheduler/run_ingestion_4h_once.sh" "$ROOT/scheduler/run_maintenance_pulse_once.sh"

render "$TPL_DIR/airlineintel-ingestion4h.service.tpl" "$OUT_DIR/airlineintel-ingestion4h.service"
render "$TPL_DIR/airlineintel-ingestion4h.timer.tpl" "$OUT_DIR/airlineintel-ingestion4h.timer"
render "$TPL_DIR/airlineintel-maintenance-pulse.service.tpl" "$OUT_DIR/airlineintel-maintenance-pulse.service"
render "$TPL_DIR/airlineintel-maintenance-pulse.timer.tpl" "$OUT_DIR/airlineintel-maintenance-pulse.timer"

echo "[systemd] installing units for user=$APP_USER group=$APP_GROUP root=$ROOT"
sudo cp "$OUT_DIR"/airlineintel-* /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now airlineintel-maintenance-pulse.timer
sudo systemctl enable --now airlineintel-ingestion4h.timer

echo
echo "[systemd] installed and started timers:"
systemctl status airlineintel-ingestion4h.timer --no-pager --lines=0 || true
systemctl status airlineintel-maintenance-pulse.timer --no-pager --lines=0 || true
echo
echo "Verify logs:"
echo "  tail -n 50 $ROOT/logs/ingestion_4h.log"
echo "  tail -n 50 $ROOT/logs/maintenance_pulse.log"

