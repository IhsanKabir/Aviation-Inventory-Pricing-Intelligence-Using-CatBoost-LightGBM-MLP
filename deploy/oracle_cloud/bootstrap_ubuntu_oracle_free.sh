#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
DB_NAME="${DB_NAME:-Playwright_API_Calling}"
DB_USER="${DB_USER:-airlineintel}"
DB_PASSWORD="${DB_PASSWORD:-airlineintel_change_me}"
INSTALL_OPTIONAL_ML="${INSTALL_OPTIONAL_ML:-0}"

echo "[bootstrap] repo root: $ROOT"
echo "[bootstrap] installing OS packages..."
sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  git \
  build-essential \
  libpq-dev \
  postgresql \
  postgresql-contrib \
  python3-venv \
  python3-pip

echo "[bootstrap] enabling PostgreSQL..."
sudo systemctl enable --now postgresql

echo "[bootstrap] creating PostgreSQL role/database if missing..."
sudo -u postgres psql -v ON_ERROR_STOP=1 <<SQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${DB_USER}') THEN
    EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L', '${DB_USER}', '${DB_PASSWORD}');
  ELSE
    EXECUTE format('ALTER ROLE %I WITH LOGIN PASSWORD %L', '${DB_USER}', '${DB_PASSWORD}');
  END IF;
END
\$\$;
SQL

if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'" | grep -q 1; then
  sudo -u postgres createdb -O "${DB_USER}" "${DB_NAME}"
fi

echo "[bootstrap] creating python virtualenv..."
"$PYTHON_BIN" -m venv "$ROOT/.venv"
"$ROOT/.venv/bin/python" -m pip install --upgrade pip wheel setuptools

echo "[bootstrap] installing Python requirements (core)..."
TMP_REQ="$(mktemp)"
grep -vE '^(catboost|lightgbm)\s*$' "$ROOT/requirements.txt" > "$TMP_REQ"
"$ROOT/.venv/bin/python" -m pip install -r "$TMP_REQ"
rm -f "$TMP_REQ"

if [[ "$INSTALL_OPTIONAL_ML" == "1" ]]; then
  echo "[bootstrap] attempting optional ML backends (catboost/lightgbm)..."
  set +e
  "$ROOT/.venv/bin/python" -m pip install catboost lightgbm
  ML_RC=$?
  set -e
  if [[ "$ML_RC" -ne 0 ]]; then
    echo "[bootstrap] optional ML backend install failed (non-fatal). Continue with core stack."
  fi
fi

if [[ ! -f "$ROOT/.env" ]]; then
  echo "[bootstrap] creating .env from .env.example"
  cp "$ROOT/.env.example" "$ROOT/.env"
fi

echo "[bootstrap] writing DB defaults into .env (non-destructive key updates)..."
"$ROOT/.venv/bin/python" - <<'PY' "$ROOT/.env" "$DB_NAME" "$DB_USER" "$DB_PASSWORD"
import sys
from pathlib import Path
env_path = Path(sys.argv[1])
db_name, db_user, db_password = sys.argv[2:5]
lines = env_path.read_text(encoding="utf-8").splitlines()
updates = {
    "DB_HOST": "localhost",
    "DB_PORT": "5432",
    "DB_NAME": db_name,
    "DB_USER": db_user,
    "DB_PASSWORD": db_password,
    "AIRLINE_DB_URL": f"postgresql+psycopg2://{db_user}:{db_password}@localhost:5432/{db_name}",
}
seen = set()
out = []
for line in lines:
    if "=" in line and not line.strip().startswith("#"):
        k = line.split("=", 1)[0].strip()
        if k in updates:
            out.append(f"{k}={updates[k]}")
            seen.add(k)
            continue
    out.append(line)
for k, v in updates.items():
    if k not in seen:
        out.append(f"{k}={v}")
env_path.write_text("\n".join(out) + "\n", encoding="utf-8")
PY

echo "[bootstrap] quick syntax check..."
"$ROOT/.venv/bin/python" -m py_compile "$ROOT/run_pipeline.py" "$ROOT/run_all.py" "$ROOT/tools/recover_interrupted_accumulation.py"

echo
echo "[bootstrap] complete"
echo "Next:"
echo "  1) Review .env: $ROOT/.env"
echo "  2) Install timers: ./deploy/oracle_cloud/install_systemd_services.sh"
echo "  3) Start with manual test: $ROOT/scheduler/run_ingestion_4h_once.sh"

