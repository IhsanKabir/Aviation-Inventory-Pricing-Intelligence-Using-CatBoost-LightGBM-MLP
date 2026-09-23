# Release Runbook — OTA Discount Comparison (0 → live)

How a change goes from your machine to fully published: **web** (Vercel), **API**
(Cloud Run), and the **desktop app** (GitHub Release + auto-update).

---

## Mental model — what triggers what

| You do | What runs | Result |
|--------|-----------|--------|
| `git push origin master` | **ci** (tests + web build) — always | pass/fail gate |
| …and you touched `apps/web/**` | **deploy-web-vercel** | website updates |
| …and you touched `apps/api/**`, `discount_engine/**`, `modules/**`, `pyproject.toml`, `requirements.txt` | **deploy-api-cloud-run** | backend updates |
| `git push origin desktop-vX.Y.Z` (a tag) | **release-desktop** | new `OTADiscountReport.exe` published; installed apps see the update banner |

Key point: **pushing to master deploys web + API automatically** (when their files
changed). The **desktop app only ships when you push a `desktop-v*` tag** — a normal
`master` push does NOT release a new exe.

Note: the report engine lives in `discount_engine/` + `modules/`, and the API imports
it — so an engine fix (like a BDFare calc) redeploys the **API too**, and should also
be shipped to the **desktop** via a tag. Web only changes when `apps/web/**` changes.

---

## One-time setup (fresh machine only — skip for normal updates)

```bash
git clone https://github.com/IhsanKabir/Aviation-Inventory-Pricing-Intelligence-Using-CatBoost-LightGBM-MLP.git
cd Aviation-Inventory-Pricing-Intelligence-Using-CatBoost-LightGBM-MLP

python -m venv .venv && .venv/Scripts/activate       # Windows
pip install -r requirements.txt
pip install pandas sqlalchemy psycopg2-binary openpyxl requests pytest httpx
pip install pyinstaller pywebview keyring psutil     # desktop build deps

cd apps/web && npm ci && cd ../..                    # web deps
gh auth login                                        # for pushing + watching pipelines
```

Secrets are already configured on Cloud Run and Vercel (`OAUTH_BRIDGE_SECRET`,
`REPORT_ACCESS_ADMIN_TOKEN`, `AIRLINE_DB_URL`, …). You don't touch them for a
normal update.

---

## The full flow (every update)

### 1. Make your change
Edit the code.

### 2. Verify locally
```bash
# Python engine / API / desktop tests (the set CI runs):
python -m pytest tests/test_true_base.py tests/test_grid_truebase.py \
  tests/test_engine_report.py tests/test_highlight.py tests/test_sync_payload_no_secrets.py \
  tests/test_discount_reports_api.py tests/test_desktop_backend.py tests/test_audit_fixes.py \
  tests/test_monitoring.py tests/test_sharetrip_combine.py tests/test_sharetrip_judge.py \
  tests/test_bdfare_commission.py -q

# ONLY if you changed apps/web — build it (use the real build, not incremental tsc):
cd apps/web && npm run build && cd ../..
```

### 3. If the desktop app changed — bump, build, boot-test
```bash
# a) bump the version in desktop/__init__.py  (e.g. 0.1.18 -> 0.1.19)

# b) clear any locked exe (Windows holds the file if the app is open):
#    (PowerShell)
#    Get-Process | ? { $_.Name -like "*OTADiscountReport*" } | Stop-Process -Force
#    Remove-Item dist/OTADiscountReport.exe -Force -ErrorAction SilentlyContinue

# c) build the single exe:
python -m PyInstaller desktop/build.spec --noconfirm --distpath dist

# d) boot-test (PowerShell): start it, wait, confirm it's alive:
#    $p = Start-Process dist/OTADiscountReport.exe -PassThru; Start-Sleep 12
#    if ($p.HasExited) { "DIED $($p.ExitCode)" } else { "ALIVE"; Stop-Process $p.Id -Force }
```
The exe is rebuilt on the CI runner during the release too — this local build is just
to catch breakage before tagging.

### 4. Commit
```bash
git add <changed files>
git commit -m "fix(scope): what changed and why"
# (LF -> CRLF warnings on Windows are harmless.)
```

### 5. Push to master  → deploys WEB + API automatically
```bash
git push origin master
```

### 6. Ship the desktop app  → only if the app/engine changed
```bash
git tag desktop-v0.1.19
git push origin desktop-v0.1.19
# If the tag push fails on a transient network error, just re-run the push line.
```

### 7. Watch the pipelines to green
```bash
gh run list --limit 4                       # see status
gh run watch <run-id> --exit-status         # follow one to completion
```
Expected green: `ci`, `deploy-web-vercel` (if web changed), `deploy-api-cloud-run`
(if engine/API changed), `release-desktop` (if you tagged).

### 8. Verify live
```bash
# API reports the new exe version:
curl -s "https://aero-pulse-api-591603094460.asia-south1.run.app/api/v1/app/latest?app=discount-report"

# release asset published:
gh release view desktop-v0.1.19 --json assets --jq '[.assets[].name]'
```
- Website: open `/downloads?product=discount-report` — the Latest card shows the new version.
- Installed apps: users get the **update banner** on next launch (no reinstall).

---

## Quick path (typical engine/app fix)

```bash
python -m pytest tests/test_bdfare_commission.py -q          # + related tests
# bump desktop/__init__.py, build + boot-test the exe (step 3)
git add -A && git commit -m "fix: ..."
git push origin master                                       # web + API deploy
git tag desktop-v0.1.X && git push origin desktop-v0.1.X     # desktop release
gh run list --limit 4                                        # watch green
```

---

## Gotchas we've actually hit

- **Exe won't rebuild — `Access is denied` / file locked:** the app is still running.
  Kill it and delete `dist/OTADiscountReport.exe` before rebuilding (step 3b).
- **Web build looked fine locally but broke on Vercel:** incremental `tsc` can miss a
  missing import. Always `npm run build` before pushing `apps/web`.
- **Tag push fails once with a connection error:** transient GitHub — just re-push the tag.
- **The update channel + `/downloads` must stay public** — do not auth-gate
  `/api/v1/app/latest`; a broken old install must be able to discover its fix.
- **Version must match the tag** — bump `desktop/__init__.py` to the same number as
  the `desktop-vX.Y.Z` tag, or the updater comparison is wrong.
