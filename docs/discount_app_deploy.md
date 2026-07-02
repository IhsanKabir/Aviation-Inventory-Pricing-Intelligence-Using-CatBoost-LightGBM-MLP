# OTA Discount App — Deploy & Operate Runbook

The desktop app parses HAR captures **locally** and syncs a small, sanitized JSON to
the backend; the web viewer shows the team the colored grid. This is the one-time
setup + the recurring operator steps.

---

## 0. One-time secrets (do this FIRST — the CRITICAL fix fails closed without it)

Generate one shared secret and set it in **both** places (same value):

```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

| Where | Secret / env var | Notes |
|-------|------------------|-------|
| **GitHub → repo Secrets** | `OAUTH_BRIDGE_SECRET` | consumed by the Cloud Run deploy workflow |
| **Vercel → Project → Env** | `OAUTH_BRIDGE_SECRET` | the web sends it on the NextAuth→API bridge |

> Until this is set on **both**, Google sign-in returns 503 by design (email/password
> login is unaffected). This closes the CRITICAL hole where anyone could mint a session
> for any email.

Optional (only if you change the release repo/asset from the defaults):
`DISCOUNT_APP_GITHUB_REPO`, `DISCOUNT_APP_ASSET_NAME` on Cloud Run.

Optional (code signing — otherwise the exe is unsigned and SmartScreen warns once):
`WINDOWS_CERT_BASE64` (base64 of your .pfx), `WINDOWS_CERT_PASSWORD` in GitHub Secrets.

---

## 1. Deploy the backend + web

Both auto-deploy from `master` on the relevant paths (the API deploy now also
triggers on `discount_engine/**` and `modules/**`, which it imports):

- **API (Cloud Run):** merge to `master` → `deploy-api-cloud-run` builds + deploys.
  On startup it auto-creates the `discount_reports` table. Verify:
  `GET https://<api>/api/v1/app/latest?app=iata` still works (backward compat).
- **Web (Vercel):** merge to `master` → `deploy-web-vercel`. Verify the
  **OTA Discounts** tab appears and `/downloads` shows the new product card.

## 2. Cut the desktop release

```bash
git tag desktop-v0.1.0
git push origin desktop-v0.1.0
```

`release-desktop` (Windows runner) builds the one-folder app, signs it if a cert is
configured, and publishes `OTADiscountReport.zip` + `.sha256` as a GitHub Release.
The `/downloads` card and the app's auto-updater both resolve it through
`/api/v1/app/latest?app=discount-report`.

## 3. Grant a teammate access (per person, once)

1. Teammate signs in on the website and opens **OTA Discounts**.
2. They click **Request access** (page_key `discount-comparison`).
3. An admin approves it (same access-request flow as Routes/Penalties).
4. After approval the teammate can sync from the desktop app **and** view on the web
   — both gates check the same approved request by their email.

---

## Recurring: the daily report (operator)

1. Capture HARs per **`docs/discount_report_guide.md`** (FT B2B: use the **Preferred
   Airline** box, e.g. `BS, 2A, BG, VQ`; ShareTrip: short per-airline captures).
2. Open **OTA Discount Report** (desktop) → **Browse** to the HAR folder → **Run**.
3. Review the grid (heed the "NOT NORMALIZED" banner — capture a FT B2B HAR if shown).
4. **Export xlsx** for your records and/or **Sync to dashboard** for the team.
5. Anyone approved sees it at `/discount-comparison`, colored with day-over-day changes.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Google sign-in → 503 | `OAUTH_BRIDGE_SECRET` not set (or mismatched) on Vercel **and** Cloud Run. |
| Desktop sync → 403 | No approved `discount-comparison` request for that email — approve it. |
| Desktop sync → "queued" | Offline/API down — it retries on next launch or **Sign in**. Nothing lost. |
| Web grid has no red | It's the first stored report (no previous day to diff) — expected. |
| "NOT NORMALIZED" banner | No FT B2B HAR / live route this run — BDFare & AKIJ on their own base. |
| SmartScreen warns on the exe | Unsigned build — add the `WINDOWS_CERT_*` secrets, or click *More info → Run anyway*. |
| `/downloads` card shows "No releases yet" | You haven't cut `desktop-v*` yet — do step 2. |
