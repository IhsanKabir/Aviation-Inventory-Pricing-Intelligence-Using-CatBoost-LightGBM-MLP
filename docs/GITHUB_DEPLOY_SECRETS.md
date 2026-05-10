# GitHub Deploy Secrets

Use these GitHub Actions secrets if you want deployment to be triggered from the repository instead of a local machine.

## Cloud Run workflow secrets

For [.github/workflows/deploy-api-cloud-run.yml](../.github/workflows/deploy-api-cloud-run.yml):

- `GCP_WORKLOAD_IDENTITY_PROVIDER`
  - Workload Identity Provider resource name
- `GCP_SERVICE_ACCOUNT_EMAIL`
  - Example: `aero-pulse-api@aeropulseintelligence.iam.gserviceaccount.com`
- `API_CORS_ORIGINS`
  - Example: `https://your-vercel-domain.vercel.app`

Recommended:
- use Workload Identity Federation instead of a long-lived JSON key
- grant the service account:
  - `Artifact Registry Writer`
  - `Cloud Run Admin`
  - `Service Account User`
  - `BigQuery Data Viewer`
  - `BigQuery Job User`

## Vercel workflow secrets

For [.github/workflows/deploy-web-vercel.yml](../.github/workflows/deploy-web-vercel.yml):

- `VERCEL_TOKEN`
- `VERCEL_ORG_ID`
- `VERCEL_PROJECT_ID`

The Vercel project must already exist with:

- Root Directory: `apps/web`
- Framework: `Next.js`
- Environment variables:
  - `API_BASE_URL`
  - `NEXT_PUBLIC_API_BASE_URL`
  - `NEXTAUTH_URL`
  - `AUTH_SECRET` when Google sign-in is enabled
  - `AUTH_GOOGLE_ID` optional
  - `AUTH_GOOGLE_SECRET` optional

## Minimum deployment sequence

1. Create the Vercel project and set frontend env vars.
2. Create the Cloud Run service account and IAM bindings.
3. Add the GitHub secrets listed above.
4. Trigger `deploy-api-cloud-run`.
5. Update the Vercel env vars to the Cloud Run URL if needed.
6. Trigger `deploy-web-vercel`.

The web workflow now performs:

- `vercel pull --environment=production`
- `vercel build --prod`
- `vercel deploy --prebuilt --prod`

Important for this monorepo:

- keep Vercel project Root Directory set to `apps/web`
- let the GitHub Actions workflow run the Vercel CLI from repository root so the root directory is not applied twice

## Why this path matters

This removes the local machine from the deployment path:

- collection and training remain local
- GitHub handles build/deploy
- Cloud Run serves the BigQuery-backed API
- Vercel serves the frontend

---

> **Codex will review your output once you are done.**
