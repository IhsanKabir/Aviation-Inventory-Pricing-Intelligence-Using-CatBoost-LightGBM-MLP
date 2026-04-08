"use client";

import { useRouter, useSearchParams } from "next/navigation";
import { useMemo, useState, useTransition } from "react";

type Mode = "login" | "register";

export function UserAuthForm({ googleEnabled = false }: { googleEnabled?: boolean }) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [mode, setMode] = useState<Mode>("login");
  const [fullName, setFullName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  const ctaLabel = useMemo(() => (mode === "login" ? "Sign in" : "Create account"), [mode]);

  async function submit() {
    setError(null);
    try {
      const response = await fetch(mode === "login" ? "/api/auth/login" : "/api/auth/register", {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          fullName,
          email,
          password
        })
      });
      const payload = (await response.json().catch(() => null)) as { detail?: string } | null;
      if (!response.ok) {
        throw new Error(payload?.detail || "Unable to continue.");
      }

      const next = searchParams.get("next") || "/routes";
      startTransition(() => {
        router.replace(next);
        router.refresh();
      });
    } catch (submitError) {
      setError(submitError instanceof Error ? submitError.message : "Unable to continue.");
    }
  }

  function continueWithGoogle() {
    const next = searchParams.get("next") || "/routes";
    window.location.href = `/api/auth/signin/google?callbackUrl=${encodeURIComponent(next)}`;
  }

  return (
    <div className="card panel admin-auth-card">
      <h2>{mode === "login" ? "User sign in" : "Create user account"}</h2>
      <p className="panel-copy">
        Sign in before requesting route data so each request can be tracked to a real user account.
      </p>

      {googleEnabled ? (
        <>
          <div className="button-row" style={{ marginBottom: "0.75rem" }}>
            <button className="button-link" onClick={continueWithGoogle} type="button">
              Continue with Google
            </button>
          </div>
          <div className="panel-copy" style={{ marginBottom: "1rem" }}>
            Google sign-in is the fastest option. Email and password can still be used as a fallback below.
          </div>
        </>
      ) : (
        <div className="status-banner warn" style={{ marginBottom: "1rem" }}>
          Google sign-in is not configured yet on this deployment. Email and password are still available below.
        </div>
      )}

      <div className="button-row" style={{ marginBottom: "1rem" }}>
        <button className="chip" data-active={mode === "login"} onClick={() => setMode("login")} type="button">
          Sign in
        </button>
        <button className="chip" data-active={mode === "register"} onClick={() => setMode("register")} type="button">
          Create account
        </button>
      </div>

      <div className="filter-form">
        {mode === "register" ? (
          <label className="field">
            <span>Full name</span>
            <input onChange={(event) => setFullName(event.target.value)} type="text" value={fullName} />
          </label>
        ) : null}

        <label className="field">
          <span>Email</span>
          <input onChange={(event) => setEmail(event.target.value)} type="email" value={email} />
        </label>

        <label className="field">
          <span>Password</span>
          <input onChange={(event) => setPassword(event.target.value)} type="password" value={password} />
        </label>

        {error ? <div className="status-banner warn">{error}</div> : null}

        <div className="button-row">
          <button className="button-link" data-pending={isPending} onClick={submit} type="button">
            {ctaLabel}
          </button>
        </div>
      </div>
    </div>
  );
}
