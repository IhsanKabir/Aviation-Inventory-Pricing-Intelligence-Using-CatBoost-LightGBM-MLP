"use client";

import { signIn } from "next-auth/react";
import { useRouter, useSearchParams } from "next/navigation";
import { useMemo, useState, useTransition } from "react";

import { sanitizeReturnPath } from "@/lib/navigation";

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

      const next = sanitizeReturnPath(searchParams.get("next"), "/routes");
      startTransition(() => {
        router.replace(next);
        router.refresh();
      });
    } catch (submitError) {
      setError(submitError instanceof Error ? submitError.message : "Unable to continue.");
    }
  }

  async function continueWithGoogle() {
    const next = sanitizeReturnPath(searchParams.get("next"), "/routes");
    await signIn("google", { callbackUrl: next });
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

      <div className="button-row" style={{ marginBottom: "1rem" }} role="group" aria-label="Sign-in mode">
        <button
          className="chip"
          aria-pressed={mode === "login"}
          data-active={mode === "login"}
          onClick={() => setMode("login")}
          type="button"
        >
          Sign in
        </button>
        <button
          className="chip"
          aria-pressed={mode === "register"}
          data-active={mode === "register"}
          onClick={() => setMode("register")}
          type="button"
        >
          Create account
        </button>
      </div>

      <form
        className="filter-form"
        onSubmit={(event) => {
          event.preventDefault();
          submit();
        }}
      >
        {mode === "register" ? (
          <label className="field">
            <span>Full name</span>
            <input
              autoComplete="name"
              onChange={(event) => setFullName(event.target.value)}
              required
              type="text"
              value={fullName}
            />
          </label>
        ) : null}

        <label className="field">
          <span>Email</span>
          <input
            autoComplete="email"
            onChange={(event) => setEmail(event.target.value)}
            required
            type="email"
            value={email}
          />
        </label>

        <label className="field">
          <span>Password</span>
          <input
            autoComplete={mode === "login" ? "current-password" : "new-password"}
            onChange={(event) => setPassword(event.target.value)}
            required
            type="password"
            value={password}
          />
        </label>

        {error ? (
          <div className="status-banner warn" role="alert">
            {error}
          </div>
        ) : null}

        <div className="button-row">
          <button className="button-link" data-pending={isPending} type="submit">
            {ctaLabel}
          </button>
        </div>
      </form>
    </div>
  );
}
