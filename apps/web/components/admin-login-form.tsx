"use client";

import { useRouter, useSearchParams } from "next/navigation";
import { useState, useTransition } from "react";

export function AdminLoginForm() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [username, setUsername] = useState("admin");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  async function submitLogin() {
    setError(null);
    try {
      const response = await fetch("/api/admin/login", {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({ username, password })
      });
      const payload = (await response.json().catch(() => null)) as { detail?: string } | null;
      if (!response.ok) {
        throw new Error(payload?.detail || "Unable to sign in.");
      }

      const next = searchParams.get("next") || "/admin";
      startTransition(() => {
        router.replace(next);
        router.refresh();
      });
    } catch (submitError) {
      setError(submitError instanceof Error ? submitError.message : "Unable to sign in.");
    }
  }

  return (
    <div className="card panel admin-auth-card">
      <h2>Admin sign in</h2>
      <p className="panel-copy">
        Use the admin credential to review access requests, monitor sensitive pages, and manage approvals.
      </p>

      <div className="filter-form">
        <label className="field">
          <span>Username</span>
          <input onChange={(event) => setUsername(event.target.value)} type="text" value={username} />
        </label>

        <label className="field">
          <span>Password</span>
          <input onChange={(event) => setPassword(event.target.value)} type="password" value={password} />
        </label>

        {error ? <div className="status-banner warn">{error}</div> : null}

        <div className="button-row">
          <button className="button-link" data-pending={isPending} onClick={submitLogin} type="button">
            Sign in
          </button>
        </div>
      </div>
    </div>
  );
}
