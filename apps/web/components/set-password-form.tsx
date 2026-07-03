"use client";

import { useState } from "react";

export function SetPasswordForm() {
  const [password, setPassword] = useState("");
  const [confirm, setConfirm] = useState("");
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);

  async function submit() {
    setMessage(null);
    setError(null);
    if (password.length < 8) {
      setError("Password must be at least 8 characters.");
      return;
    }
    if (password !== confirm) {
      setError("Passwords do not match.");
      return;
    }
    setIsSubmitting(true);
    try {
      const response = await fetch("/api/user/set-password", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ password })
      });
      const payload = (await response.json().catch(() => null)) as
        | { ok?: boolean; detail?: string }
        | null;
      if (!response.ok) {
        throw new Error(payload?.detail || `${response.status} ${response.statusText}`);
      }
      setMessage("Password saved. You can now sign in to the desktop app with your email + this password.");
      setPassword("");
      setConfirm("");
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Could not save the password.");
    } finally {
      setIsSubmitting(false);
    }
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10, maxWidth: 360 }}>
      <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
        New password
        <input
          type="password"
          value={password}
          autoComplete="new-password"
          onChange={(event) => setPassword(event.target.value)}
        />
      </label>
      <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
        Confirm password
        <input
          type="password"
          value={confirm}
          autoComplete="new-password"
          onChange={(event) => setConfirm(event.target.value)}
        />
      </label>
      <button className="button-link" onClick={submit} disabled={isSubmitting}>
        {isSubmitting ? "Saving…" : "Save password"}
      </button>
      {message ? <p style={{ color: "var(--good, #0a7d33)" }}>{message}</p> : null}
      {error ? <p style={{ color: "var(--alert, #b00020)" }}>{error}</p> : null}
    </div>
  );
}
