"use client";

import { useEffect, useState, useTransition } from "react";

type AdminUser = {
  user_id: string;
  email: string;
  full_name: string | null;
  status: string;
  auth_provider?: string | null;
  created_at_utc?: string | null;
  last_login_at_utc?: string | null;
};

/** Account-level control: the kill switch. Disabling a user revokes every live
 * session immediately — desktop and web are locked out on their next request. */
export function AdminUsersPanel() {
  const [users, setUsers] = useState<AdminUser[]>([]);
  const [search, setSearch] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  async function refresh() {
    const response = await fetch("/api/admin/users", { cache: "no-store" });
    const payload = (await response.json().catch(() => null)) as
      | { items?: AdminUser[]; detail?: string }
      | null;
    if (!response.ok) {
      throw new Error(payload?.detail || "Unable to load users.");
    }
    setUsers(payload?.items || []);
  }

  useEffect(() => {
    refresh().catch((loadError) =>
      setError(loadError instanceof Error ? loadError.message : "Unable to load users."));
  }, []);

  async function setStatus(user: AdminUser, status: "active" | "disabled") {
    if (status === "disabled" &&
        !window.confirm(`Disable ${user.email}? Every live session is revoked immediately.`)) {
      return;
    }
    setError(null);
    try {
      const response = await fetch(`/api/admin/users/${encodeURIComponent(user.user_id)}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status })
      });
      const payload = (await response.json().catch(() => null)) as { detail?: string } | null;
      if (!response.ok) {
        throw new Error(payload?.detail || "Unable to update the account.");
      }
      startTransition(() => {
        refresh().catch(() => undefined);
      });
    } catch (updateError) {
      setError(updateError instanceof Error ? updateError.message : "Unable to update the account.");
    }
  }

  const needle = search.trim().toLowerCase();
  const visible = needle
    ? users.filter((user) =>
        `${user.email} ${user.full_name ?? ""}`.toLowerCase().includes(needle))
    : users;

  return (
    <div className="admin-request-workbench">
      <div className="admin-toolbar">
        <div>
          <div className="admin-section-kicker">User accounts</div>
          <input
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Search by email or name…"
            style={{ minWidth: 260, marginTop: 6 }}
            type="search"
            value={search}
          />
        </div>
      </div>

      {error ? <div className="panel-copy" style={{ color: "var(--alert)" }}>{error}</div> : null}

      <div className="admin-request-list">
        {visible.map((user) => (
          <div className="card panel admin-request-card" key={user.user_id}>
            <div className="admin-request-card-header">
              <div>
                <h3>{user.full_name || user.email}</h3>
                <div className="admin-request-meta">
                  <span>{user.email}</span>
                  <span>{user.auth_provider === "password" ? "email+password" : user.auth_provider}</span>
                  <span>Last login {user.last_login_at_utc?.slice(0, 10) || "never"}</span>
                </div>
              </div>
              <span className={`pill ${user.status === "active" ? "good" : "warn"}`}>
                {user.status === "active" ? "Active" : "DISABLED"}
              </span>
            </div>
            <div className="button-row admin-decision-row">
              {user.status === "active" ? (
                <button
                  className="button-link ghost"
                  data-pending={isPending}
                  onClick={() => setStatus(user, "disabled")}
                  style={{ color: "var(--alert)" }}
                  type="button"
                >
                  Disable account (revokes all sessions)
                </button>
              ) : (
                <button
                  className="button-link"
                  data-pending={isPending}
                  onClick={() => setStatus(user, "active")}
                  type="button"
                >
                  Re-enable account
                </button>
              )}
            </div>
          </div>
        ))}
        {!visible.length ? <p className="panel-copy">No matching users.</p> : null}
      </div>
    </div>
  );
}
