"use client";

import { useMemo, useState, useTransition } from "react";

import type { ReportAccessRequest } from "@/lib/api";

type AdminStatusFilter = "all" | ReportAccessRequest["status"];

function formatStatusLabel(status: ReportAccessRequest["status"]) {
  if (status === "approved") return "Approved";
  if (status === "payment_required") return "Payment required";
  if (status === "rejected") return "Rejected";
  return "Pending";
}

function extractDetail(payload: unknown) {
  if (payload && typeof payload === "object" && "detail" in payload) {
    const detail = (payload as { detail?: unknown }).detail;
    if (typeof detail === "string" && detail.trim()) {
      return detail;
    }
  }
  return null;
}

export function AdminAccessRequestsDashboard({
  initialItems
}: {
  initialItems: ReportAccessRequest[];
}) {
  const [items, setItems] = useState(initialItems);
  const [statusFilter, setStatusFilter] = useState<AdminStatusFilter>("all");
  const [decisionNotes, setDecisionNotes] = useState<Record<string, string>>({});
  const [error, setError] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  const filteredItems = useMemo(() => {
    if (statusFilter === "all") {
      return items;
    }
    return items.filter((item) => item.status === statusFilter);
  }, [items, statusFilter]);

  async function refreshItems(nextStatus = statusFilter) {
    const query = nextStatus === "all" ? "" : `?status=${encodeURIComponent(nextStatus)}`;
    const response = await fetch(`/api/admin/access-requests${query}`, {
      cache: "no-store"
    });
    const payload = (await response.json().catch(() => null)) as { items?: ReportAccessRequest[]; detail?: string } | null;
    if (!response.ok) {
      throw new Error(payload?.detail || "Unable to load access requests.");
    }
    setItems(payload?.items || []);
  }

  async function applyDecision(requestId: string, status: ReportAccessRequest["status"]) {
    setError(null);
    try {
      const response = await fetch(`/api/admin/access-requests/${encodeURIComponent(requestId)}`, {
        method: "PATCH",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          status,
          decision_note: decisionNotes[requestId] || undefined
        })
      });
      const payload = (await response.json().catch(() => null)) as ReportAccessRequest | { detail?: string } | null;
      if (!response.ok) {
        throw new Error(extractDetail(payload) || "Unable to update access request.");
      }
      await refreshItems();
    } catch (updateError) {
      setError(updateError instanceof Error ? updateError.message : "Unable to update access request.");
    }
  }

  async function signOut() {
    setError(null);
    await fetch("/api/admin/logout", { method: "POST" });
    window.location.href = "/admin/login";
  }

  function startRefresh(nextStatus = statusFilter) {
    startTransition(() => {
      refreshItems(nextStatus).catch((refreshError) => {
        setError(refreshError instanceof Error ? refreshError.message : "Unable to refresh access requests.");
      });
    });
  }

  return (
    <div className="stack">
      <div className="button-row" style={{ justifyContent: "space-between", alignItems: "center" }}>
        <div className="chip-row">
          {(["all", "pending", "payment_required", "approved", "rejected"] as AdminStatusFilter[]).map((status) => (
            <button
              key={status}
              className="chip"
              data-active={statusFilter === status}
              data-pending={isPending}
              onClick={() => {
                setStatusFilter(status);
                startRefresh(status);
              }}
              type="button"
            >
              {status === "all" ? "All requests" : formatStatusLabel(status)}
            </button>
          ))}
        </div>

        <div className="button-row">
          <button className="button-link ghost" data-pending={isPending} onClick={() => startRefresh()} type="button">
            Refresh
          </button>
          <button className="button-link ghost" onClick={signOut} type="button">
            Sign out
          </button>
        </div>
      </div>

      {error ? <div className="status-banner warn">{error}</div> : null}

      {!filteredItems.length ? (
        <div className="empty-state">No access requests match the current filter.</div>
      ) : (
        <div className="admin-request-list">
          {filteredItems.map((item) => (
            <div className="card panel admin-request-card" key={item.request_id}>
              <div className="button-row" style={{ justifyContent: "space-between", alignItems: "flex-start" }}>
                <div>
                  <h2 style={{ margin: 0, fontSize: "1.1rem" }}>{item.requester_name || "Unnamed requester"}</h2>
                  <div className="panel-copy" style={{ marginBottom: 0 }}>
                    {item.requester_contact || "No contact provided"} | {item.page_key} | submitted {item.created_at_utc || "-"}
                  </div>
                </div>
                <span className={`pill ${item.status === "approved" ? "good" : "warn"}`}>{formatStatusLabel(item.status)}</span>
              </div>

              <div className="admin-scope-grid">
                <div>
                  <strong>Requested window</strong>
                  <div className="mono">
                    {item.requested_start_date || "any"} to {item.requested_end_date || "any"}
                  </div>
                </div>
                <div>
                  <strong>Scope</strong>
                  <div className="mono">{JSON.stringify(item.request_scope || {}, null, 2)}</div>
                </div>
              </div>

              {item.notes ? (
                <div>
                  <strong>Requester note</strong>
                  <div className="panel-copy" style={{ marginTop: "0.35rem", marginBottom: 0 }}>{item.notes}</div>
                </div>
              ) : null}

              <label className="field">
                <span>Decision note</span>
                <input
                  onChange={(event) => setDecisionNotes((current) => ({ ...current, [item.request_id]: event.target.value }))}
                  placeholder={item.decision_note || "Optional approval, rejection, or payment note"}
                  type="text"
                  value={decisionNotes[item.request_id] ?? ""}
                />
              </label>

              <div className="button-row">
                <button className="button-link" data-pending={isPending} onClick={() => applyDecision(item.request_id, "approved")} type="button">
                  Approve
                </button>
                <button className="button-link ghost" data-pending={isPending} onClick={() => applyDecision(item.request_id, "payment_required")} type="button">
                  Mark payment required
                </button>
                <button className="button-link ghost" data-pending={isPending} onClick={() => applyDecision(item.request_id, "rejected")} type="button">
                  Reject
                </button>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
