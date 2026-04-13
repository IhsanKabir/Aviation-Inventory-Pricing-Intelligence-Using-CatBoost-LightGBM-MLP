"use client";

import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useMemo, useState, useTransition } from "react";

import type { AuthenticatedUser, ReportAccessRequest } from "@/lib/api";

function formatStatusLabel(status: ReportAccessRequest["status"]) {
  if (status === "approved") return "Approved";
  if (status === "payment_required") return "Payment required";
  if (status === "rejected") return "Rejected";
  return "Pending review";
}

function statusCopy(request: ReportAccessRequest, resourceLabel: string) {
  if (request.status === "approved") {
    return request.decision_note || `This ${resourceLabel} scope is unlocked.`;
  }
  if (request.status === "payment_required") {
    return request.decision_note || `This request needs payment approval before the ${resourceLabel} view can be opened.`;
  }
  if (request.status === "rejected") {
    return request.decision_note || `This request was rejected. Adjust the ${resourceLabel} scope and submit a new request.`;
  }
  return request.decision_note || "This request is waiting for manual review. Refresh later to check the result.";
}

export function ReportAccessRequestPanel({
  pageKey,
  scope,
  scopeSummary,
  requestWindow,
  request,
  currentUser,
  headline,
  description,
  submitLabel,
  resourceLabel
}: {
  pageKey: string;
  scope: Record<string, unknown>;
  scopeSummary: string[];
  requestWindow?: {
    startDate?: string;
    endDate?: string;
  };
  request: ReportAccessRequest | null;
  currentUser: AuthenticatedUser | null;
  headline: string;
  description: string;
  submitLabel: string;
  resourceLabel: string;
}) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const [isPending, startTransition] = useTransition();
  const [notes, setNotes] = useState("");
  const [submitError, setSubmitError] = useState<string | null>(null);
  const requestedStartDate = requestWindow?.startDate || undefined;
  const requestedEndDate = requestWindow?.endDate || requestWindow?.startDate || undefined;
  const stableScopeSummary = useMemo(() => scopeSummary, [scopeSummary]);

  async function submitRequest() {
    setSubmitError(null);
    try {
      const response = await fetch("/api/user/access-requests", {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          page_key: pageKey,
          requested_start_date: requestedStartDate,
          requested_end_date: requestedEndDate,
          notes: notes || undefined,
          request_scope: scope
        })
      });
      const payload = (await response.json()) as ReportAccessRequest | { detail?: string };
      if (!response.ok) {
        throw new Error("detail" in payload && payload.detail ? payload.detail : `${response.status} ${response.statusText}`);
      }
      if (!("request_id" in payload)) {
        throw new Error("The API did not return a request id.");
      }

      const next = new URLSearchParams(searchParams.toString());
      next.set("request_id", payload.request_id);
      startTransition(() => {
        router.replace(`${pathname}?${next.toString()}`, { scroll: false });
        router.refresh();
      });
    } catch (error) {
      setSubmitError(error instanceof Error ? error.message : "Unable to submit the access request.");
    }
  }

  function refreshRequest() {
    startTransition(() => {
      router.refresh();
    });
  }

  function clearRequest() {
    const next = new URLSearchParams(searchParams.toString());
    next.delete("request_id");
    startTransition(() => {
      router.replace(next.toString() ? `${pathname}?${next.toString()}` : pathname, { scroll: false });
      router.refresh();
    });
  }

  return (
    <div className="filter-form">
      <div className="empty-state">
        <strong>{headline}</strong>
        <div style={{ marginTop: "0.6rem" }}>
          {description}
        </div>
        <div className="table-list" style={{ marginTop: "0.9rem" }}>
          {stableScopeSummary.map((item) => (
            <div className="table-row" key={item}>
              <strong>{item}</strong>
            </div>
          ))}
        </div>
        <div className="mono" style={{ marginTop: "0.8rem" }}>
          Request window: {requestedStartDate ?? "any"} to {requestedEndDate ?? "any"}
        </div>
      </div>

      {request ? (
        <div className={`status-banner ${request.status === "approved" ? "" : "warn"}`}>
          <div className="button-row" style={{ justifyContent: "space-between", alignItems: "center" }}>
            <div>
              <strong>{formatStatusLabel(request.status)}</strong>
            </div>
            <span className={`pill ${request.status === "approved" ? "good" : "warn"}`}>{formatStatusLabel(request.status)}</span>
          </div>
          <div style={{ marginTop: "0.7rem" }}>{statusCopy(request, resourceLabel)}</div>
          {request.requested_start_date || request.requested_end_date ? (
            <div className="mono" style={{ marginTop: "0.55rem" }}>
              Requested window: {request.requested_start_date ?? "any"} to {request.requested_end_date ?? "any"}
            </div>
          ) : null}
          <div className="button-row" style={{ marginTop: "0.9rem" }}>
            <button className="button-link ghost" data-pending={isPending} onClick={refreshRequest} type="button">
              Refresh request status
            </button>
            <button className="button-link ghost" data-pending={isPending} onClick={clearRequest} type="button">
              Start a new request
            </button>
          </div>
        </div>
      ) : null}

      {!request ? (
        <>
          {currentUser ? (
            <div className="status-banner">
              Signed in as <strong>{currentUser.full_name || currentUser.email}</strong>
              <div className="mono" style={{ marginTop: "0.35rem" }}>{currentUser.email}</div>
            </div>
          ) : (
            <div className="status-banner warn">
              Sign in first so this access request can be tracked to a user account.
              <div className="button-row" style={{ marginTop: "0.7rem" }}>
                <a className="button-link ghost" href={`/login?next=${encodeURIComponent(`${pathname}?${searchParams.toString()}`)}`}>
                  Sign in or create account
                </a>
              </div>
            </div>
          )}

          <label className="field">
            <span>Request note</span>
            <input
              onChange={(event) => setNotes(event.target.value)}
              placeholder="Why this window is needed, urgency, payment note, or reporting purpose"
              type="text"
              value={notes}
            />
          </label>

          {submitError ? <div className="status-banner warn">{submitError}</div> : null}

          <div className="button-row">
            <button className="button-link" data-pending={isPending} disabled={!currentUser} onClick={submitRequest} type="button">
              {submitLabel}
            </button>
          </div>
        </>
      ) : null}
    </div>
  );
}
