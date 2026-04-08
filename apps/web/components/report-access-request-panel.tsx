"use client";

import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useMemo, useState, useTransition } from "react";

import type { AuthenticatedUser, ReportAccessRequest } from "@/lib/api";

type RouteAccessScope = {
  cycleId?: string;
  airlines: string[];
  routePairs?: string[];
  origin?: string;
  destination?: string;
  cabin?: string;
  tripType: string;
  startDate?: string;
  endDate?: string;
  returnScope: string;
  returnDate?: string;
  returnDateStart?: string;
  returnDateEnd?: string;
  routeLimit: number;
  historyLimit: number;
};

function buildScopePayload(scope: RouteAccessScope) {
  const hasExactRoutePairs = Boolean(scope.routePairs?.length);
  return {
    cycle_id: scope.cycleId,
    airline: scope.airlines,
    route_pair: scope.routePairs,
    origin: hasExactRoutePairs ? undefined : scope.origin,
    destination: hasExactRoutePairs ? undefined : scope.destination,
    cabin: scope.cabin,
    trip_type: scope.tripType,
    start_date: scope.startDate,
    end_date: scope.endDate,
    return_scope: scope.returnScope,
    return_date: scope.returnDate,
    return_date_start: scope.returnDateStart,
    return_date_end: scope.returnDateEnd,
    route_limit: scope.routeLimit,
    history_limit: scope.historyLimit
  };
}

function formatStatusLabel(status: ReportAccessRequest["status"]) {
  if (status === "approved") return "Approved";
  if (status === "payment_required") return "Payment required";
  if (status === "rejected") return "Rejected";
  return "Pending review";
}

function statusCopy(request: ReportAccessRequest) {
  if (request.status === "approved") {
    return request.decision_note || "This scope is unlocked. The selected route view can now be opened.";
  }
  if (request.status === "payment_required") {
    return request.decision_note || "This request needs payment approval before the route view can be opened.";
  }
  if (request.status === "rejected") {
    return request.decision_note || "This request was rejected. Adjust the route or travel window and submit a new request.";
  }
  return request.decision_note || "This request is waiting for manual review. Refresh later to check the result.";
}

export function ReportAccessRequestPanel({
  scope,
  request,
  currentUser
}: {
  scope: RouteAccessScope;
  request: ReportAccessRequest | null;
  currentUser: AuthenticatedUser | null;
}) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const [isPending, startTransition] = useTransition();
  const [notes, setNotes] = useState("");
  const [submitError, setSubmitError] = useState<string | null>(null);
  const requestedStartDate = scope.startDate || undefined;
  const requestedEndDate = scope.endDate || scope.startDate || undefined;

  const scopeSummary = useMemo(() => {
    const lines: string[] = [];
    if (scope.routePairs?.length) {
      lines.push(`Routes: ${scope.routePairs.join(", ")}`);
    } else {
      lines.push(`Route: ${scope.origin || "any"} -> ${scope.destination || "any"}`);
    }
    lines.push(`Trip: ${scope.tripType === "RT" ? "Round-trip" : "One-way"}`);
    if (scope.airlines.length) {
      lines.push(`Airlines: ${scope.airlines.join(", ")}`);
    }
    if (scope.cabin) {
      lines.push(`Cabin: ${scope.cabin}`);
    }
    if (scope.cycleId) {
      lines.push("Saved update selected");
    }
    if (scope.startDate || scope.endDate) {
      lines.push(`Outbound window: ${scope.startDate ?? "any"} to ${scope.endDate ?? "any"}`);
    } else {
      lines.push("Outbound window: all collected outbound dates");
    }
    if (scope.tripType === "RT") {
      if (scope.returnDateStart || scope.returnDateEnd) {
        lines.push(`Inbound window: ${scope.returnDateStart ?? "any"} to ${scope.returnDateEnd ?? "any"}`);
      } else {
        lines.push("Inbound window: all collected inbound dates");
      }
    }
    lines.push(`View size: ${scope.routeLimit} route blocks | ${scope.historyLimit} history rows`);
    return lines;
  }, [scope]);

  async function submitRequest() {
    setSubmitError(null);
    try {
      const response = await fetch("/api/user/access-requests", {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          page_key: "routes",
          requested_start_date: requestedStartDate,
          requested_end_date: requestedEndDate,
          notes: notes || undefined,
          request_scope: buildScopePayload(scope)
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
        <strong>Route access requires approval.</strong>
        <div style={{ marginTop: "0.6rem" }}>
          Submit the route and travel window first. After approval, this page will unlock the route view.
        </div>
        <div className="table-list" style={{ marginTop: "0.9rem" }}>
      {scopeSummary.map((item) => (
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
          <div style={{ marginTop: "0.7rem" }}>{statusCopy(request)}</div>
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
              Sign in first so this route-data request can be tracked to a user account.
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
              Submit route data request
            </button>
          </div>
        </>
      ) : null}
    </div>
  );
}
