"use client";

import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useMemo, useState, useTransition } from "react";

import { getApiBaseUrl, type ReportAccessRequest } from "@/lib/api";

type RouteAccessScope = {
  cycleId?: string;
  airlines: string[];
  origin?: string;
  destination?: string;
  cabin?: string;
  tripType: string;
  returnScope: string;
  returnDate?: string;
  returnDateStart?: string;
  returnDateEnd?: string;
  routeLimit: number;
  historyLimit: number;
};

function buildScopePayload(scope: RouteAccessScope) {
  return {
    cycle_id: scope.cycleId,
    airline: scope.airlines,
    origin: scope.origin,
    destination: scope.destination,
    cabin: scope.cabin,
    trip_type: scope.tripType,
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
    return request.decision_note || "This scope is unlocked. The page can now load route data for this approved request.";
  }
  if (request.status === "payment_required") {
    return request.decision_note || "This request needs manual payment approval before route data can be shown.";
  }
  if (request.status === "rejected") {
    return request.decision_note || "This request was rejected. Adjust the scope or date window and submit a new request.";
  }
  return request.decision_note || "This request is waiting for manual review. Refresh later after the owner checks BigQuery free-tier headroom.";
}

export function ReportAccessRequestPanel({
  scope,
  request
}: {
  scope: RouteAccessScope;
  request: ReportAccessRequest | null;
}) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const [isPending, startTransition] = useTransition();
  const [requesterName, setRequesterName] = useState("");
  const [requesterContact, setRequesterContact] = useState("");
  const [requestedStartDate, setRequestedStartDate] = useState("");
  const [requestedEndDate, setRequestedEndDate] = useState("");
  const [notes, setNotes] = useState("");
  const [submitError, setSubmitError] = useState<string | null>(null);

  const scopeSummary = useMemo(() => {
    const lines: string[] = [];
    lines.push(`Route: ${scope.origin || "any"} -> ${scope.destination || "any"}`);
    lines.push(`Trip: ${scope.tripType === "RT" ? "Round-trip" : "One-way"}`);
    if (scope.airlines.length) {
      lines.push(`Airlines: ${scope.airlines.join(", ")}`);
    }
    if (scope.cabin) {
      lines.push(`Cabin: ${scope.cabin}`);
    }
    if (scope.cycleId) {
      lines.push(`Cycle: ${scope.cycleId}`);
    }
    lines.push(`Matrix scope: ${scope.routeLimit} route blocks | ${scope.historyLimit} capture depth`);
    return lines;
  }, [scope]);

  async function submitRequest() {
    if (!requestedStartDate || !requestedEndDate) {
      setSubmitError("Start and end dates are required before the request can be submitted.");
      return;
    }
    setSubmitError(null);
    try {
      const response = await fetch(`${getApiBaseUrl()}/api/v1/access-requests`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          page_key: "routes",
          requester_name: requesterName || undefined,
          requester_contact: requesterContact || undefined,
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
        <strong>Route data is request-gated.</strong>
        <div style={{ marginTop: "0.6rem" }}>
          Submit the date window and current route scope first. After approval, this same page will unlock the route data.
        </div>
        <div className="table-list" style={{ marginTop: "0.9rem" }}>
          {scopeSummary.map((item) => (
            <div className="table-row" key={item}>
              <strong>{item}</strong>
            </div>
          ))}
        </div>
      </div>

      {request ? (
        <div className={`status-banner ${request.status === "approved" ? "" : "warn"}`}>
          <div className="button-row" style={{ justifyContent: "space-between", alignItems: "center" }}>
            <div>
              <strong>{formatStatusLabel(request.status)}</strong>
              <div className="mono">Request ID: {request.request_id}</div>
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
            {request.status !== "approved" ? (
              <button className="button-link ghost" data-pending={isPending} onClick={clearRequest} type="button">
                Start a new request
              </button>
            ) : null}
          </div>
        </div>
      ) : null}

      {!request ? (
        <>
          <div className="field-grid">
            <label className="field">
              <span>Requester name</span>
              <input onChange={(event) => setRequesterName(event.target.value)} placeholder="Optional" type="text" value={requesterName} />
            </label>
            <label className="field">
              <span>Contact</span>
              <input onChange={(event) => setRequesterContact(event.target.value)} placeholder="Email or phone" type="text" value={requesterContact} />
            </label>
          </div>

          <div className="field-grid">
            <label className="field">
              <span>Requested start date</span>
              <input onChange={(event) => setRequestedStartDate(event.target.value)} type="date" value={requestedStartDate} />
            </label>
            <label className="field">
              <span>Requested end date</span>
              <input onChange={(event) => setRequestedEndDate(event.target.value)} type="date" value={requestedEndDate} />
            </label>
          </div>

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
            <button className="button-link" data-pending={isPending} onClick={submitRequest} type="button">
              Submit route data request
            </button>
          </div>
        </>
      ) : null}
    </div>
  );
}
