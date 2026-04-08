from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session


VALID_PAGE_KEYS = {"routes"}
VALID_STATUSES = {"pending", "approved", "rejected", "payment_required"}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def ensure_tables(engine: Engine | None) -> None:
    if engine is None:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS report_access_requests (
                    request_id TEXT PRIMARY KEY,
                    page_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    requester_user_id TEXT NULL,
                    requester_email TEXT NULL,
                    requester_name TEXT NULL,
                    requester_contact TEXT NULL,
                    requested_start_date DATE NULL,
                    requested_end_date DATE NULL,
                    notes TEXT NULL,
                    request_scope JSONB NOT NULL DEFAULT '{}'::jsonb,
                    decision_note TEXT NULL,
                    decided_at_utc TIMESTAMPTZ NULL,
                    created_at_utc TIMESTAMPTZ NOT NULL,
                    updated_at_utc TIMESTAMPTZ NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS ix_report_access_requests_status_created
                ON report_access_requests (status, created_at_utc DESC)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS ix_report_access_requests_page_created
                ON report_access_requests (page_key, created_at_utc DESC)
                """
            )
        )
        conn.execute(text("ALTER TABLE report_access_requests ADD COLUMN IF NOT EXISTS requester_user_id TEXT NULL"))
        conn.execute(text("ALTER TABLE report_access_requests ADD COLUMN IF NOT EXISTS requester_email TEXT NULL"))


def _normalize_text(value: Any) -> str | None:
    text_value = str(value or "").strip()
    return text_value or None


def _normalize_scalar(value: Any) -> str | None:
    if isinstance(value, (list, tuple, set)):
        for item in value:
            normalized = _normalize_text(item)
            if normalized:
                return normalized
        return None
    return _normalize_text(value)


def _normalize_code_list(values: Any) -> list[str]:
    if not values:
        return []
    normalized = {
        str(value or "").strip().upper()
        for value in values
        if str(value or "").strip()
    }
    return sorted(normalized)


def _normalize_route_pair_list(values: Any) -> list[str]:
    if not values:
        return []
    normalized: set[str] = set()
    for value in values:
        cleaned = str(value or "").strip().upper()
        if not cleaned:
            continue
        if "-" not in cleaned:
            continue
        origin, destination = cleaned.split("-", 1)
        origin = origin.strip().upper()
        destination = destination.strip().upper()
        if origin and destination:
            normalized.add(f"{origin}-{destination}")
    return sorted(normalized)


def normalize_request_scope(scope: dict[str, Any] | None) -> dict[str, Any]:
    raw = scope or {}
    normalized_origin = _normalize_scalar(raw.get("origin"))
    normalized_destination = _normalize_scalar(raw.get("destination"))
    normalized_route_pairs = _normalize_route_pair_list(raw.get("route_pair"))
    if not normalized_route_pairs and normalized_origin and normalized_destination:
        normalized_route_pairs = [f"{normalized_origin.upper()}-{normalized_destination.upper()}"]
    normalized: dict[str, Any] = {
        "cycle_id": _normalize_scalar(raw.get("cycle_id")),
        "airline": _normalize_code_list(raw.get("airline")),
        "origin": normalized_origin,
        "destination": normalized_destination,
        "route_pair": normalized_route_pairs,
        "cabin": _normalize_scalar(raw.get("cabin")),
        "trip_type": _normalize_scalar(raw.get("trip_type")),
        "start_date": _normalize_scalar(raw.get("start_date")),
        "end_date": _normalize_scalar(raw.get("end_date")),
        "return_scope": _normalize_scalar(raw.get("return_scope")),
        "return_date": _normalize_scalar(raw.get("return_date")),
        "return_date_start": _normalize_scalar(raw.get("return_date_start")),
        "return_date_end": _normalize_scalar(raw.get("return_date_end")),
    }
    route_limit = raw.get("route_limit")
    history_limit = raw.get("history_limit")
    if route_limit is not None and str(route_limit).strip():
        try:
            normalized["route_limit"] = max(1, int(route_limit))
        except Exception:
            pass
    if history_limit is not None and str(history_limit).strip():
        try:
            normalized["history_limit"] = max(1, int(history_limit))
        except Exception:
            pass

    return {key: value for key, value in normalized.items() if value not in (None, [], "")}


def _row_to_payload(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    scope = row.get("request_scope")
    if isinstance(scope, str):
        try:
            scope = json.loads(scope)
        except Exception:
            scope = {}
    if not isinstance(scope, dict):
        scope = {}
    return {
        "request_id": row.get("request_id"),
        "page_key": row.get("page_key"),
        "status": row.get("status"),
        "requester_user_id": row.get("requester_user_id"),
        "requester_email": row.get("requester_email"),
        "requester_name": row.get("requester_name"),
        "requester_contact": row.get("requester_contact"),
        "requested_start_date": row.get("requested_start_date").isoformat() if row.get("requested_start_date") else None,
        "requested_end_date": row.get("requested_end_date").isoformat() if row.get("requested_end_date") else None,
        "notes": row.get("notes"),
        "request_scope": normalize_request_scope(scope),
        "decision_note": row.get("decision_note"),
        "decided_at_utc": row.get("decided_at_utc").isoformat() if row.get("decided_at_utc") else None,
        "created_at_utc": row.get("created_at_utc").isoformat() if row.get("created_at_utc") else None,
        "updated_at_utc": row.get("updated_at_utc").isoformat() if row.get("updated_at_utc") else None,
    }


def create_request(
    db: Session,
    *,
    page_key: str,
    requester_name: str | None,
    requester_contact: str | None,
    requester_user_id: str | None = None,
    requester_email: str | None = None,
    requested_start_date: date | None,
    requested_end_date: date | None,
    notes: str | None,
    request_scope: dict[str, Any] | None,
) -> dict[str, Any]:
    normalized_page_key = _normalize_text(page_key)
    if normalized_page_key not in VALID_PAGE_KEYS:
        raise ValueError("Unsupported page key")
    now = _utcnow()
    request_id = str(uuid4())
    normalized_scope = normalize_request_scope(request_scope)
    db.execute(
        text(
            """
            INSERT INTO report_access_requests (
                request_id,
                page_key,
                status,
                requester_user_id,
                requester_email,
                requester_name,
                requester_contact,
                requested_start_date,
                requested_end_date,
                notes,
                request_scope,
                decision_note,
                decided_at_utc,
                created_at_utc,
                updated_at_utc
            )
            VALUES (
                :request_id,
                :page_key,
                'pending',
                :requester_user_id,
                :requester_email,
                :requester_name,
                :requester_contact,
                :requested_start_date,
                :requested_end_date,
                :notes,
                CAST(:request_scope AS JSONB),
                NULL,
                NULL,
                :created_at_utc,
                :updated_at_utc
            )
            """
        ),
        {
            "request_id": request_id,
            "page_key": normalized_page_key,
            "requester_user_id": _normalize_text(requester_user_id),
            "requester_email": _normalize_text(requester_email),
            "requester_name": _normalize_text(requester_name),
            "requester_contact": _normalize_text(requester_contact),
            "requested_start_date": requested_start_date,
            "requested_end_date": requested_end_date,
            "notes": _normalize_text(notes),
            "request_scope": json.dumps(normalized_scope),
            "created_at_utc": now,
            "updated_at_utc": now,
        },
    )
    db.commit()
    payload = get_request(db, request_id)
    if not payload:
        raise RuntimeError("Failed to load created access request")
    return payload


def get_request(db: Session, request_id: str) -> dict[str, Any] | None:
    row = (
        db.execute(
            text(
                """
                SELECT
                    request_id,
                    page_key,
                    status,
                    requester_user_id,
                    requester_email,
                    requester_name,
                    requester_contact,
                    requested_start_date,
                    requested_end_date,
                    notes,
                    request_scope,
                    decision_note,
                    decided_at_utc,
                    created_at_utc,
                    updated_at_utc
                FROM report_access_requests
                WHERE request_id = :request_id
                """
            ),
            {"request_id": request_id},
        )
        .mappings()
        .first()
    )
    return _row_to_payload(dict(row)) if row else None


def list_requests(
    db: Session,
    *,
    status: str | None = None,
    page_key: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    normalized_status = _normalize_text(status)
    if normalized_status and normalized_status not in VALID_STATUSES:
        raise ValueError("Unsupported access-request status")

    normalized_page_key = _normalize_text(page_key)
    if normalized_page_key and normalized_page_key not in VALID_PAGE_KEYS:
        raise ValueError("Unsupported page key")

    capped_limit = max(1, min(int(limit or 100), 500))
    filters: list[str] = []
    params: dict[str, Any] = {"limit": capped_limit}

    if normalized_status:
        filters.append("status = :status")
        params["status"] = normalized_status
    if normalized_page_key:
        filters.append("page_key = :page_key")
        params["page_key"] = normalized_page_key

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    rows = db.execute(
        text(
            f"""
            SELECT
                request_id,
                page_key,
                status,
                requester_user_id,
                requester_email,
                requester_name,
                requester_contact,
                requested_start_date,
                requested_end_date,
                notes,
                request_scope,
                decision_note,
                decided_at_utc,
                created_at_utc,
                updated_at_utc
            FROM report_access_requests
            {where_clause}
            ORDER BY
                CASE status
                    WHEN 'pending' THEN 0
                    WHEN 'payment_required' THEN 1
                    WHEN 'approved' THEN 2
                    WHEN 'rejected' THEN 3
                    ELSE 4
                END,
                created_at_utc DESC
            LIMIT :limit
            """
        ),
        params,
    ).mappings().all()
    return [_row_to_payload(dict(row)) for row in rows if row]


def update_request_status(
    db: Session,
    *,
    request_id: str,
    status: str,
    decision_note: str | None,
) -> dict[str, Any] | None:
    normalized_status = _normalize_text(status)
    if normalized_status not in VALID_STATUSES:
        raise ValueError("Unsupported access-request status")
    now = _utcnow()
    db.execute(
        text(
            """
            UPDATE report_access_requests
            SET
                status = :status,
                decision_note = :decision_note,
                decided_at_utc = :decided_at_utc,
                updated_at_utc = :updated_at_utc
            WHERE request_id = :request_id
            """
        ),
        {
            "request_id": request_id,
            "status": normalized_status,
            "decision_note": _normalize_text(decision_note),
            "decided_at_utc": now,
            "updated_at_utc": now,
        },
    )
    db.commit()
    return get_request(db, request_id)


def require_approved_request(
    db: Session,
    *,
    request_id: str | None,
    page_key: str,
    scope: dict[str, Any] | None,
) -> dict[str, Any]:
    normalized_page_key = _normalize_text(page_key)
    if normalized_page_key not in VALID_PAGE_KEYS:
        raise ValueError("Unsupported page key")
    normalized_request_id = _normalize_text(request_id)
    if not normalized_request_id:
        raise PermissionError("An approved request is required before route data can be shown.")
    request_payload = get_request(db, normalized_request_id)
    if not request_payload:
        raise LookupError("Access request not found.")
    if request_payload.get("page_key") != normalized_page_key:
        raise PermissionError("This request does not unlock the selected page.")
    status = request_payload.get("status")
    if status == "payment_required":
        raise PermissionError(request_payload.get("decision_note") or "Payment is required before this request can be unlocked.")
    if status != "approved":
        raise PermissionError("This request has not been approved yet.")

    approved_scope = normalize_request_scope(request_payload.get("request_scope"))
    current_scope = normalize_request_scope(scope)
    if not _scope_matches(approved_scope, current_scope):
        raise PermissionError("The current scope does not match the approved request.")
    return request_payload


def _scope_matches(approved_scope: dict[str, Any], current_scope: dict[str, Any]) -> bool:
    approved_route_pairs = approved_scope.get("route_pair") or []
    current_route_pairs = current_scope.get("route_pair") or []
    if approved_route_pairs:
        if not current_route_pairs:
            return False
        if not set(current_route_pairs).issubset(set(approved_route_pairs)):
            return False

    for key in ("cycle_id", "origin", "destination", "cabin", "trip_type", "return_date"):
        approved_value = approved_scope.get(key)
        if approved_value and current_scope.get(key) != approved_value:
            return False

    approved_start_date = approved_scope.get("start_date")
    current_start_date = current_scope.get("start_date")
    if approved_start_date:
        if not current_start_date or current_start_date < approved_start_date:
            return False

    approved_end_date = approved_scope.get("end_date")
    current_end_date = current_scope.get("end_date")
    if approved_end_date:
        if not current_end_date or current_end_date > approved_end_date:
            return False

    approved_return_start = approved_scope.get("return_date_start")
    current_return_start = current_scope.get("return_date_start")
    if approved_return_start:
        if not current_return_start or current_return_start < approved_return_start:
            return False

    approved_return_end = approved_scope.get("return_date_end")
    current_return_end = current_scope.get("return_date_end")
    if approved_return_end:
        if not current_return_end or current_return_end > approved_return_end:
            return False

    approved_airlines = approved_scope.get("airline") or []
    current_airlines = current_scope.get("airline") or []
    if approved_airlines and current_airlines != approved_airlines:
        return False

    approved_route_limit = approved_scope.get("route_limit")
    current_route_limit = current_scope.get("route_limit")
    if approved_route_limit is not None and current_route_limit is not None and int(current_route_limit) > int(approved_route_limit):
        return False

    approved_history_limit = approved_scope.get("history_limit")
    current_history_limit = current_scope.get("history_limit")
    if approved_history_limit is not None and current_history_limit is not None and int(current_history_limit) > int(approved_history_limit):
        return False

    return True
