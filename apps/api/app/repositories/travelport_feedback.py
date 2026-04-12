"""
repositories/travelport_feedback.py - Travelport desktop feedback storage.

Stores feedback submissions in BigQuery so admins can review them from the web app.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import date, datetime, timezone
from typing import Any

from google.cloud import bigquery


def _feedback_table() -> str:
    project = os.environ.get("BIGQUERY_PROJECT_ID", "aeropulseintelligence")
    dataset = os.environ.get("BIGQUERY_DATASET", "aviation_intel")
    return f"`{project}.{dataset}.ops_travelport_feedback`"


def _serialize(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _rows_to_dicts(rows) -> list[dict]:
    return [{k: _serialize(v) for k, v in dict(row.items()).items()} for row in rows]


def create_feedback(client: bigquery.Client, payload: dict[str, Any]) -> dict[str, Any]:
    """Insert one feedback row and return the stored payload."""
    row = {
        "feedback_id": payload.get("feedback_id") or f"fb_{uuid.uuid4().hex[:12]}",
        "submitted_at_utc": payload.get("submitted_at_utc")
        or datetime.now(timezone.utc).isoformat(),
        "category": str(payload.get("category") or "general").strip().lower(),
        "subject": str(payload.get("subject") or "").strip(),
        "message": str(payload.get("message") or "").strip(),
        "status": str(payload.get("status") or "new").strip().lower(),
        "app_version": str(payload.get("app_version") or "").strip(),
        "device_id": str(payload.get("device_id") or "").strip(),
        "device_name": str(payload.get("device_name") or "").strip(),
        "hostname": str(payload.get("hostname") or "").strip(),
        "os_version": str(payload.get("os_version") or "").strip(),
        "source": str(payload.get("source") or "desktop_gui").strip(),
        "context_json": json.dumps(payload.get("context") or {}, ensure_ascii=False),
        "admin_note": str(payload.get("admin_note") or "").strip(),
    }

    errors = client.insert_rows_json(_feedback_table().strip("`"), [row])
    if errors:
        raise RuntimeError(f"BigQuery feedback insert failed: {errors[:2]}")
    return row


def list_feedback(
    client: bigquery.Client,
    *,
    limit: int = 100,
    status: str | None = None,
) -> list[dict]:
    """Return recent desktop feedback submissions for the admin UI."""
    filters = ["TRUE"]
    params = [bigquery.ScalarQueryParameter("limit", "INT64", limit)]

    if status and status != "all":
        filters.append("status = @status")
        params.append(bigquery.ScalarQueryParameter("status", "STRING", status))

    query = f"""
        SELECT
            feedback_id,
            submitted_at_utc,
            category,
            subject,
            message,
            status,
            app_version,
            device_id,
            device_name,
            hostname,
            os_version,
            source,
            context_json,
            admin_note
        FROM {_feedback_table()}
        WHERE {" AND ".join(filters)}
        ORDER BY submitted_at_utc DESC
        LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return _rows_to_dicts(client.query(query, job_config=job_config).result())
