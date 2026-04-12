"""
routers/travelport_feedback.py - Desktop feedback endpoints

Mount in main.py with:
    from app.routers import travelport_feedback as travelport_feedback_router
    app.include_router(
        travelport_feedback_router.router,
        prefix="/travelport-agent",
        tags=["Travelport Feedback"],
    )
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from google.cloud import bigquery
from pydantic import BaseModel, Field

from app.repositories import travelport_feedback as feedback_repo

router = APIRouter()


def get_bq_client() -> bigquery.Client:
    import os

    project = os.environ.get("BIGQUERY_PROJECT_ID", "aeropulseintelligence")
    return bigquery.Client(project=project)


class TravelportFeedbackCreate(BaseModel):
    category: str = Field(default="general", min_length=2, max_length=40)
    subject: str = Field(min_length=3, max_length=160)
    message: str = Field(min_length=5, max_length=5000)
    app_version: str | None = Field(default=None, max_length=40)
    device_id: str | None = Field(default=None, max_length=100)
    device_name: str | None = Field(default=None, max_length=100)
    hostname: str | None = Field(default=None, max_length=100)
    os_version: str | None = Field(default=None, max_length=200)
    source: str | None = Field(default="desktop_gui", max_length=50)
    submitted_at_utc: str | None = None
    context: dict[str, Any] | None = None


@router.post("/feedback")
async def create_feedback(
    payload: TravelportFeedbackCreate,
    client: bigquery.Client = Depends(get_bq_client),
):
    """Receive a feedback submission from the desktop GUI."""
    try:
        payload_dict = (
            payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
        )
        return feedback_repo.create_feedback(client, payload_dict)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/feedback")
async def list_feedback(
    limit: int = Query(default=100, ge=1, le=500),
    status: str = Query(default="all", description="all | new | reviewed | resolved"),
    client: bigquery.Client = Depends(get_bq_client),
):
    """Return feedback submissions for the admin page."""
    try:
        return feedback_repo.list_feedback(client, limit=limit, status=status)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
