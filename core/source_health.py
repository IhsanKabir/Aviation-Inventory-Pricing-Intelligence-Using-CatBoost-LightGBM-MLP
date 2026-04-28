from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_MAX_CAPTURE_AGE_HOURS = 8.0


def max_capture_age_hours(env_name: str | None = None) -> float:
    raw = os.getenv(env_name or "", "") if env_name else ""
    if not raw:
        raw = os.getenv("MAX_CAPTURE_AGE_HOURS", str(DEFAULT_MAX_CAPTURE_AGE_HOURS))
    try:
        return max(0.0, float(raw))
    except Exception:
        return DEFAULT_MAX_CAPTURE_AGE_HOURS


def parse_utc(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def file_age_hours(path: str | Path | None) -> float | None:
    if not path:
        return None
    try:
        p = Path(path)
        if not p.exists():
            return None
        mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
        return max(0.0, (datetime.now(timezone.utc) - mtime).total_seconds() / 3600.0)
    except Exception:
        return None


def timestamp_age_hours(value: Any) -> float | None:
    parsed = parse_utc(value)
    if not parsed:
        return None
    return max(0.0, (datetime.now(timezone.utc) - parsed).total_seconds() / 3600.0)


def capture_is_stale(*, generated_at: Any = None, path: str | Path | None = None, max_age_hours: float | None = None) -> dict[str, Any]:
    limit = DEFAULT_MAX_CAPTURE_AGE_HOURS if max_age_hours is None else float(max_age_hours)
    age = timestamp_age_hours(generated_at)
    source = "timestamp"
    if age is None:
        age = file_age_hours(path)
        source = "mtime"
    if age is None:
        return {"known": False, "stale": True, "age_hours": None, "max_age_hours": limit, "age_source": None}
    return {
        "known": True,
        "stale": bool(limit >= 0 and age > limit),
        "age_hours": round(age, 3),
        "max_age_hours": limit,
        "age_source": source,
    }


def ok(source: str, *, message: str = "available", **extra: Any) -> dict[str, Any]:
    return {
        "source": source,
        "ok": True,
        "status": "ok",
        "blocking": False,
        "message": message,
        **extra,
    }


def warn(source: str, *, message: str, blocking: bool = False, **extra: Any) -> dict[str, Any]:
    return {
        "source": source,
        "ok": not blocking,
        "status": "fail" if blocking else "warn",
        "blocking": bool(blocking),
        "message": message,
        **extra,
    }


def stale_capture_result(source: str, *, generated_at: Any = None, path: str | Path | None = None, max_age_env: str | None = None) -> dict[str, Any]:
    state = capture_is_stale(
        generated_at=generated_at,
        path=path,
        max_age_hours=max_capture_age_hours(max_age_env),
    )
    if state["stale"]:
        return warn(
            source,
            message="capture is missing or stale",
            blocking=True,
            manual_action_required=True,
            capture_state=state,
            capture_file=str(path) if path else None,
            generated_at_utc=generated_at,
        )
    return ok(
        source,
        message="fresh capture available",
        manual_action_required=False,
        capture_state=state,
        capture_file=str(path) if path else None,
        generated_at_utc=generated_at,
    )
