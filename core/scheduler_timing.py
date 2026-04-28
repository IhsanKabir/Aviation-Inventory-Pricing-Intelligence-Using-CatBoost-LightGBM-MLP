from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from core.source_switches import load_source_switches, source_switch_status


TIME_RE = re.compile(r"^\d{2}:\d{2}$")


@dataclass(frozen=True)
class SchedulerTimingEntry:
    scope_type: str
    scope_id: str
    enabled: bool
    start_time: str
    repeat_minutes: int
    completion_buffer_minutes: int | None = None
    airline: str | None = None
    source: str | None = None
    origin: str | None = None
    destination: str | None = None
    airlines: tuple[str, ...] = ()


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _truthy(value: Any, default: bool = True) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on", "y"}


def _clean_code(value: Any) -> str:
    return str(value or "").strip().upper()


def _clean_source(value: Any) -> str:
    return str(value or "").strip().lower()


def _valid_time(value: Any) -> str:
    text = str(value or "").strip()
    if not TIME_RE.fullmatch(text):
        raise ValueError(f"Invalid scheduler time '{text}'. Expected HH:MM.")
    hour, minute = [int(part) for part in text.split(":", 1)]
    if hour > 23 or minute > 59:
        raise ValueError(f"Invalid scheduler time '{text}'. Expected HH:MM.")
    return text


def _repeat_minutes(value: Any, default: int = 360) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    if parsed < 1:
        raise ValueError("repeat_minutes must be >= 1")
    return parsed


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _airlines_by_primary_source(
    *,
    airlines_file: Path,
    source_switches_file: Path | str | None,
) -> dict[str, list[str]]:
    if not airlines_file.exists():
        return {}
    payload = _read_json(airlines_file)
    switches = load_source_switches(source_switches_file)
    out: dict[str, list[str]] = {}
    for row in payload if isinstance(payload, list) else []:
        if not isinstance(row, dict) or not row.get("enabled"):
            continue
        code = _clean_code(row.get("code"))
        source = _clean_source(row.get("module"))
        if not code or not source:
            continue
        if not source_switch_status(source, switches=switches).get("enabled"):
            continue
        out.setdefault(source, []).append(code)
    return {key: sorted(dict.fromkeys(values)) for key, values in out.items()}


def _iter_source_entries(raw: Any) -> Iterable[tuple[str, dict[str, Any]]]:
    if isinstance(raw, dict):
        for key, value in raw.items():
            if isinstance(value, dict):
                yield str(value.get("source") or key), value
    elif isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                source = _clean_source(item.get("source") or item.get("id") or item.get("name"))
                if source:
                    yield source, item


def _iter_airline_entries(raw: Any) -> Iterable[tuple[str, dict[str, Any]]]:
    if isinstance(raw, dict):
        for key, value in raw.items():
            if isinstance(value, dict):
                yield _clean_code(value.get("airline") or key), value
    elif isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                airline = _clean_code(item.get("airline") or item.get("id") or item.get("code"))
                if airline:
                    yield airline, item


def _route_scope_id(airline: str, origin: str, destination: str) -> str:
    return f"{airline}_{origin}_{destination}"


def load_scheduler_timing_plan(
    *,
    schedule_file: Path,
    airlines_file: Path,
    source_switches_file: Path | str | None = None,
) -> dict[str, Any]:
    schedule = _read_json(schedule_file) if schedule_file.exists() else {}
    root = schedule.get("scheduler_timing") if isinstance(schedule, dict) else {}
    root = root if isinstance(root, dict) else {}
    task_windows = schedule.get("task_windows") if isinstance(schedule.get("task_windows"), dict) else {}
    ingestion_window = task_windows.get("ingestion") if isinstance(task_windows.get("ingestion"), dict) else {}

    enabled = _truthy(root.get("enabled"), default=True)
    global_raw = root.get("global") if isinstance(root.get("global"), dict) else {}
    fallback_repeat = ingestion_window.get("repeat_minutes") or int(float(schedule.get("auto_run_interval_hours") or 6) * 60)
    global_start = global_raw.get("start_time") or ingestion_window.get("start_time") or "00:05"
    global_repeat = global_raw.get("repeat_minutes") or fallback_repeat
    global_entry = SchedulerTimingEntry(
        scope_type="global",
        scope_id="global",
        enabled=enabled and _truthy(global_raw.get("enabled"), default=True),
        start_time=_valid_time(global_start),
        repeat_minutes=_repeat_minutes(global_repeat, default=360),
        completion_buffer_minutes=_optional_int(global_raw.get("completion_buffer_minutes")),
    )

    source_airlines = _airlines_by_primary_source(
        airlines_file=airlines_file,
        source_switches_file=source_switches_file,
    )
    entries = [global_entry]

    for source, cfg in _iter_source_entries(root.get("sources")):
        source_key = _clean_source(source)
        airlines = tuple(_clean_code(x) for x in (cfg.get("airlines") or source_airlines.get(source_key) or []) if _clean_code(x))
        entries.append(
            SchedulerTimingEntry(
                scope_type="source",
                scope_id=source_key,
                enabled=enabled and _truthy(cfg.get("enabled"), default=False),
                start_time=_valid_time(cfg.get("start_time") or global_entry.start_time),
                repeat_minutes=_repeat_minutes(cfg.get("repeat_minutes") or global_entry.repeat_minutes),
                completion_buffer_minutes=_optional_int(cfg.get("completion_buffer_minutes")),
                source=source_key,
                airlines=tuple(sorted(dict.fromkeys(airlines))),
            )
        )

    for airline, cfg in _iter_airline_entries(root.get("airlines")):
        entries.append(
            SchedulerTimingEntry(
                scope_type="airline",
                scope_id=airline,
                enabled=enabled and _truthy(cfg.get("enabled"), default=False),
                start_time=_valid_time(cfg.get("start_time") or global_entry.start_time),
                repeat_minutes=_repeat_minutes(cfg.get("repeat_minutes") or global_entry.repeat_minutes),
                completion_buffer_minutes=_optional_int(cfg.get("completion_buffer_minutes")),
                airline=airline,
                airlines=(airline,),
            )
        )

    routes_raw = root.get("routes") if isinstance(root.get("routes"), list) else []
    for cfg in routes_raw:
        if not isinstance(cfg, dict):
            continue
        airline = _clean_code(cfg.get("airline"))
        origin = _clean_code(cfg.get("origin"))
        destination = _clean_code(cfg.get("destination"))
        if not airline or not origin or not destination:
            raise ValueError("Each scheduler_timing.routes entry requires airline, origin, and destination.")
        entries.append(
            SchedulerTimingEntry(
                scope_type="route",
                scope_id=str(cfg.get("id") or _route_scope_id(airline, origin, destination)),
                enabled=enabled and _truthy(cfg.get("enabled"), default=False),
                start_time=_valid_time(cfg.get("start_time") or global_entry.start_time),
                repeat_minutes=_repeat_minutes(cfg.get("repeat_minutes") or global_entry.repeat_minutes),
                completion_buffer_minutes=_optional_int(cfg.get("completion_buffer_minutes")),
                airline=airline,
                origin=origin,
                destination=destination,
                airlines=(airline,),
            )
        )

    return {
        "enabled": enabled,
        "timezone": str(root.get("timezone") or "local"),
        "entries": entries,
    }


def find_timing_entry(plan: dict[str, Any], *, scope_type: str, scope_id: str) -> SchedulerTimingEntry | None:
    wanted_type = str(scope_type or "").strip().lower()
    wanted_id = str(scope_id or "").strip().lower()
    for entry in plan.get("entries") or []:
        if not isinstance(entry, SchedulerTimingEntry):
            continue
        if entry.scope_type.lower() == wanted_type and entry.scope_id.lower() == wanted_id:
            return entry
    return None


def pipeline_filter_args(entry: SchedulerTimingEntry) -> list[str]:
    args: list[str] = []
    if entry.scope_type == "source":
        if not entry.airlines:
            return []
        args.extend(["--airline", ",".join(entry.airlines)])
    elif entry.scope_type == "airline" and entry.airline:
        args.extend(["--airline", entry.airline])
    elif entry.scope_type == "route" and entry.airline and entry.origin and entry.destination:
        args.extend(["--airline", entry.airline, "--origin", entry.origin, "--destination", entry.destination])
    return args


def timing_entry_to_dict(entry: SchedulerTimingEntry) -> dict[str, Any]:
    return {
        "scope_type": entry.scope_type,
        "scope_id": entry.scope_id,
        "enabled": entry.enabled,
        "start_time": entry.start_time,
        "repeat_minutes": entry.repeat_minutes,
        "completion_buffer_minutes": entry.completion_buffer_minutes,
        "source": entry.source,
        "airline": entry.airline,
        "origin": entry.origin,
        "destination": entry.destination,
        "airlines": list(entry.airlines),
        "pipeline_filter_args": pipeline_filter_args(entry),
    }
