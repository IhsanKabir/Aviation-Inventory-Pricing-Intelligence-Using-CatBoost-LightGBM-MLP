from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import socket
import subprocess
import sys
from pathlib import Path


ACTIVE_PIPELINE_MARKERS = ("run_pipeline.py", "run_all.py", "generate_reports.py", "parallel_airline_runner.py")


def parse_args():
    p = argparse.ArgumentParser(
        description="Detect stale/interrupted accumulation runs and optionally relaunch safely."
    )
    p.add_argument("--mode", choices=["preflight", "recover", "guarded-run"], required=True)
    p.add_argument("--python-exe", default=sys.executable)
    p.add_argument("--root", default=None, help="Repo root (defaults to project root inferred from this file)")
    p.add_argument("--reports-dir", default="output/reports")
    p.add_argument(
        "--status-file",
        default=None,
        help="Heartbeat status JSON (defaults to reports-dir/run_all_accumulation_status_latest.json)",
    )
    p.add_argument(
        "--state-file",
        default=None,
        help="Recovery state JSON (defaults to reports-dir/accumulation_recovery_state.json)",
    )
    p.add_argument(
        "--output-json",
        default=None,
        help="Latest recovery status JSON (defaults to reports-dir/accumulation_recovery_latest.json)",
    )
    p.add_argument(
        "--lock-file",
        default=None,
        help="Wrapper lock JSON (defaults to reports-dir/accumulation_wrapper_lock.json)",
    )
    p.add_argument(
        "--stale-minutes",
        type=float,
        default=15.0,
        help="Treat heartbeat as stale running state after this many minutes with no updates",
    )
    p.add_argument(
        "--max-idle-minutes",
        type=float,
        default=300.0,
        help="If no active accumulation and heartbeat age exceeds this, start a recovery cycle",
    )
    p.add_argument(
        "--cooldown-minutes",
        type=float,
        default=60.0,
        help="Minimum time between recovery launch attempts",
    )
    p.add_argument(
        "--min-completed-gap-minutes",
        type=float,
        default=float(os.getenv("ACCUMULATION_COMPLETION_BUFFER_MINUTES", "72")),
        help="Minimum buffer after a completed accumulation before another cycle may start",
    )
    p.add_argument(
        "--lock-stale-minutes",
        type=float,
        default=10.0,
        help="Minimum lock age before a wrapper lock may be treated as stale if no active pipeline exists",
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("command", nargs=argparse.REMAINDER)
    return p.parse_args()


def _repo_root(args) -> Path:
    if args.root:
        return Path(args.root).resolve()
    return Path(__file__).resolve().parents[1]


def _reports_dir(root: Path, reports_dir: str) -> Path:
    p = Path(reports_dir)
    if p.is_absolute():
        return p
    return (root / p).resolve()


def _status_path(args, reports_dir: Path) -> Path:
    if args.status_file:
        p = Path(args.status_file)
        return p if p.is_absolute() else (Path.cwd() / p).resolve()
    return reports_dir / "run_all_accumulation_status_latest.json"


def _state_path(args, reports_dir: Path) -> Path:
    if args.state_file:
        p = Path(args.state_file)
        return p if p.is_absolute() else (Path.cwd() / p).resolve()
    return reports_dir / "accumulation_recovery_state.json"


def _output_path(args, reports_dir: Path) -> Path:
    if args.output_json:
        p = Path(args.output_json)
        return p if p.is_absolute() else (Path.cwd() / p).resolve()
    return reports_dir / "accumulation_recovery_latest.json"


def _cycle_state_path(reports_dir: Path) -> Path:
    return reports_dir / "accumulation_cycle_latest.json"


def _lock_path(args, reports_dir: Path) -> Path:
    if args.lock_file:
        p = Path(args.lock_file)
        return p if p.is_absolute() else (Path.cwd() / p).resolve()
    return reports_dir / "accumulation_wrapper_lock.json"


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _read_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    except Exception:
        return {}
    return values


def _write_json(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _parse_iso(ts: str | None):
    if not ts:
        return None
    try:
        return dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        return None


def _now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _db_connection_target(root: Path) -> tuple[str, int]:
    env = _read_env_file(root / ".env")
    host = env.get("DB_HOST") or os.getenv("DB_HOST") or "localhost"
    port_raw = env.get("DB_PORT") or os.getenv("DB_PORT") or "5432"
    try:
        port = int(port_raw)
    except Exception:
        port = 5432
    return host, port


def _db_local_host(host: str) -> bool:
    return str(host or "").strip().lower() in {"", ".", "localhost", "127.0.0.1", "::1"}


def _db_port_reachable(host: str, port: int, timeout_seconds: float = 2.0) -> tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return True, "db_reachable"
    except OSError as exc:
        return False, f"db_unreachable:{exc}"


def _powershell_json(command: str):
    try:
        raw = subprocess.check_output(
            ["powershell.exe", "-NoProfile", "-Command", command],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return None
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def _candidate_postgres_service_names(root: Path) -> list[str]:
    env = _read_env_file(root / ".env")
    raw_names = [
        env.get("POSTGRES_SERVICE_NAME"),
        os.getenv("POSTGRES_SERVICE_NAME"),
        env.get("POSTGRES_WINDOWS_SERVICE_NAME"),
        os.getenv("POSTGRES_WINDOWS_SERVICE_NAME"),
        "postgresql-x64-18",
    ]
    seen: set[str] = set()
    names: list[str] = []
    for raw in raw_names:
        name = str(raw or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        names.append(name)
    return names


def _discover_windows_postgres_service(root: Path) -> dict:
    candidates = _candidate_postgres_service_names(root)
    for candidate in candidates:
        data = _powershell_json(
            f"$s=Get-Service -Name '{candidate}' -ErrorAction SilentlyContinue | "
            "Select-Object Name,Status; $s | ConvertTo-Json -Compress"
        )
        if isinstance(data, dict) and data.get("Name"):
            return {"name": str(data.get("Name")), "status": str(data.get("Status") or "")}
    data = _powershell_json(
        "$s=Get-Service -Name 'postgresql*' -ErrorAction SilentlyContinue | "
        "Select-Object Name,Status; $s | ConvertTo-Json -Compress"
    )
    if isinstance(data, list):
        for item in data:
            if not isinstance(item, dict) or not item.get("Name"):
                continue
            if str(item.get("Status") or "").strip().lower() == "running":
                return {"name": str(item.get("Name")), "status": str(item.get("Status") or "")}
        first = next((item for item in data if isinstance(item, dict) and item.get("Name")), None)
        if first:
            return {"name": str(first.get("Name")), "status": str(first.get("Status") or "")}
    if isinstance(data, dict) and data.get("Name"):
        return {"name": str(data.get("Name")), "status": str(data.get("Status") or "")}
    return {}


def _attempt_windows_postgres_restart(root: Path) -> dict:
    service = _discover_windows_postgres_service(root)
    service_name = str(service.get("name") or "")
    service_status_before = str(service.get("status") or "")
    payload = {
        "attempted": False,
        "succeeded": False,
        "failed": False,
        "service_name": service_name or None,
        "service_status_before": service_status_before or None,
        "service_status_after": None,
        "detail": "restart_not_attempted",
    }
    if not service_name:
        payload["failed"] = True
        payload["detail"] = "service_not_found"
        return payload
    if service_status_before.strip().lower() == "running":
        payload["detail"] = "service_already_running"
        payload["service_status_after"] = service_status_before or "Running"
        return payload
    payload["attempted"] = True
    restart = _powershell_json(
        f"try {{ Start-Service -Name '{service_name}' -ErrorAction Stop; Start-Sleep -Seconds 3; "
        f"$s=Get-Service -Name '{service_name}' -ErrorAction Stop | Select-Object Name,Status; "
        "$r=[pscustomobject]@{ok=$true;name=$s.Name;status=$s.Status;detail='start_service_ok'} } "
        "catch { $r=[pscustomobject]@{ok=$false;name=$null;status=$null;detail=$_.Exception.Message} }; "
        "$r | ConvertTo-Json -Compress"
    )
    if isinstance(restart, dict):
        payload["service_status_after"] = restart.get("status")
        payload["detail"] = str(restart.get("detail") or payload["detail"])
        payload["succeeded"] = bool(restart.get("ok")) and str(restart.get("status") or "").strip().lower() == "running"
    else:
        payload["detail"] = "restart_command_failed"
    payload["failed"] = not payload["succeeded"]
    return payload


def _db_health_check(root: Path, timeout_seconds: float = 2.0) -> dict:
    host, port = _db_connection_target(root)
    ok, reason = _db_port_reachable(host, port, timeout_seconds=timeout_seconds)
    payload = {
        "ok": ok,
        "reason": reason,
        "host": host,
        "port": port,
        "restart_attempted": False,
        "restart_succeeded": False,
        "restart_failed": False,
        "service_name": None,
        "service_status_before": None,
        "service_status_after": None,
        "restart_detail": None,
    }
    if ok or os.name != "nt" or not _db_local_host(host):
        return payload
    restart = _attempt_windows_postgres_restart(root)
    payload.update(
        {
            "restart_attempted": bool(restart.get("attempted")),
            "restart_succeeded": bool(restart.get("succeeded")),
            "restart_failed": bool(restart.get("failed")),
            "service_name": restart.get("service_name"),
            "service_status_before": restart.get("service_status_before"),
            "service_status_after": restart.get("service_status_after"),
            "restart_detail": restart.get("detail"),
        }
    )
    if payload["restart_succeeded"]:
        ok_after, reason_after = _db_port_reachable(host, port, timeout_seconds=timeout_seconds)
        payload["ok"] = ok_after
        payload["reason"] = "db_reachable_after_restart" if ok_after else reason_after
        if not ok_after:
            payload["restart_failed"] = True
    else:
        payload["reason"] = f"{reason};restart_failed:{payload['restart_detail'] or 'unknown'}"
    return payload


def _file_age_minutes(path: Path) -> float | None:
    if not path.exists():
        return None
    try:
        mtime = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc)
    except Exception:
        return None
    return max(0.0, (_now_utc() - mtime).total_seconds() / 60.0)


def _heartbeat_ts(payload: dict):
    return (
        _parse_iso(payload.get("accumulation_written_at_utc"))
        or _parse_iso(payload.get("written_at_utc"))
        or _parse_iso(payload.get("accumulation_last_query_at_utc"))
        or _parse_iso(payload.get("last_query_at_utc"))
        or _parse_iso(payload.get("accumulation_started_at_utc"))
        or _parse_iso(payload.get("started_at_utc"))
    )


def _heartbeat_age_minutes(payload: dict) -> float | None:
    ts = _heartbeat_ts(payload)
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return max(0.0, (_now_utc() - ts.astimezone(dt.timezone.utc)).total_seconds() / 60.0)


def _heartbeat_state(payload: dict) -> str:
    return str((payload or {}).get("state") or "").strip().lower()


def _heartbeat_running_recent(payload: dict, stale_minutes: float) -> bool:
    age = _heartbeat_age_minutes(payload)
    return _heartbeat_state(payload) == "running" and age is not None and age < float(stale_minutes)


def _heartbeat_completed_recent(payload: dict, min_completed_gap_minutes: float) -> bool:
    age = _heartbeat_age_minutes(payload)
    return _heartbeat_state(payload) == "completed" and age is not None and age < float(min_completed_gap_minutes)


def _heartbeat_running_stale(payload: dict, stale_minutes: float) -> bool:
    if _heartbeat_state(payload) != "running":
        return False
    age = _heartbeat_age_minutes(payload)
    if age is None:
        return True
    return age >= float(stale_minutes)


def _reconcile_stale_running_heartbeat(status_file: Path, heartbeat: dict, stale_minutes: float, reason: str) -> dict:
    if not _heartbeat_running_stale(heartbeat, stale_minutes):
        return heartbeat or {}
    payload = dict(heartbeat or {})
    now_iso = _now_utc().isoformat()
    payload["state"] = "interrupted"
    payload["reason"] = reason
    payload["phase"] = payload.get("phase") or "stale"
    payload["completed_at_utc"] = payload.get("completed_at_utc") or now_iso
    payload["written_at_utc"] = now_iso
    payload["accumulation_written_at_utc"] = now_iso
    payload["stale_reconciled_at_utc"] = now_iso
    _write_json(status_file, payload)
    return payload


def _resume_cycle_id_from_heartbeat(heartbeat: dict) -> str | None:
    if not isinstance(heartbeat, dict):
        return None
    raw = str(heartbeat.get("cycle_id") or heartbeat.get("accumulation_run_id") or heartbeat.get("scrape_id") or "").strip()
    return raw or None


def _command_has_flag(command: list[str], flag: str) -> bool:
    return any(str(part or "").strip() == flag for part in command or [])


def _with_resume_cycle_id(command: list[str], heartbeat: dict) -> list[str]:
    cycle_id = _resume_cycle_id_from_heartbeat(heartbeat)
    if not cycle_id or _command_has_flag(command, "--cycle-id"):
        return list(command or [])
    updated = list(command or [])
    if any("run_pipeline.py" in str(part or "") for part in updated) or any("run_all.py" in str(part or "") for part in updated):
        updated.extend(["--cycle-id", cycle_id])
    return updated


def _list_relevant_processes() -> list[dict]:
    if os.name == "nt":
        ps_cmd = r"""
$procs = Get-CimInstance Win32_Process | Where-Object { $_.Name -match 'python|powershell|cmd' } |
  Select-Object ProcessId, ParentProcessId, Name, CommandLine
$procs | ConvertTo-Json -Depth 4 -Compress
"""
        try:
            raw = subprocess.check_output(
                ["powershell.exe", "-NoProfile", "-Command", ps_cmd],
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
            if not raw:
                return []
            data = json.loads(raw)
            if isinstance(data, dict):
                return [data]
            if isinstance(data, list):
                return [x for x in data if isinstance(x, dict)]
            return []
        except Exception:
            return []

    try:
        raw = subprocess.check_output(
            ["ps", "-eo", "pid=,ppid=,comm=,args="],
            text=True,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        return []
    rows: list[dict] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            # Split into 4 fields: pid ppid comm args
            pid_s, ppid_s, name, cmd = line.split(None, 3)
            rows.append(
                {
                    "ProcessId": int(pid_s),
                    "ParentProcessId": int(ppid_s),
                    "Name": name,
                    "CommandLine": cmd,
                }
            )
        except Exception:
            continue
    return rows


def _is_active_pipeline_process(proc: dict) -> bool:
    cmd = str(proc.get("CommandLine") or "").lower()
    if not cmd:
        return False
    if "recover_interrupted_accumulation.py" in cmd:
        return False
    return any(marker in cmd for marker in ACTIVE_PIPELINE_MARKERS)


def _active_pipeline_processes() -> list[dict]:
    return [p for p in _list_relevant_processes() if _is_active_pipeline_process(p)]


def _lock_age_minutes(lock_file: Path, payload: dict | None = None) -> float | None:
    payload = payload or {}
    ts = _parse_iso(payload.get("created_at_utc"))
    if ts is not None:
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=dt.timezone.utc)
        return max(0.0, (_now_utc() - ts.astimezone(dt.timezone.utc)).total_seconds() / 60.0)
    return _file_age_minutes(lock_file)


def _lock_can_be_cleared(lock_file: Path, payload: dict, active_procs: list[dict], heartbeat: dict, args) -> tuple[bool, str]:
    if not lock_file.exists():
        return False, "lock_missing"
    lock_age = _lock_age_minutes(lock_file, payload)
    if active_procs:
        return False, "active_pipeline_process_detected"
    if _heartbeat_running_recent(heartbeat, stale_minutes=args.stale_minutes):
        return False, "fresh_running_heartbeat"
    if lock_age is None or lock_age < float(args.lock_stale_minutes):
        return False, "lock_too_fresh"
    hb_state = _heartbeat_state(heartbeat)
    if hb_state in {"completed", "failed", "error", "interrupted", "recovered", "skipped"}:
        return True, f"terminal_heartbeat_state:{hb_state}"
    if not heartbeat:
        return True, "no_heartbeat_found"
    return True, "stale_lock_no_active_pipeline"


def _try_remove_stale_lock(lock_file: Path, active_procs: list[dict], heartbeat: dict, args) -> tuple[bool, str]:
    payload = _read_json(lock_file)
    ok, reason = _lock_can_be_cleared(lock_file, payload, active_procs, heartbeat, args)
    if not ok:
        return False, reason
    try:
        lock_file.unlink(missing_ok=True)
    except Exception as exc:
        return False, f"lock_unlink_failed:{exc}"
    return True, f"stale_lock_cleared:{reason}"


def _acquire_lock(lock_file: Path, payload: dict) -> tuple[bool, str]:
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    try:
        fd = os.open(str(lock_file), flags)
    except FileExistsError:
        return False, "lock_exists"
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
    except Exception as exc:
        try:
            lock_file.unlink(missing_ok=True)
        except Exception:
            pass
        return False, f"lock_write_failed:{exc}"
    return True, "lock_acquired"


def _release_lock(lock_file: Path):
    try:
        lock_file.unlink(missing_ok=True)
    except Exception:
        pass


def _launch_ingestion_batch(root: Path, dry_run: bool) -> tuple[bool, str]:
    if os.name == "nt":
        batch = root / "scheduler" / "run_ingestion_4h_once.bat"
        if not batch.exists():
            return False, f"missing batch: {batch}"
        if dry_run:
            return True, f"dry-run would launch: {batch}"
        creationflags = 0
        for name in ("CREATE_NEW_PROCESS_GROUP", "DETACHED_PROCESS"):
            creationflags |= int(getattr(subprocess, name, 0))
        subprocess.Popen(
            ["cmd.exe", "/c", str(batch)],
            cwd=str(root),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=creationflags,
        )
        return True, f"launched: {batch}"

    shell_script = root / "scheduler" / "run_ingestion_4h_once.sh"
    if shell_script.exists():
        if dry_run:
            return True, f"dry-run would launch: {shell_script}"
        subprocess.Popen(
            ["/bin/bash", str(shell_script)],
            cwd=str(root),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        return True, f"launched: {shell_script}"

    # Fallback on non-Windows if shell wrapper is not present.
    cmd = [
        sys.executable,
        str(root / "run_pipeline.py"),
        "--python-exe",
        sys.executable,
        "--skip-reports",
        "--report-output-dir",
        str((root / "output" / "reports").resolve()),
        "--report-timestamp-tz",
        "local",
    ]
    if dry_run:
        return True, f"dry-run would launch fallback: {cmd}"
    subprocess.Popen(
        cmd,
        cwd=str(root),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    return True, f"launched fallback run_pipeline: {cmd}"


def _minutes_since_last_attempt(state: dict) -> float | None:
    ts = _parse_iso(state.get("last_launch_attempt_at_utc"))
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return max(0.0, (_now_utc() - ts.astimezone(dt.timezone.utc)).total_seconds() / 60.0)


def _derive_wrapper_event(action: str, reason: str, launched: bool) -> str:
    action = str(action or "").strip().lower()
    reason = str(reason or "").strip().lower()
    if action == "launch" and launched:
        if reason.startswith("stale_running_heartbeat"):
            return "recovered_stale_heartbeat"
        if reason.startswith("idle_gap_exceeded"):
            return "recovered_idle_gap"
        if reason.startswith("no_heartbeat_found"):
            return "launched_new_cycle_no_heartbeat"
        return "launched_new_cycle"
    if action == "skip":
        if "postgres_unreachable" in reason:
            return "skipped_db_unavailable"
        if "completed_buffer_active" in reason:
            return "skipped_buffer"
        if (
            "active_pipeline_process_detected" in reason
            or "fresh_running_heartbeat" in reason
            or "wrapper_lock_present" in reason
            or "lock_acquire_failed" in reason
        ):
            return "skipped_active_run"
        return "skipped_other"
    if action == "none":
        if "postgres_unreachable" in reason:
            return "recovery_wait_db_unavailable"
        if "completed_buffer_active" in reason:
            return "recovery_wait_buffer"
        if "fresh_running_heartbeat" in reason or "active_pipeline_process_detected" in reason:
            return "recovery_wait_active_run"
        if "cooldown_active" in reason:
            return "recovery_wait_cooldown"
        return "recovery_noop"
    if action == "ok":
        return "preflight_ready"
    if action == "completed":
        return "wrapper_finished_success"
    if action == "failed":
        return "wrapper_finished_failure"
    if action == "error":
        return "wrapper_error"
    return "wrapper_unknown"


def _build_status(
    *,
    mode: str,
    root: Path,
    reports_dir: Path,
    status_file: Path,
    state_file: Path,
    active_procs: list[dict],
    heartbeat: dict,
    action: str,
    reason: str,
    launched: bool = False,
    lock_file: Path | None = None,
    db_check: dict | None = None,
) -> dict:
    hb_age = _heartbeat_age_minutes(heartbeat) if heartbeat else None
    lock_payload = _read_json(lock_file) if lock_file and lock_file.exists() else {}
    lock_age = _lock_age_minutes(lock_file, lock_payload) if lock_file else None
    wrapper_event = _derive_wrapper_event(action, reason, bool(launched))
    return {
        "mode": mode,
        "root": str(root),
        "reports_dir": str(reports_dir),
        "status_file": str(status_file),
        "state_file": str(state_file),
        "active_pipeline_process_count": len(active_procs),
        "active_pipeline_processes": [
            {
                "pid": p.get("ProcessId"),
                "name": p.get("Name"),
                "cmd": str(p.get("CommandLine") or "")[:500],
            }
            for p in active_procs
        ],
        "heartbeat_state": heartbeat.get("state") if heartbeat else None,
        "heartbeat_accumulation_run_id": (heartbeat or {}).get("accumulation_run_id") or (heartbeat or {}).get("scrape_id"),
        "heartbeat_age_minutes": hb_age,
        "lock_file": str(lock_file) if lock_file else None,
        "lock_present": bool(lock_file and lock_file.exists()),
        "lock_age_minutes": lock_age,
        "lock_created_at_utc": lock_payload.get("created_at_utc") if lock_payload else None,
        "db_check": db_check or {},
        "action": action,
        "wrapper_event": wrapper_event,
        "reason": reason,
        "launched": bool(launched),
        "checked_at_utc": _now_utc().isoformat(),
    }


def _build_cycle_state_payload(
    *,
    lifecycle_state: str,
    base_payload: dict,
    status_file: Path,
    parallel_file: Path,
    heartbeat: dict | None,
    parallel_status: dict | None,
    command: list[str] | None = None,
    return_code: int | None = None,
) -> dict:
    heartbeat = heartbeat or {}
    parallel_status = parallel_status or {}
    cycle_id = str(parallel_status.get("cycle_id") or heartbeat.get("cycle_id") or heartbeat.get("scrape_id") or "").strip() or None
    aggregate_completed = bool(
        cycle_id
        and parallel_status.get("cycle_id") == cycle_id
        and parallel_status.get("completed_at_utc")
    )
    completed_at_utc = parallel_status.get("completed_at_utc") or heartbeat.get("completed_at_utc") or base_payload.get("checked_at_utc")
    started_at_utc = parallel_status.get("started_at_utc") or heartbeat.get("started_at_utc") or heartbeat.get("accumulation_started_at_utc")
    lifecycle_action = base_payload.get("action")
    wrapper_event = base_payload.get("wrapper_event")
    reason = base_payload.get("reason")

    # If aggregate parallel output confirms this cycle completed, preserve that
    # terminal state instead of letting later transient wrapper events relabel it.
    if aggregate_completed:
        lifecycle_state = "completed"
        lifecycle_action = "completed"
        wrapper_event = "wrapper_finished_success"
        reason = "parallel_scrape_done"
        # A completed aggregate cycle is canonical; do not carry forward a
        # transient preflight/recovery mode or action label for the same cycle.
        base_payload = dict(base_payload)
        base_payload["mode"] = "aggregate"
        base_payload["action"] = lifecycle_action

    return {
        "state": lifecycle_state,
        "status_source": "wrapper_cycle_state",
        "mode": base_payload.get("mode"),
        "action": lifecycle_action,
        "wrapper_event": wrapper_event,
        "reason": reason,
        "checked_at_utc": base_payload.get("checked_at_utc"),
        "cycle_id": cycle_id,
        "accumulation_run_id": cycle_id,
        "started_at_utc": started_at_utc,
        "completed_at_utc": completed_at_utc,
        "phase": heartbeat.get("phase"),
        "selected_dates": heartbeat.get("selected_dates"),
        "overall_query_total": heartbeat.get("overall_query_total"),
        "overall_query_completed": heartbeat.get("overall_query_completed"),
        "total_rows_accumulated": heartbeat.get("total_rows_accumulated"),
        "aggregate_airline_count": parallel_status.get("airline_count"),
        "aggregate_failed_count": parallel_status.get("failed_count"),
        "duration_sec": parallel_status.get("duration_sec"),
        "worker_status_path": str(status_file),
        "parallel_status_path": str(parallel_file),
        "db_check": base_payload.get("db_check") or {},
        "command": command or [],
        "return_code": return_code,
    }


def _event_timestamp(payload: dict) -> dt.datetime | None:
    return _parse_iso(payload.get("checked_at_utc")) or _parse_iso(payload.get("completed_at_utc"))


def _should_preserve_newer_active_cycle(existing_cycle_state: dict, payload: dict) -> bool:
    existing_cycle = str(existing_cycle_state.get("cycle_id") or "").strip()
    payload_cycle = str(payload.get("cycle_id") or "").strip()
    if existing_cycle and payload_cycle and existing_cycle == payload_cycle:
        return False
    existing_state = str(existing_cycle_state.get("state") or "").strip().lower()
    payload_state = str(payload.get("state") or "").strip().lower()
    if existing_state not in {"starting", "running"}:
        return False
    if payload_state not in {"starting", "running", "completed", "skipped"}:
        return False
    existing_started = (
        _parse_iso(existing_cycle_state.get("started_at_utc"))
        or _parse_iso(existing_cycle_state.get("checked_at_utc"))
        or _parse_iso(existing_cycle_state.get("completed_at_utc"))
    )
    payload_started = (
        _parse_iso(payload.get("started_at_utc"))
        or _parse_iso(payload.get("checked_at_utc"))
        or _parse_iso(payload.get("completed_at_utc"))
    )
    if existing_started is None or payload_started is None:
        return False
    return existing_started > payload_started


def _parallel_completion_timestamp(parallel_status: dict | None) -> dt.datetime | None:
    parallel_status = parallel_status or {}
    return _parse_iso(parallel_status.get("completed_at_utc")) or _parse_iso(parallel_status.get("generated_at"))


def _latest_worker_statuses_for_cycle(
    reports_dir: Path,
    cycle_id: str,
    *,
    prefer_terminal_when_idle: bool = False,
) -> dict[str, dict]:
    cycle_id = str(cycle_id or "").strip()
    if not cycle_id:
        return {}
    latest_by_airline: dict[str, tuple[dt.datetime, dict]] = {}
    latest_terminal_by_airline: dict[str, tuple[dt.datetime, dict]] = {}
    pattern = f"run_all_status_*_{cycle_id}_*.json"
    for path in reports_dir.glob(pattern):
        payload = _read_json(path)
        if not isinstance(payload, dict):
            continue
        airline = str(payload.get("current_airline") or payload.get("airline") or "").strip().upper()
        if not airline:
            continue
        ts = (
            _parse_iso(payload.get("written_at_utc"))
            or _parse_iso(payload.get("completed_at_utc"))
            or _parse_iso(payload.get("started_at_utc"))
            or dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc)
        )
        existing = latest_by_airline.get(airline)
        if existing is None or ts > existing[0]:
            latest_by_airline[airline] = (ts, payload)
        if _terminal_worker_state(payload.get("state")):
            existing_terminal = latest_terminal_by_airline.get(airline)
            if existing_terminal is None or ts > existing_terminal[0]:
                latest_terminal_by_airline[airline] = (ts, payload)

    effective: dict[str, dict] = {}
    for airline, (_, payload) in latest_by_airline.items():
        if prefer_terminal_when_idle and not _terminal_worker_state(payload.get("state")):
            terminal = latest_terminal_by_airline.get(airline)
            if terminal is not None:
                effective[airline] = terminal[1]
                continue
            reconciled = _reconcile_idle_stale_worker(payload)
            if reconciled is not None:
                effective[airline] = reconciled
                continue
        effective[airline] = payload
    return effective


def _enabled_airline_codes(root: Path) -> list[str]:
    airlines_file = root / "config" / "airlines.json"
    if not airlines_file.exists():
        return []
    data = _read_json(airlines_file)
    if not isinstance(data, list):
        return []
    codes: list[str] = []
    for row in data:
        if not isinstance(row, dict) or not row.get("enabled"):
            continue
        code = str(row.get("code") or "").strip().upper()
        if code:
            codes.append(code)
    return codes


def _terminal_worker_state(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"completed", "failed", "error", "interrupted", "skipped"}


def _worker_status_timestamp(payload: dict) -> dt.datetime | None:
    return (
        _parse_iso(payload.get("written_at_utc"))
        or _parse_iso(payload.get("completed_at_utc"))
        or _parse_iso(payload.get("accumulation_written_at_utc"))
        or _parse_iso(payload.get("started_at_utc"))
    )


def _reconcile_idle_stale_worker(payload: dict) -> dict | None:
    if _terminal_worker_state(payload.get("state")):
        return payload
    ts = _worker_status_timestamp(payload)
    if ts is None:
        return None
    reconciled = dict(payload)
    completed_at = (
        payload.get("completed_at_utc")
        or payload.get("written_at_utc")
        or payload.get("accumulation_written_at_utc")
        or _now_utc().isoformat()
    )
    reconciled["state"] = "interrupted"
    reconciled["phase"] = "stale"
    reconciled["completed_at_utc"] = completed_at
    reconciled["written_at_utc"] = payload.get("written_at_utc") or completed_at
    reconciled["accumulation_written_at_utc"] = payload.get("accumulation_written_at_utc") or reconciled["written_at_utc"]
    reconciled["reason"] = "idle_stale_worker_reconciled"
    return reconciled


def _rebuild_parallel_status_from_workers(
    root: Path,
    reports_dir: Path,
    cycle_id: str,
    *,
    prefer_terminal_when_idle: bool = False,
) -> dict | None:
    cycle_id = str(cycle_id or "").strip()
    if not cycle_id:
        return None
    worker_payloads = _latest_worker_statuses_for_cycle(
        reports_dir,
        cycle_id,
        prefer_terminal_when_idle=prefer_terminal_when_idle,
    )
    if not worker_payloads:
        return None
    enabled_codes = _enabled_airline_codes(root)
    expected_codes = set(enabled_codes) if enabled_codes else set(worker_payloads)
    available_codes = set(worker_payloads)
    if expected_codes and not expected_codes.issubset(available_codes):
        return None
    if any(not _terminal_worker_state(payload.get("state")) for payload in worker_payloads.values()):
        return None

    started_candidates = [_parse_iso(payload.get("started_at_utc")) for payload in worker_payloads.values()]
    completed_candidates = [
        _parse_iso(payload.get("completed_at_utc"))
        or _parse_iso(payload.get("written_at_utc"))
        or _parse_iso(payload.get("accumulation_written_at_utc"))
        for payload in worker_payloads.values()
    ]
    started_candidates = [ts for ts in started_candidates if ts is not None]
    completed_candidates = [ts for ts in completed_candidates if ts is not None]
    started_at = min(started_candidates) if started_candidates else None
    completed_at = max(completed_candidates) if completed_candidates else None
    if completed_at is None:
        completed_at = dt.datetime.now(dt.timezone.utc)

    results = []
    failed_count = 0
    for airline in sorted(worker_payloads):
        payload = worker_payloads[airline]
        state = str(payload.get("state") or "").strip().lower()
        ok = state == "completed"
        if not ok:
            failed_count += 1
        started_ts = _parse_iso(payload.get("started_at_utc"))
        completed_ts = (
            _parse_iso(payload.get("completed_at_utc"))
            or _parse_iso(payload.get("written_at_utc"))
            or _parse_iso(payload.get("accumulation_written_at_utc"))
        )
        duration_sec = None
        if started_ts is not None and completed_ts is not None:
            duration_sec = max(0.0, (completed_ts - started_ts).total_seconds())
        results.append(
            {
                "airline": airline,
                "cmd": "",
                "rc": 0 if ok else 1,
                "duration_sec": duration_sec,
                "stdout_tail": "",
                "stderr_tail": "",
            }
        )

    duration_sec = None
    if started_at is not None and completed_at is not None:
        duration_sec = max(0.0, (completed_at - started_at).total_seconds())

    return {
        "generated_at": completed_at.isoformat(),
        "started_at_utc": started_at.isoformat() if started_at is not None else None,
        "completed_at_utc": completed_at.isoformat(),
        "duration_sec": duration_sec,
        "cycle_id": cycle_id,
        "airline_count": len(expected_codes) if expected_codes else len(worker_payloads),
        "failed_count": failed_count,
        "results": results,
        "rebuilt_from_worker_status": True,
    }


def _resolve_parallel_status(
    root: Path,
    reports_dir: Path,
    heartbeat: dict | None,
    parallel_file: Path,
    *,
    active_procs: list[dict] | None = None,
) -> dict:
    parallel_status = _read_json(parallel_file)
    heartbeat = heartbeat or {}
    active_procs = active_procs or []
    prefer_terminal_when_idle = not active_procs
    heartbeat_cycle_id = str(heartbeat.get("cycle_id") or heartbeat.get("accumulation_run_id") or heartbeat.get("scrape_id") or "").strip()
    parallel_cycle_id = str((parallel_status or {}).get("cycle_id") or "").strip()
    if heartbeat_cycle_id and heartbeat_cycle_id != parallel_cycle_id:
        rebuilt = _rebuild_parallel_status_from_workers(
            root,
            reports_dir,
            heartbeat_cycle_id,
            prefer_terminal_when_idle=prefer_terminal_when_idle,
        )
        if rebuilt:
            ts = dt.datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
            run_file = reports_dir / f"scrape_parallel_{ts}_{heartbeat_cycle_id}.json"
            existing_parallel = _read_json(parallel_file)
            existing_cycle = str((existing_parallel or {}).get("cycle_id") or "").strip()
            rebuilt_airline_count = int(rebuilt.get("airline_count") or 0)
            existing_airline_count = int((existing_parallel or {}).get("airline_count") or 0)
            if rebuilt_airline_count >= 5 or not existing_parallel or existing_cycle == heartbeat_cycle_id or existing_airline_count < 5:
                _write_json(parallel_file, rebuilt)
            _write_json(run_file, rebuilt)
            return rebuilt
    if not parallel_status and heartbeat_cycle_id:
        rebuilt = _rebuild_parallel_status_from_workers(
            root,
            reports_dir,
            heartbeat_cycle_id,
            prefer_terminal_when_idle=prefer_terminal_when_idle,
        )
        if rebuilt:
            ts = dt.datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
            run_file = reports_dir / f"scrape_parallel_{ts}_{heartbeat_cycle_id}.json"
            existing_parallel = _read_json(parallel_file)
            existing_cycle = str((existing_parallel or {}).get("cycle_id") or "").strip()
            rebuilt_airline_count = int(rebuilt.get("airline_count") or 0)
            existing_airline_count = int((existing_parallel or {}).get("airline_count") or 0)
            if rebuilt_airline_count >= 5 or not existing_parallel or existing_cycle == heartbeat_cycle_id or existing_airline_count < 5:
                _write_json(parallel_file, rebuilt)
            _write_json(run_file, rebuilt)
            return rebuilt
    return parallel_status or {}


def _parallel_completion_is_newer(existing_cycle_state: dict, parallel_status: dict | None) -> bool:
    parallel_ts = _parallel_completion_timestamp(parallel_status)
    if parallel_ts is None:
        return False
    existing_ts = _event_timestamp(existing_cycle_state)
    if existing_ts is None:
        return True
    return parallel_ts > existing_ts


def _parallel_completion_is_newer_than_payload(payload: dict, parallel_status: dict | None) -> bool:
    parallel_ts = _parallel_completion_timestamp(parallel_status)
    if parallel_ts is None:
        return False
    payload_ts = _event_timestamp(payload)
    if payload_ts is None:
        return True
    return parallel_ts > payload_ts


def _parallel_completion_matches_payload_cycle(payload: dict, parallel_status: dict | None) -> bool:
    if not isinstance(parallel_status, dict):
        return False
    parallel_ts = _parallel_completion_timestamp(parallel_status)
    if parallel_ts is None:
        return False
    parallel_cycle_id = str(parallel_status.get("cycle_id") or "").strip()
    payload_cycle_id = str(payload.get("cycle_id") or payload.get("accumulation_run_id") or "").strip()
    if not parallel_cycle_id or not payload_cycle_id:
        return False
    return parallel_cycle_id == payload_cycle_id


def _reconcile_heartbeat_from_parallel_completion(
    status_file: Path,
    heartbeat: dict | None,
    parallel_status: dict | None,
    active_procs: list[dict] | None = None,
) -> dict:
    heartbeat = heartbeat or {}
    parallel_status = parallel_status or {}
    active_procs = active_procs or []
    if active_procs:
        return heartbeat
    completed_at = parallel_status.get("completed_at_utc") or parallel_status.get("generated_at")
    parallel_cycle_id = str(parallel_status.get("cycle_id") or "").strip()
    if not completed_at or not parallel_cycle_id:
        return heartbeat
    heartbeat_cycle_id = str(
        heartbeat.get("cycle_id") or heartbeat.get("accumulation_run_id") or heartbeat.get("scrape_id") or ""
    ).strip()
    if heartbeat_cycle_id and heartbeat_cycle_id != parallel_cycle_id:
        return heartbeat
    payload = dict(heartbeat)
    payload["state"] = "completed"
    payload["cycle_id"] = parallel_cycle_id
    payload["scrape_id"] = parallel_cycle_id
    payload["accumulation_run_id"] = parallel_cycle_id
    payload["reason"] = "parallel_scrape_done"
    payload["completed_at_utc"] = completed_at
    payload["written_at_utc"] = completed_at
    payload["accumulation_written_at_utc"] = completed_at
    payload["aggregate_failed_count"] = parallel_status.get("failed_count")
    _write_json(status_file, payload)
    return payload


def _preserve_existing_completed_cycle(existing_cycle_state: dict, payload: dict) -> bool:
    existing_state = str(existing_cycle_state.get("state") or "").strip().lower()
    payload_state = str(payload.get("state") or "").strip().lower()
    if existing_state != "completed":
        return False
    if payload_state not in {"skipped", "running", "starting"}:
        return False
    existing_completed_at = _parse_iso(existing_cycle_state.get("completed_at_utc"))
    if existing_completed_at is None:
        return False
    existing_cycle_id = str(existing_cycle_state.get("cycle_id") or "").strip()
    payload_cycle_id = str(payload.get("cycle_id") or "").strip()
    if existing_cycle_id and payload_cycle_id and existing_cycle_id != payload_cycle_id:
        return False
    return True


def _write_cycle_state(
    cycle_state_file: Path,
    *,
    lifecycle_state: str,
    base_payload: dict,
    status_file: Path,
    parallel_file: Path,
    heartbeat: dict | None,
    parallel_status: dict | None,
    command: list[str] | None = None,
    return_code: int | None = None,
):
    payload = _build_cycle_state_payload(
        lifecycle_state=lifecycle_state,
        base_payload=base_payload,
        status_file=status_file,
        parallel_file=parallel_file,
        heartbeat=heartbeat,
        parallel_status=parallel_status,
        command=command,
        return_code=return_code,
    )

    existing_cycle_state = _read_json(cycle_state_file)
    if _parallel_completion_is_newer(existing_cycle_state, parallel_status) or _parallel_completion_is_newer_than_payload(payload, parallel_status):
        parallel_status = parallel_status or {}
        heartbeat = heartbeat or {}
        parallel_cycle_id = str(parallel_status.get("cycle_id") or "").strip()
        heartbeat_cycle_id = str(heartbeat.get("cycle_id") or heartbeat.get("scrape_id") or "").strip()
        aligned_heartbeat = heartbeat if parallel_cycle_id and heartbeat_cycle_id == parallel_cycle_id else {}
        completed_payload = {
            "mode": "aggregate-sync",
            "action": "completed",
            "wrapper_event": "wrapper_finished_success",
            "reason": "parallel_scrape_done",
            "checked_at_utc": parallel_status.get("completed_at_utc")
            or parallel_status.get("generated_at")
            or base_payload.get("checked_at_utc"),
            "db_check": base_payload.get("db_check") or {},
        }
        payload = _build_cycle_state_payload(
            lifecycle_state="completed",
            base_payload=completed_payload,
            status_file=status_file,
            parallel_file=parallel_file,
            heartbeat=aligned_heartbeat,
            parallel_status=parallel_status,
            command=command,
            return_code=return_code,
        )
    elif _parallel_completion_matches_payload_cycle(payload, parallel_status):
        parallel_status = parallel_status or {}
        heartbeat = heartbeat or {}
        parallel_cycle_id = str(parallel_status.get("cycle_id") or "").strip()
        heartbeat_cycle_id = str(heartbeat.get("cycle_id") or heartbeat.get("scrape_id") or "").strip()
        aligned_heartbeat = heartbeat if parallel_cycle_id and heartbeat_cycle_id == parallel_cycle_id else {}
        completed_payload = {
            "mode": "aggregate-sync",
            "action": "completed",
            "wrapper_event": "wrapper_finished_success",
            "reason": "parallel_scrape_done",
            "checked_at_utc": parallel_status.get("completed_at_utc")
            or parallel_status.get("generated_at")
            or base_payload.get("checked_at_utc"),
            "db_check": base_payload.get("db_check") or {},
        }
        payload = _build_cycle_state_payload(
            lifecycle_state="completed",
            base_payload=completed_payload,
            status_file=status_file,
            parallel_file=parallel_file,
            heartbeat=aligned_heartbeat,
            parallel_status=parallel_status,
            command=command,
            return_code=return_code,
        )
    elif _should_preserve_newer_active_cycle(existing_cycle_state, payload):
        payload = existing_cycle_state
    elif _preserve_existing_completed_cycle(existing_cycle_state, payload):
        payload = existing_cycle_state

    _write_json(cycle_state_file, payload)


def _handle_preflight(args, root: Path, reports_dir: Path, status_file: Path, state_file: Path, output_file: Path, cycle_state_file: Path, lock_file: Path) -> int:
    active = _active_pipeline_processes()
    heartbeat = _read_json(status_file)
    parallel_file = reports_dir / "scrape_parallel_latest.json"
    parallel_status = _resolve_parallel_status(root, reports_dir, heartbeat, parallel_file, active_procs=active)
    heartbeat = _reconcile_heartbeat_from_parallel_completion(status_file, heartbeat, parallel_status, active)
    db_check = _db_health_check(root)
    db_ok = bool(db_check.get("ok"))
    if lock_file.exists():
        cleared, reason = _try_remove_stale_lock(lock_file, active, heartbeat, args)
        if not cleared:
            payload = _build_status(
                mode="preflight",
                root=root,
                reports_dir=reports_dir,
                status_file=status_file,
                state_file=state_file,
                active_procs=active,
                heartbeat=heartbeat,
                action="skip",
                reason=f"wrapper_lock_present:{reason}",
                lock_file=lock_file,
                db_check=db_check,
            )
            _write_json(output_file, payload)
            _write_cycle_state(
                cycle_state_file,
                lifecycle_state="skipped",
                base_payload=payload,
                status_file=status_file,
                parallel_file=parallel_file,
                heartbeat=heartbeat,
                parallel_status=parallel_status,
            )
            print(json.dumps(payload, ensure_ascii=False))
            return 10
    if active:
        payload = _build_status(
            mode="preflight",
            root=root,
            reports_dir=reports_dir,
            status_file=status_file,
            state_file=state_file,
            active_procs=active,
            heartbeat=heartbeat,
            action="skip",
            reason="active_pipeline_process_detected",
            lock_file=lock_file,
            db_check=db_check,
        )
        _write_json(output_file, payload)
        _write_cycle_state(
            cycle_state_file,
            lifecycle_state="skipped",
            base_payload=payload,
            status_file=status_file,
            parallel_file=parallel_file,
            heartbeat=heartbeat,
            parallel_status=parallel_status,
        )
        print(json.dumps(payload, ensure_ascii=False))
        return 10
    heartbeat = _reconcile_stale_running_heartbeat(
        status_file,
        heartbeat,
        args.stale_minutes,
        "stale_heartbeat_no_active_pipeline",
    )
    if _heartbeat_running_recent(heartbeat, stale_minutes=args.stale_minutes):
        payload = _build_status(
            mode="preflight",
            root=root,
            reports_dir=reports_dir,
            status_file=status_file,
            state_file=state_file,
            active_procs=[],
            heartbeat=heartbeat,
            action="skip",
            reason="fresh_running_heartbeat",
            lock_file=lock_file,
            db_check=db_check,
        )
        _write_json(output_file, payload)
        _write_cycle_state(
            cycle_state_file,
            lifecycle_state="skipped",
            base_payload=payload,
            status_file=status_file,
            parallel_file=parallel_file,
            heartbeat=heartbeat,
            parallel_status=parallel_status,
        )
        print(json.dumps(payload, ensure_ascii=False))
        return 10
    if _heartbeat_completed_recent(heartbeat, min_completed_gap_minutes=args.min_completed_gap_minutes):
        payload = _build_status(
            mode="preflight",
            root=root,
            reports_dir=reports_dir,
            status_file=status_file,
            state_file=state_file,
            active_procs=[],
            heartbeat=heartbeat,
            action="skip",
            reason="completed_buffer_active",
            lock_file=lock_file,
            db_check=db_check,
        )
        _write_json(output_file, payload)
        _write_cycle_state(
            cycle_state_file,
            lifecycle_state="skipped",
            base_payload=payload,
            status_file=status_file,
            parallel_file=parallel_file,
            heartbeat=heartbeat,
            parallel_status=parallel_status,
        )
        print(json.dumps(payload, ensure_ascii=False))
        return 11
    if not db_ok:
        payload = _build_status(
            mode="preflight",
            root=root,
            reports_dir=reports_dir,
            status_file=status_file,
            state_file=state_file,
            active_procs=[],
            heartbeat=heartbeat,
            action="skip",
            reason="postgres_unreachable",
            lock_file=lock_file,
            db_check=db_check,
        )
        _write_json(output_file, payload)
        _write_cycle_state(
            cycle_state_file,
            lifecycle_state="skipped",
            base_payload=payload,
            status_file=status_file,
            parallel_file=parallel_file,
            heartbeat=heartbeat,
            parallel_status=parallel_status,
        )
        print(json.dumps(payload, ensure_ascii=False))
        return 12
    payload = _build_status(
        mode="preflight",
        root=root,
        reports_dir=reports_dir,
        status_file=status_file,
        state_file=state_file,
        active_procs=active,
        heartbeat=heartbeat,
        action="ok",
        reason="no_active_pipeline_process",
        lock_file=lock_file,
        db_check=db_check,
    )
    _write_json(output_file, payload)
    _write_cycle_state(
        cycle_state_file,
        lifecycle_state="ready",
        base_payload=payload,
        status_file=status_file,
        parallel_file=parallel_file,
        heartbeat=heartbeat,
        parallel_status=parallel_status,
    )
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def _handle_recover(args, root: Path, reports_dir: Path, status_file: Path, state_file: Path, output_file: Path, cycle_state_file: Path, lock_file: Path) -> int:
    active = _active_pipeline_processes()
    heartbeat = _read_json(status_file)
    state = _read_json(state_file)
    parallel_file = reports_dir / "scrape_parallel_latest.json"
    parallel_status = _resolve_parallel_status(root, reports_dir, heartbeat, parallel_file, active_procs=active)
    heartbeat = _reconcile_heartbeat_from_parallel_completion(status_file, heartbeat, parallel_status, active)
    db_check = _db_health_check(root)
    db_ok = bool(db_check.get("ok"))

    if active:
        payload = _build_status(
            mode="recover",
            root=root,
            reports_dir=reports_dir,
            status_file=status_file,
            state_file=state_file,
            active_procs=active,
            heartbeat=heartbeat,
            action="none",
            reason="active_pipeline_process_detected",
            lock_file=lock_file,
            db_check=db_check,
        )
        _write_json(output_file, payload)
        _write_cycle_state(
            cycle_state_file,
            lifecycle_state="running",
            base_payload=payload,
            status_file=status_file,
            parallel_file=parallel_file,
            heartbeat=heartbeat,
            parallel_status=parallel_status,
        )
        print(json.dumps(payload, ensure_ascii=False))
        return 0
    heartbeat = _reconcile_stale_running_heartbeat(
        status_file,
        heartbeat,
        args.stale_minutes,
        "stale_heartbeat_no_active_pipeline",
    )

    hb_state = _heartbeat_state(heartbeat) if heartbeat else ""
    hb_age = _heartbeat_age_minutes(heartbeat) if heartbeat else None
    if _heartbeat_running_recent(heartbeat, args.stale_minutes):
        payload = _build_status(
            mode="recover",
            root=root,
            reports_dir=reports_dir,
            status_file=status_file,
            state_file=state_file,
            active_procs=[],
            heartbeat=heartbeat,
            action="none",
            reason="fresh_running_heartbeat",
            lock_file=lock_file,
            db_check=db_check,
        )
        _write_json(output_file, payload)
        _write_cycle_state(
            cycle_state_file,
            lifecycle_state="running",
            base_payload=payload,
            status_file=status_file,
            parallel_file=parallel_file,
            heartbeat=heartbeat,
            parallel_status=parallel_status,
        )
        print(json.dumps(payload, ensure_ascii=False))
        return 0
    if _heartbeat_completed_recent(heartbeat, args.min_completed_gap_minutes):
        payload = _build_status(
            mode="recover",
            root=root,
            reports_dir=reports_dir,
            status_file=status_file,
            state_file=state_file,
            active_procs=[],
            heartbeat=heartbeat,
            action="none",
            reason="completed_buffer_active",
            lock_file=lock_file,
            db_check=db_check,
        )
        _write_json(output_file, payload)
        _write_cycle_state(
            cycle_state_file,
            lifecycle_state="skipped",
            base_payload=payload,
            status_file=status_file,
            parallel_file=parallel_file,
            heartbeat=heartbeat,
            parallel_status=parallel_status,
        )
        print(json.dumps(payload, ensure_ascii=False))
        return 0
    stale_running = hb_state == "running" and hb_age is not None and hb_age >= float(args.stale_minutes)
    idle_too_long = hb_age is not None and hb_age >= float(args.max_idle_minutes)
    no_heartbeat = not heartbeat

    should_launch = False
    launch_reason = ""
    if stale_running:
        should_launch = True
        launch_reason = "stale_running_heartbeat"
    elif idle_too_long:
        should_launch = True
        launch_reason = "idle_gap_exceeded"
    elif no_heartbeat:
        # Fresh setup: allow one start if there is no heartbeat at all.
        should_launch = True
        launch_reason = "no_heartbeat_found"

    mins_since_attempt = _minutes_since_last_attempt(state)
    if should_launch and mins_since_attempt is not None and mins_since_attempt < float(args.cooldown_minutes):
        should_launch = False
        launch_reason = "cooldown_active"

    if not should_launch:
        reason = launch_reason or "no_recovery_trigger"
        payload = _build_status(
            mode="recover",
            root=root,
            reports_dir=reports_dir,
            status_file=status_file,
            state_file=state_file,
            active_procs=[],
            heartbeat=heartbeat,
            action="none",
            reason=reason,
            lock_file=lock_file,
            db_check=db_check,
        )
        _write_json(output_file, payload)
        print(json.dumps(payload, ensure_ascii=False))
        return 0
    if not db_ok:
        payload = _build_status(
            mode="recover",
            root=root,
            reports_dir=reports_dir,
            status_file=status_file,
            state_file=state_file,
            active_procs=[],
            heartbeat=heartbeat,
            action="none",
            reason="postgres_unreachable",
            lock_file=lock_file,
            db_check=db_check,
        )
        _write_json(output_file, payload)
        _write_cycle_state(
            cycle_state_file,
            lifecycle_state="skipped",
            base_payload=payload,
            status_file=status_file,
            parallel_file=parallel_file,
            heartbeat=heartbeat,
            parallel_status=parallel_status,
        )
        print(json.dumps(payload, ensure_ascii=False))
        return 0

    ok, msg = _launch_ingestion_batch(root, args.dry_run)
    now_iso = _now_utc().isoformat()
    state["last_launch_attempt_at_utc"] = now_iso
    state["last_launch_attempt_reason"] = launch_reason
    state["last_launch_attempt_ok"] = bool(ok)
    if ok and not args.dry_run:
        state["last_launch_started_at_utc"] = now_iso
    _write_json(state_file, state)

    payload = _build_status(
        mode="recover",
        root=root,
        reports_dir=reports_dir,
        status_file=status_file,
        state_file=state_file,
        active_procs=[],
        heartbeat=heartbeat,
        action="launch" if ok else "error",
        reason=f"{launch_reason}: {msg}",
        launched=ok and not args.dry_run,
        lock_file=lock_file,
        db_check=db_check,
    )
    _write_json(output_file, payload)
    _write_cycle_state(
        cycle_state_file,
        lifecycle_state="starting" if ok else "failed",
        base_payload=payload,
        status_file=status_file,
        parallel_file=parallel_file,
        heartbeat=heartbeat,
        parallel_status=parallel_status,
    )
    print(json.dumps(payload, ensure_ascii=False))
    return 0 if ok else 1


def _handle_guarded_run(args, root: Path, reports_dir: Path, status_file: Path, state_file: Path, output_file: Path, cycle_state_file: Path, lock_file: Path) -> int:
    active = _active_pipeline_processes()
    heartbeat = _read_json(status_file)
    parallel_file = reports_dir / "scrape_parallel_latest.json"
    parallel_status = _resolve_parallel_status(root, reports_dir, heartbeat, parallel_file, active_procs=active)
    heartbeat = _reconcile_heartbeat_from_parallel_completion(status_file, heartbeat, parallel_status, active)
    db_check = _db_health_check(root)
    db_ok = bool(db_check.get("ok"))
    if lock_file.exists():
        cleared, reason = _try_remove_stale_lock(lock_file, active, heartbeat, args)
        if not cleared:
            payload = _build_status(
                mode="guarded-run",
                root=root,
                reports_dir=reports_dir,
                status_file=status_file,
                state_file=state_file,
                active_procs=active,
                heartbeat=heartbeat,
                action="skip",
                reason=f"wrapper_lock_present:{reason}",
                lock_file=lock_file,
                db_check=db_check,
            )
            _write_json(output_file, payload)
            _write_cycle_state(
                cycle_state_file,
                lifecycle_state="skipped",
                base_payload=payload,
                status_file=status_file,
                parallel_file=parallel_file,
                heartbeat=heartbeat,
                parallel_status=parallel_status,
            )
            print(json.dumps(payload, ensure_ascii=False))
            return 10

    if active:
        payload = _build_status(
            mode="guarded-run",
            root=root,
            reports_dir=reports_dir,
            status_file=status_file,
            state_file=state_file,
            active_procs=active,
            heartbeat=heartbeat,
            action="skip",
            reason="active_pipeline_process_detected",
            lock_file=lock_file,
            db_check=db_check,
        )
        _write_json(output_file, payload)
        _write_cycle_state(
            cycle_state_file,
            lifecycle_state="skipped",
            base_payload=payload,
            status_file=status_file,
            parallel_file=parallel_file,
            heartbeat=heartbeat,
            parallel_status=parallel_status,
        )
        print(json.dumps(payload, ensure_ascii=False))
        return 10
    heartbeat = _reconcile_stale_running_heartbeat(
        status_file,
        heartbeat,
        args.stale_minutes,
        "stale_heartbeat_no_active_pipeline",
    )

    if _heartbeat_running_recent(heartbeat, stale_minutes=args.stale_minutes):
        payload = _build_status(
            mode="guarded-run",
            root=root,
            reports_dir=reports_dir,
            status_file=status_file,
            state_file=state_file,
            active_procs=[],
            heartbeat=heartbeat,
            action="skip",
            reason="fresh_running_heartbeat",
            lock_file=lock_file,
            db_check=db_check,
        )
        _write_json(output_file, payload)
        _write_cycle_state(
            cycle_state_file,
            lifecycle_state="skipped",
            base_payload=payload,
            status_file=status_file,
            parallel_file=parallel_file,
            heartbeat=heartbeat,
            parallel_status=parallel_status,
        )
        print(json.dumps(payload, ensure_ascii=False))
        return 10

    if _heartbeat_completed_recent(heartbeat, min_completed_gap_minutes=args.min_completed_gap_minutes):
        payload = _build_status(
            mode="guarded-run",
            root=root,
            reports_dir=reports_dir,
            status_file=status_file,
            state_file=state_file,
            active_procs=[],
            heartbeat=heartbeat,
            action="skip",
            reason="completed_buffer_active",
            lock_file=lock_file,
            db_check=db_check,
        )
        _write_json(output_file, payload)
        _write_cycle_state(
            cycle_state_file,
            lifecycle_state="skipped",
            base_payload=payload,
            status_file=status_file,
            parallel_file=parallel_file,
            heartbeat=heartbeat,
            parallel_status=parallel_status,
        )
        print(json.dumps(payload, ensure_ascii=False))
        return 11
    if not db_ok:
        payload = _build_status(
            mode="guarded-run",
            root=root,
            reports_dir=reports_dir,
            status_file=status_file,
            state_file=state_file,
            active_procs=[],
            heartbeat=heartbeat,
            action="skip",
            reason="postgres_unreachable",
            lock_file=lock_file,
            db_check=db_check,
        )
        _write_json(output_file, payload)
        _write_cycle_state(
            cycle_state_file,
            lifecycle_state="skipped",
            base_payload=payload,
            status_file=status_file,
            parallel_file=parallel_file,
            heartbeat=heartbeat,
            parallel_status=parallel_status,
        )
        print(json.dumps(payload, ensure_ascii=False))
        return 12

    command = list(args.command or [])
    if command and command[0] == "--":
        command = command[1:]
    if _heartbeat_state(heartbeat) in {"running", "interrupted"}:
        command = _with_resume_cycle_id(command, heartbeat)
    if not command:
        payload = _build_status(
            mode="guarded-run",
            root=root,
            reports_dir=reports_dir,
            status_file=status_file,
            state_file=state_file,
            active_procs=[],
            heartbeat=heartbeat,
            action="error",
            reason="missing_command",
            lock_file=lock_file,
            db_check=db_check,
        )
        _write_json(output_file, payload)
        _write_cycle_state(
            cycle_state_file,
            lifecycle_state="failed",
            base_payload=payload,
            status_file=status_file,
            parallel_file=parallel_file,
            heartbeat=heartbeat,
            parallel_status=parallel_status,
        )
        print(json.dumps(payload, ensure_ascii=False))
        return 2

    lock_payload = {
        "created_at_utc": _now_utc().isoformat(),
        "mode": "guarded-run",
        "root": str(root),
        "command": command,
    }
    acquired, reason = _acquire_lock(lock_file, lock_payload)
    if not acquired:
        payload = _build_status(
            mode="guarded-run",
            root=root,
            reports_dir=reports_dir,
            status_file=status_file,
            state_file=state_file,
            active_procs=[],
            heartbeat=heartbeat,
            action="skip",
            reason=f"lock_acquire_failed:{reason}",
            lock_file=lock_file,
            db_check=db_check,
        )
        _write_json(output_file, payload)
        _write_cycle_state(
            cycle_state_file,
            lifecycle_state="skipped",
            base_payload=payload,
            status_file=status_file,
            parallel_file=parallel_file,
            heartbeat=heartbeat,
            parallel_status=parallel_status,
        )
        print(json.dumps(payload, ensure_ascii=False))
        return 10

    start_payload = _build_status(
        mode="guarded-run",
        root=root,
        reports_dir=reports_dir,
        status_file=status_file,
        state_file=state_file,
        active_procs=[],
        heartbeat=heartbeat,
        action="launch",
        reason="lock_acquired_and_command_starting",
        launched=True,
        lock_file=lock_file,
        db_check=db_check,
    )
    _write_json(output_file, start_payload)
    _write_cycle_state(
        cycle_state_file,
        lifecycle_state="starting",
        base_payload=start_payload,
        status_file=status_file,
        parallel_file=parallel_file,
        heartbeat={},
        parallel_status={},
        command=command,
    )
    print(json.dumps(start_payload, ensure_ascii=False))

    if args.dry_run:
        _release_lock(lock_file)
        return 0

    try:
        completed = subprocess.run(command, cwd=str(root))
        final_heartbeat = _read_json(status_file)
        final_parallel = _read_json(parallel_file)
        final_payload = _build_status(
            mode="guarded-run",
            root=root,
            reports_dir=reports_dir,
            status_file=status_file,
            state_file=state_file,
            active_procs=[],
            heartbeat=final_heartbeat,
            action="completed" if int(completed.returncode) == 0 else "failed",
            reason=f"command_exit_rc:{int(completed.returncode)}",
            launched=True,
            lock_file=lock_file,
            db_check=db_check,
        )
        _write_json(output_file, final_payload)
        _write_cycle_state(
            cycle_state_file,
            lifecycle_state="completed" if int(completed.returncode) == 0 else "failed",
            base_payload=final_payload,
            status_file=status_file,
            parallel_file=parallel_file,
            heartbeat=final_heartbeat,
            parallel_status=final_parallel,
            command=command,
            return_code=int(completed.returncode),
        )
        print(json.dumps(final_payload, ensure_ascii=False))
        return int(completed.returncode)
    finally:
        _release_lock(lock_file)


def main():
    args = parse_args()
    root = _repo_root(args)
    reports_dir = _reports_dir(root, args.reports_dir)
    status_file = _status_path(args, reports_dir)
    state_file = _state_path(args, reports_dir)
    output_file = _output_path(args, reports_dir)
    cycle_state_file = _cycle_state_path(reports_dir)
    lock_file = _lock_path(args, reports_dir)

    if args.mode == "preflight":
        return _handle_preflight(args, root, reports_dir, status_file, state_file, output_file, cycle_state_file, lock_file)
    if args.mode == "recover":
        return _handle_recover(args, root, reports_dir, status_file, state_file, output_file, cycle_state_file, lock_file)
    return _handle_guarded_run(args, root, reports_dir, status_file, state_file, output_file, cycle_state_file, lock_file)


if __name__ == "__main__":
    raise SystemExit(main())
