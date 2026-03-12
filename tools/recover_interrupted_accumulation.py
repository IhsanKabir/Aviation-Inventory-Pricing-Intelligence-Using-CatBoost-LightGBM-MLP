from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import socket
import subprocess
import sys
from pathlib import Path


ACTIVE_PIPELINE_MARKERS = ("run_pipeline.py", "run_all.py", "generate_reports.py")


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


def _db_reachable(root: Path, timeout_seconds: float = 2.0) -> tuple[bool, str, str, int]:
    host, port = _db_connection_target(root)
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return True, "db_reachable", host, port
    except OSError as exc:
        return False, f"db_unreachable:{exc}", host, port


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
        "reason": reason,
        "launched": bool(launched),
        "checked_at_utc": _now_utc().isoformat(),
    }


def _handle_preflight(args, root: Path, reports_dir: Path, status_file: Path, state_file: Path, output_file: Path, lock_file: Path) -> int:
    active = _active_pipeline_processes()
    heartbeat = _read_json(status_file)
    db_ok, db_reason, db_host, db_port = _db_reachable(root)
    db_check = {"ok": db_ok, "reason": db_reason, "host": db_host, "port": db_port}
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
        print(json.dumps(payload, ensure_ascii=False))
        return 10
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
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def _handle_recover(args, root: Path, reports_dir: Path, status_file: Path, state_file: Path, output_file: Path, lock_file: Path) -> int:
    active = _active_pipeline_processes()
    heartbeat = _read_json(status_file)
    state = _read_json(state_file)
    db_ok, db_reason, db_host, db_port = _db_reachable(root)
    db_check = {"ok": db_ok, "reason": db_reason, "host": db_host, "port": db_port}

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
        print(json.dumps(payload, ensure_ascii=False))
        return 0

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
    print(json.dumps(payload, ensure_ascii=False))
    return 0 if ok else 1


def _handle_guarded_run(args, root: Path, reports_dir: Path, status_file: Path, state_file: Path, output_file: Path, lock_file: Path) -> int:
    active = _active_pipeline_processes()
    heartbeat = _read_json(status_file)
    db_ok, db_reason, db_host, db_port = _db_reachable(root)
    db_check = {"ok": db_ok, "reason": db_reason, "host": db_host, "port": db_port}
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
        print(json.dumps(payload, ensure_ascii=False))
        return 10

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
        print(json.dumps(payload, ensure_ascii=False))
        return 12

    command = list(args.command or [])
    if command and command[0] == "--":
        command = command[1:]
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
    print(json.dumps(start_payload, ensure_ascii=False))

    if args.dry_run:
        _release_lock(lock_file)
        return 0

    try:
        completed = subprocess.run(command, cwd=str(root))
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
    lock_file = _lock_path(args, reports_dir)

    if args.mode == "preflight":
        return _handle_preflight(args, root, reports_dir, status_file, state_file, output_file, lock_file)
    if args.mode == "recover":
        return _handle_recover(args, root, reports_dir, status_file, state_file, output_file, lock_file)
    return _handle_guarded_run(args, root, reports_dir, status_file, state_file, output_file, lock_file)


if __name__ == "__main__":
    raise SystemExit(main())
