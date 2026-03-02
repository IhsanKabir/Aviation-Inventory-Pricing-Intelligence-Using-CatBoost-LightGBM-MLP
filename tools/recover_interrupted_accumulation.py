from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path


ACTIVE_PIPELINE_MARKERS = ("run_pipeline.py", "run_all.py", "generate_reports.py")


def parse_args():
    p = argparse.ArgumentParser(
        description="Detect stale/interrupted accumulation runs and optionally relaunch safely."
    )
    p.add_argument("--mode", choices=["preflight", "recover"], required=True)
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
    p.add_argument("--dry-run", action="store_true")
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


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


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
) -> dict:
    hb_age = _heartbeat_age_minutes(heartbeat) if heartbeat else None
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
        "action": action,
        "reason": reason,
        "launched": bool(launched),
        "checked_at_utc": _now_utc().isoformat(),
    }


def _handle_preflight(root: Path, reports_dir: Path, status_file: Path, state_file: Path, output_file: Path) -> int:
    active = _active_pipeline_processes()
    heartbeat = _read_json(status_file)
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
        )
        _write_json(output_file, payload)
        print(json.dumps(payload, ensure_ascii=False))
        return 10
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
    )
    _write_json(output_file, payload)
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def _handle_recover(args, root: Path, reports_dir: Path, status_file: Path, state_file: Path, output_file: Path) -> int:
    active = _active_pipeline_processes()
    heartbeat = _read_json(status_file)
    state = _read_json(state_file)

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
        )
        _write_json(output_file, payload)
        print(json.dumps(payload, ensure_ascii=False))
        return 0

    hb_state = str(heartbeat.get("state") or "").lower() if heartbeat else ""
    hb_age = _heartbeat_age_minutes(heartbeat) if heartbeat else None
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
    )
    _write_json(output_file, payload)
    print(json.dumps(payload, ensure_ascii=False))
    return 0 if ok else 1


def main():
    args = parse_args()
    root = _repo_root(args)
    reports_dir = _reports_dir(root, args.reports_dir)
    status_file = _status_path(args, reports_dir)
    state_file = _state_path(args, reports_dir)
    output_file = _output_path(args, reports_dir)

    if args.mode == "preflight":
        return _handle_preflight(root, reports_dir, status_file, state_file, output_file)
    return _handle_recover(args, root, reports_dir, status_file, state_file, output_file)


if __name__ == "__main__":
    raise SystemExit(main())
