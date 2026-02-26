from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
RUNNER_TOOL = REPO_ROOT / "tools" / "maldivian_plnext_capture_runner.py"
DEFAULT_CDP_URL = "http://127.0.0.1:9222"


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_UTC")


def _json_load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _int_opt(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    s = str(value).strip()
    if s == "":
        return None
    return int(s)


def _iter_csv_rows(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        filtered = [line for line in f if line.strip() and not line.lstrip().startswith("#")]
    if not filtered:
        return []
    reader = csv.DictReader(filtered)
    out = []
    for row in reader:
        if not isinstance(row, dict):
            continue
        out.append({str(k or "").strip(): v for k, v in row.items()})
    return out


def _load_queue_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise SystemExit(f"--queue-file not found: {path}")

    if path.suffix.lower() == ".json":
        raw = _json_load(path)
        if isinstance(raw, dict):
            jobs = raw.get("jobs")
            if not isinstance(jobs, list):
                raise SystemExit("JSON queue file must be a list or an object with a 'jobs' list")
            raw_rows = jobs
        elif isinstance(raw, list):
            raw_rows = raw
        else:
            raise SystemExit("JSON queue file must be a list or an object with a 'jobs' list")
        rows = []
        for item in raw_rows:
            if not isinstance(item, dict):
                continue
            rows.append({str(k): v for k, v in item.items()})
        return rows

    return list(_iter_csv_rows(path))


def _normalize_job(raw: dict[str, Any], *, defaults: argparse.Namespace) -> dict[str, Any]:
    def pick(*keys: str) -> Any:
        for k in keys:
            if k in raw and raw[k] not in (None, ""):
                return raw[k]
        return None

    carrier_raw = str(pick("carrier", "airline") or "Q2").strip().upper()
    if carrier_raw and carrier_raw != "Q2":
        raise SystemExit(f"Queue row carrier must be Q2 for this runner: {raw!r}")

    origin = str(pick("origin", "from") or "").strip().upper()
    destination = str(pick("destination", "dest", "to") or "").strip().upper()
    date = str(pick("date", "departure_date") or "").strip()
    cabin = str(pick("cabin") or defaults.cabin or "Economy").strip() or "Economy"

    adt = _int_opt(pick("adt", "adults"))
    chd = _int_opt(pick("chd", "children"))
    inf = _int_opt(pick("inf", "infants"))
    if adt is None:
        adt = int(defaults.adt)
    if chd is None:
        chd = int(defaults.chd)
    if inf is None:
        inf = int(defaults.inf)

    if not (origin and destination and date):
        raise SystemExit(f"Queue row missing origin/destination/date: {raw!r}")

    return {
        "carrier": "Q2",
        "origin": origin,
        "destination": destination,
        "date": date,
        "cabin": cabin,
        "adt": int(adt),
        "chd": int(chd),
        "inf": int(inf),
        "note": pick("note", "notes"),
    }


def _dedupe_jobs(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = set()
    out = []
    for job in jobs:
        k = (
            job["origin"],
            job["destination"],
            job["date"],
            job["cabin"],
            int(job["adt"]),
            int(job["chd"]),
            int(job["inf"]),
        )
        if k in seen:
            continue
        seen.add(k)
        out.append(job)
    return out


def _add_arg(cmd: list[str], flag: str, value: Any | None) -> None:
    if value is None:
        return
    cmd.extend([flag, str(value)])


def _find_run_dir_for_job(session_root: Path, job: dict[str, Any], started_ts: float) -> Path | None:
    runs_dir = session_root / "runs"
    if not runs_dir.exists():
        return None
    prefix = f"q2_{job['origin']}_{job['destination']}_{job['date']}_".lower()
    candidates = []
    for d in runs_dir.iterdir():
        if not d.is_dir():
            continue
        if not d.name.lower().startswith(prefix):
            continue
        if d.stat().st_mtime + 2 < started_ts:
            continue
        candidates.append(d)
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def _load_run_artifacts(run_dir: Path | None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if not run_dir:
        return None, None
    probe = run_dir / "q2_probe_response.json"
    ingest = run_dir / "q2_manual_ingest_result.json"
    probe_obj = None
    ingest_obj = None
    try:
        if probe.exists():
            raw = _json_load(probe)
            if isinstance(raw, dict):
                probe_obj = raw
    except Exception:
        probe_obj = None
    try:
        if ingest.exists():
            raw = _json_load(ingest)
            if isinstance(raw, dict):
                ingest_obj = raw
    except Exception:
        ingest_obj = None
    return probe_obj, ingest_obj


def _runner_cmd_for_job(args: argparse.Namespace, job: dict[str, Any]) -> list[str]:
    runner_python = args.runner_python or sys.executable or "python"
    cmd: list[str] = [str(runner_python), str(RUNNER_TOOL)]
    _add_arg(cmd, "--origin", job["origin"])
    _add_arg(cmd, "--destination", job["destination"])
    _add_arg(cmd, "--date", job["date"])
    _add_arg(cmd, "--cabin", job["cabin"])
    _add_arg(cmd, "--adt", job["adt"])
    _add_arg(cmd, "--chd", job["chd"])
    _add_arg(cmd, "--inf", job["inf"])
    _add_arg(cmd, "--cdp-url", args.cdp_url)
    _add_arg(cmd, "--proxy-server", args.proxy_server)
    _add_arg(cmd, "--user-data-dir", args.user_data_dir)
    _add_arg(cmd, "--session-root", args.session_root)
    _add_arg(cmd, "--timeout-s", args.timeout_s)
    _add_arg(cmd, "--poll-ms", args.poll_ms)
    if args.launch_cdp_browser:
        cmd.append("--launch-cdp-browser")
    if args.chrome_path:
        _add_arg(cmd, "--chrome-path", args.chrome_path)
    if args.keep_browser_open:
        cmd.append("--keep-browser-open")
    if args.open_index:
        cmd.append("--open-index")
    elif args.open_home:
        cmd.append("--open-home")
    if args.ingest:
        cmd.append("--ingest")
    if args.ingest_dry_run:
        cmd.append("--ingest-dry-run")
    return cmd


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sequential queue runner for Maldivian (Q2) PLNext manual-assisted capture (+ optional auto-ingest).",
        epilog=(
            "Queue file formats:\n"
            "  CSV headers: origin,destination,date[,cabin,adt,chd,inf,note]\n"
            "               Optional carrier/airline column is allowed but must be Q2.\n"
            "  JSON: [{...}, {...}] or {\"jobs\": [{...}]}\n"
            "\n"
            "Typical use:\n"
            "  python tools/maldivian_plnext_capture_batch_runner.py --queue-file jobs.csv --cdp-url http://127.0.0.1:9222 --ingest --open-home\n"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--queue-file", required=True, help="CSV/JSON file listing Q2 jobs")
    parser.add_argument("--cdp-url", default=DEFAULT_CDP_URL)
    parser.add_argument("--launch-cdp-browser", action="store_true")
    parser.add_argument("--chrome-path")
    parser.add_argument("--proxy-server")
    parser.add_argument("--user-data-dir")
    parser.add_argument("--session-root", default=str(REPO_ROOT / "output" / "manual_sessions"))
    parser.add_argument("--timeout-s", type=int, default=300)
    parser.add_argument("--poll-ms", type=int, default=500)
    parser.add_argument("--open-home", action=argparse.BooleanOptionalAction, default=True, help="Open Maldivian home page before each job if runner needs to navigate (default: on)")
    parser.add_argument("--open-index", action="store_true", help="Use direct Override.action navigation (less safe; can trigger Bad Request)")
    parser.add_argument("--cabin", default="Economy")
    parser.add_argument("--adt", type=int, default=1)
    parser.add_argument("--chd", type=int, default=0)
    parser.add_argument("--inf", type=int, default=0)
    parser.add_argument("--ingest", action=argparse.BooleanOptionalAction, default=True, help="Run auto-ingest after each successful capture (default: on)")
    parser.add_argument("--ingest-dry-run", action="store_true")
    parser.add_argument("--runner-python", help="Python executable to run maldivian_plnext_capture_runner.py (default: current interpreter)")
    parser.add_argument("--stop-on-error", action="store_true", help="Stop queue on first failed job")
    parser.add_argument("--sleep-sec", type=float, default=0.0, help="Sleep between jobs")
    parser.add_argument("--dry-run-queue", action="store_true", help="Validate/print queue without running jobs")
    parser.add_argument("--keep-browser-open", action="store_true")
    parser.add_argument("--print-command", action="store_true")
    parser.add_argument("--result-out", help="Write queue run summary JSON (default under output/manual_sessions/queue_runs)")
    args = parser.parse_args()

    if args.ingest_dry_run and not args.ingest:
        parser.error("--ingest-dry-run requires --ingest")
    if args.open_index and args.open_home is False:
        # allowed; explicit open-index wins. No action needed.
        pass

    queue_path = Path(args.queue_file)
    if not RUNNER_TOOL.exists():
        raise SystemExit(f"Capture runner not found: {RUNNER_TOOL}")

    raw_rows = _load_queue_rows(queue_path)
    jobs = [_normalize_job(r, defaults=args) for r in raw_rows]
    jobs = _dedupe_jobs(jobs)
    if not jobs:
        raise SystemExit("Queue is empty after parsing/deduplication")

    session_root = Path(args.session_root)
    queue_runs_dir = session_root / "queue_runs"
    queue_runs_dir.mkdir(parents=True, exist_ok=True)
    result_out = Path(args.result_out) if args.result_out else (queue_runs_dir / f"q2_queue_run_{_now_tag()}.json")

    summary: dict[str, Any] = {
        "started_at_utc": _now_utc_iso(),
        "queue_file": str(queue_path.resolve()),
        "queue_jobs_total": len(raw_rows),
        "queue_jobs_deduped": len(jobs),
        "defaults": {
            "carrier": "Q2",
            "cdp_url": args.cdp_url,
            "ingest": bool(args.ingest),
            "open_home": bool(args.open_home),
            "open_index": bool(args.open_index),
            "cabin": args.cabin,
            "adt": args.adt,
            "chd": args.chd,
            "inf": args.inf,
            "timeout_s": args.timeout_s,
            "poll_ms": args.poll_ms,
        },
        "jobs": jobs,
        "results": [],
    }

    print(f"[q2-batch] Loaded {len(raw_rows)} queue rows ({len(jobs)} after dedupe) from {queue_path}")
    for i, job in enumerate(jobs, start=1):
        print(f"[q2-batch] {i:03d}/{len(jobs)} Q2 {job['origin']}->{job['destination']} {job['date']} cabin={job['cabin']}")

    if args.dry_run_queue:
        summary["ended_at_utc"] = _now_utc_iso()
        summary["dry_run_queue"] = True
        result_out.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"[q2-batch] Dry-run only. Wrote queue summary: {result_out}")
        return 0

    failures = 0
    successes = 0

    for idx, job in enumerate(jobs, start=1):
        print("")
        print("=" * 80)
        print(f"[q2-batch] Starting job {idx}/{len(jobs)}: Q2 {job['origin']}->{job['destination']} {job['date']}")
        if job.get("note"):
            print(f"[q2-batch] Note: {job['note']}")
        started_ts = time.time()
        started_utc = _now_utc_iso()
        cmd = _runner_cmd_for_job(args, job)
        if args.print_command:
            print("[q2-batch] Runner command:")
            print("  " + subprocess.list2cmdline(cmd))
        proc = subprocess.run(cmd, cwd=str(REPO_ROOT))
        ended_utc = _now_utc_iso()

        run_dir = _find_run_dir_for_job(session_root, job, started_ts)
        probe_obj, ingest_obj = _load_run_artifacts(run_dir)
        rc = int(proc.returncode or 0)
        is_ok = rc == 0
        if is_ok:
            successes += 1
        else:
            failures += 1

        result_item: dict[str, Any] = {
            "index": idx,
            "job": job,
            "started_at_utc": started_utc,
            "ended_at_utc": ended_utc,
            "return_code": rc,
            "ok": is_ok,
            "run_dir": str(run_dir.resolve()) if run_dir else None,
        }
        if probe_obj:
            result_item["probe_summary"] = {
                "status": probe_obj.get("status"),
                "ok": probe_obj.get("ok"),
                "parsed_selected_days_rows_count": probe_obj.get("parsed_selected_days_rows_count"),
                "final_page_url": probe_obj.get("final_page_url"),
                "seen_fare_calls_count": len(probe_obj.get("seen_fare_calls") or []),
                "input_mismatch": probe_obj.get("parsed_selected_days_input_mismatch"),
            }
        if ingest_obj:
            result_item["ingest_summary"] = {
                "scrape_id": ingest_obj.get("scrape_id"),
                "dry_run": ingest_obj.get("dry_run"),
                "rows_parsed_total": ingest_obj.get("rows_parsed_total"),
                "rows_deduped_for_core": ingest_obj.get("rows_deduped_for_core"),
                "rows_inserted": ingest_obj.get("rows_inserted"),
            }
        summary["results"].append(result_item)

        status_label = "SUCCESS" if is_ok else "FAILED"
        rows_inserted = ((ingest_obj or {}).get("rows_inserted"))
        extra = f" rows_inserted={rows_inserted}" if rows_inserted is not None else ""
        print(f"[q2-batch] {status_label}: job {idx}/{len(jobs)} rc={rc}{extra}")

        summary["succeeded"] = successes
        summary["failed"] = failures
        summary["last_completed_job_index"] = idx
        summary["updated_at_utc"] = _now_utc_iso()
        result_out.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

        if not is_ok and args.stop_on_error:
            print("[q2-batch] Stopping due to --stop-on-error")
            break
        if idx < len(jobs) and args.sleep_sec > 0:
            print(f"[q2-batch] Sleeping {args.sleep_sec:.1f}s before next job...")
            time.sleep(args.sleep_sec)

    summary["ended_at_utc"] = _now_utc_iso()
    summary["succeeded"] = successes
    summary["failed"] = failures
    summary["ok"] = failures == 0
    result_out.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print("")
    print("[q2-batch] Queue summary")
    print(json.dumps(
        {
            "jobs_total": len(jobs),
            "succeeded": successes,
            "failed": failures,
            "ok": failures == 0,
            "result_out": str(result_out),
        },
        indent=2,
    ))
    return 0 if failures == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
