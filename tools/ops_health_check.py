"""
Operational health check for scheduler/pipeline logs.

Usage examples:
  python tools/ops_health_check.py --hours 24
  python tools/ops_health_check.py --hours 24 --output output/reports/ops_health_latest.md
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple


DEFAULT_LOGS = [
    "logs/scheduler_bg.err.log",
    "logs/scheduler_bg.out.log",
    "logs/scheduler_vq.err.log",
    "logs/scheduler_vq.out.log",
    "logs/run_all.log",
]

LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\s+\[(?P<level>[A-Z]+)\]\s+(?P<logger>[^:]+):\s*(?P<msg>.*)$"
)

FAIL_KEYWORDS = (
    "traceback",
    "exception",
    "module not found",
    "failed",
    "error",
)


def parse_args():
    p = argparse.ArgumentParser(description="Summarize ops health from recent log lines")
    p.add_argument("--hours", type=float, default=24.0, help="Time window to analyze")
    p.add_argument("--log", action="append", dest="logs", help="Additional log file paths")
    p.add_argument("--output", help="Optional markdown output path")
    return p.parse_args()


def parse_line(line: str) -> Optional[Tuple[dt.datetime, str, str, str]]:
    m = LINE_RE.match(line.rstrip("\n"))
    if not m:
        return None
    ts = dt.datetime.strptime(m.group("ts"), "%Y-%m-%d %H:%M:%S")
    return ts, m.group("level"), m.group("logger"), m.group("msg")


def collect_events(paths: List[Path], cutoff: dt.datetime):
    events = []
    unreadable = []
    for path in paths:
        if not path.exists():
            unreadable.append((str(path), "missing"))
            continue
        try:
            text_blob = None
            for enc in ("utf-8", "utf-16", "utf-16-le"):
                try:
                    text_blob = path.read_text(encoding=enc)
                    break
                except Exception:
                    continue
            if text_blob is None:
                text_blob = path.read_text(encoding="utf-8", errors="ignore")

            for line in text_blob.splitlines():
                parsed = parse_line(line)
                if not parsed:
                    continue
                ts, level, logger, msg = parsed
                if ts >= cutoff:
                    events.append((str(path), ts, level, logger, msg))
        except Exception as exc:
            unreadable.append((str(path), f"read_error:{exc}"))
    return events, unreadable


def summarize(events):
    by_level: Dict[str, int] = {}
    by_file: Dict[str, int] = {}
    fail_hits = []
    pipeline_rc_nonzero = []
    scheduler_runs = 0
    row_emits = 0

    latest_ts = None
    earliest_ts = None

    for file_path, ts, level, logger, msg in events:
        by_level[level] = by_level.get(level, 0) + 1
        by_file[file_path] = by_file.get(file_path, 0) + 1

        if earliest_ts is None or ts < earliest_ts:
            earliest_ts = ts
        if latest_ts is None or ts > latest_ts:
            latest_ts = ts

        low = msg.lower()
        if "pipeline finished rc=" in low:
            scheduler_runs += 1
            if "rc=0" not in low:
                pipeline_rc_nonzero.append((file_path, ts, msg))

        if "done. total rows:" in low or "saved csv:" in low:
            row_emits += 1

        if any(k in low for k in FAIL_KEYWORDS):
            # keep noisy "no rows" out of failure hits
            if "no rows for" not in low and "no rows to write" not in low:
                fail_hits.append((file_path, ts, level, msg))

    status = "PASS"
    reasons = []
    if pipeline_rc_nonzero:
        status = "FAIL"
        reasons.append(f"{len(pipeline_rc_nonzero)} pipeline runs ended with non-zero rc")
    if fail_hits:
        # warnings with "failed" still matter but not always fatal
        status = "WARN" if status != "FAIL" else status
        reasons.append(f"{len(fail_hits)} failure-keyword log hits")

    return {
        "status": status,
        "reasons": reasons,
        "events_in_window": len(events),
        "earliest_ts": earliest_ts,
        "latest_ts": latest_ts,
        "by_level": by_level,
        "by_file": by_file,
        "scheduler_runs": scheduler_runs,
        "pipeline_rc_nonzero": pipeline_rc_nonzero,
        "row_emit_signals": row_emits,
        "failure_hits": fail_hits[:25],  # cap
    }


def to_markdown(summary, unreadable, hours):
    lines = []
    lines.append(f"# Ops Health Check ({hours}h)")
    lines.append("")
    lines.append(f"- Status: **{summary['status']}**")
    if summary["reasons"]:
        lines.append(f"- Reasons: {', '.join(summary['reasons'])}")
    lines.append(f"- Events in window: {summary['events_in_window']}")
    lines.append(f"- Scheduler run signals: {summary['scheduler_runs']}")
    lines.append(f"- Row-emission signals: {summary['row_emit_signals']}")
    lines.append(f"- Time range: {summary['earliest_ts']} -> {summary['latest_ts']}")
    lines.append("")

    lines.append("## Level Counts")
    for k in sorted(summary["by_level"].keys()):
        lines.append(f"- {k}: {summary['by_level'][k]}")
    if not summary["by_level"]:
        lines.append("- none")
    lines.append("")

    lines.append("## File Coverage")
    for k in sorted(summary["by_file"].keys()):
        lines.append(f"- {k}: {summary['by_file'][k]} events")
    if unreadable:
        for path, err in unreadable:
            lines.append(f"- {path}: {err}")
    lines.append("")

    lines.append("## Non-zero Pipeline RC")
    if summary["pipeline_rc_nonzero"]:
        for fp, ts, msg in summary["pipeline_rc_nonzero"]:
            lines.append(f"- {ts} | {fp} | {msg}")
    else:
        lines.append("- none")
    lines.append("")

    lines.append("## Failure Keyword Hits (sample)")
    if summary["failure_hits"]:
        for fp, ts, lvl, msg in summary["failure_hits"]:
            lines.append(f"- {ts} [{lvl}] {fp} :: {msg}")
    else:
        lines.append("- none")
    lines.append("")
    return "\n".join(lines)


def main():
    args = parse_args()
    now = dt.datetime.now()
    cutoff = now - dt.timedelta(hours=args.hours)
    logs = [Path(p) for p in DEFAULT_LOGS]
    if args.logs:
        logs.extend(Path(p) for p in args.logs)

    events, unreadable = collect_events(logs, cutoff)
    summary = summarize(events)
    md = to_markdown(summary, unreadable, args.hours)
    print(md)

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(md, encoding="utf-8")
        print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
