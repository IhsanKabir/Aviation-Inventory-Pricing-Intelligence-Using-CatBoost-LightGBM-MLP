from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from modules import maldivian as q2


DEFAULT_SESSION_ROOT = REPO_ROOT / "output" / "manual_sessions"


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_UTC")


def _json_dump(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Import Maldivian (Q2) fare data from a HAR into manual-session artifacts.",
    )
    parser.add_argument("--har", required=True, help="Path to book.maldivian.aero HAR file")
    parser.add_argument("--session-root", default=str(DEFAULT_SESSION_ROOT), help="Root manual-session directory")
    parser.add_argument("--cabin", default="Economy")
    parser.add_argument("--adt", type=int, default=1)
    parser.add_argument("--chd", type=int, default=0)
    parser.add_argument("--inf", type=int, default=0)
    args = parser.parse_args()

    har_path = Path(args.har)
    if not har_path.exists():
        raise SystemExit(f"HAR not found: {har_path}")

    har_payload = json.loads(har_path.read_text(encoding="utf-8-sig"))
    extracted = q2.extract_fare_capture_from_har(
        har_payload,
        requested_cabin=args.cabin,
        adt=args.adt,
        chd=args.chd,
        inf=args.inf,
        source_har_path=str(har_path.resolve()),
    )
    if not extracted.get("ok") or not isinstance(extracted.get("fare_payload"), dict):
        summary = dict(extracted)
        summary.pop("fare_payload", None)
        print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))
        return 1

    origin = str(extracted.get("origin") or "UNK").upper()
    destination = str(extracted.get("destination") or "UNK").upper()
    date = str(extracted.get("date") or "unknown-date")[:10]

    session_root = Path(args.session_root)
    run_dir = session_root / "runs" / f"q2_{origin}_{destination}_{date}_{_now_tag()}"
    fare_json_path = run_dir / "q2_fare_uid_response.json"
    summary_path = run_dir / "q2_probe_response.json"

    _json_dump(fare_json_path, extracted["fare_payload"])

    rows = extracted.get("rows") or []
    summary = {
        "carrier": "Q2",
        "status": extracted.get("status"),
        "ok": bool(rows),
        "origin": origin,
        "destination": destination,
        "date": date,
        "cabin": extracted.get("cabin") or args.cabin,
        "adt": int(extracted.get("adt") or args.adt),
        "chd": int(extracted.get("chd") or args.chd),
        "inf": int(extracted.get("inf") or args.inf),
        "fare_uid_url": extracted.get("fare_uid_url"),
        "fare_uid_request_body": extracted.get("fare_uid_request_body"),
        "fare_uid_response_path": str(fare_json_path.resolve()),
        "source_har_path": str(har_path.resolve()),
        "source_har_entry_index": extracted.get("fare_entry_index"),
        "seen_fare_calls": extracted.get("seen_fare_calls") or [],
        "parsed_selected_days_rows_count": len(rows),
        "parsed_selected_days_sample_rows": rows[:3],
        "parsed_selected_days_input_mismatch": None,
    }
    _json_dump(summary_path, summary)

    out = {
        "ok": True,
        "run_dir": str(run_dir.resolve()),
        "summary_path": str(summary_path.resolve()),
        "fare_json_path": str(fare_json_path.resolve()),
        "origin": origin,
        "destination": destination,
        "date": date,
        "rows_count": len(rows),
        "sample_rows": rows[:3],
    }
    print(json.dumps(out, indent=2, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
