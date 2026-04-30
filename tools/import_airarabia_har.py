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

from core.atomic_write import atomic_write_json
from modules import airarabia as g9


DEFAULT_SESSION_ROOT = REPO_ROOT / "output" / "manual_sessions"


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_UTC")


def _json_dump(path: Path, payload: Any) -> None:
    atomic_write_json(path, payload, default=str)


def main() -> int:
    parser = argparse.ArgumentParser(description="Import Air Arabia (G9) fare data from a HAR into manual-session artifacts.")
    parser.add_argument("--har", required=True, help="Path to www.airarabia.com HAR file")
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
    extracted = g9.extract_fare_capture_from_har(har_payload, requested_cabin=args.cabin, adt=args.adt, chd=args.chd, inf=args.inf, source_har_path=str(har_path.resolve()))
    if not extracted.get("ok") or not isinstance(extracted.get("response_body"), dict):
        summary = dict(extracted)
        summary.pop("response_body", None)
        print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))
        return 1

    origin = str(extracted.get("origin") or "UNK").upper()
    destination = str(extracted.get("destination") or "UNK").upper()
    date = str(extracted.get("date") or "unknown-date")[:10]
    session_root = Path(args.session_root)
    run_dir = session_root / "runs" / f"g9_{origin}_{destination}_{date}_{_now_tag()}"
    response_path = run_dir / "airarabia_flight_search_fare_response.json"
    summary_path = run_dir / "airarabia_capture_summary.json"

    _json_dump(response_path, extracted["response_body"])
    summary = {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "carrier": "G9",
        "ok": True,
        "origin": origin,
        "destination": destination,
        "date": date,
        "cabin": args.cabin,
        "adt": args.adt,
        "chd": args.chd,
        "inf": args.inf,
        "request_url": extracted.get("request_url"),
        "request_body": extracted.get("request_body"),
        "response_json_path": str(response_path.resolve()),
        "response_body": extracted.get("response_body"),
        "source_har_path": str(har_path.resolve()),
        "source_har_entry_index": extracted.get("fare_entry_index"),
        "rows_count": len(extracted.get("rows") or []),
        "sample_rows": (extracted.get("rows") or [])[:3],
    }
    _json_dump(summary_path, summary)
    print(json.dumps({"ok": True, "run_dir": str(run_dir.resolve()), "summary_path": str(summary_path.resolve()), "rows_count": len(extracted.get("rows") or []), "sample_rows": (extracted.get("rows") or [])[:3]}, indent=2, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
