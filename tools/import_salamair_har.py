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

from modules import salamair as ov


DEFAULT_SESSION_ROOT = REPO_ROOT / "output" / "manual_sessions"


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_UTC")


def _json_dump(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Import SalamAir (OV) fare data from a HAR into manual-session artifacts.")
    parser.add_argument("--har", required=True, help="Path to booking.salamair.com HAR file")
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
    extracted = ov.extract_capture_from_har(har_payload, requested_cabin=args.cabin, adt=args.adt, chd=args.chd, inf=args.inf, source_har_path=str(har_path.resolve()))
    if not extracted.get("ok") or not isinstance(extracted.get("flight_fares_response_body"), dict):
        summary = dict(extracted)
        summary.pop("flight_fares_response_body", None)
        summary.pop("confirm_response_body", None)
        print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))
        return 1

    origin = str(extracted.get("origin") or "UNK").upper()
    destination = str(extracted.get("destination") or "UNK").upper()
    date = str(extracted.get("date") or "unknown-date")[:10]
    session_root = Path(args.session_root)
    run_dir = session_root / "runs" / f"ov_{origin}_{destination}_{date}_{_now_tag()}"
    fares_path = run_dir / "salamair_flight_fares_response.json"
    confirm_path = run_dir / "salamair_confirm_response.json"
    summary_path = run_dir / "salamair_capture_summary.json"

    _json_dump(fares_path, extracted["flight_fares_response_body"])
    if isinstance(extracted.get("confirm_response_body"), dict):
        _json_dump(confirm_path, extracted["confirm_response_body"])

    summary = {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "carrier": "OV",
        "ok": True,
        "origin": origin,
        "destination": destination,
        "date": date,
        "cabin": args.cabin,
        "adt": args.adt,
        "chd": args.chd,
        "inf": args.inf,
        "flight_fares_request_body": extracted.get("flight_fares_request_body"),
        "flight_fares_response_body_path": str(fares_path.resolve()),
        "flight_fares_response_body": extracted.get("flight_fares_response_body"),
        "confirm_response_body_path": str(confirm_path.resolve()) if isinstance(extracted.get("confirm_response_body"), dict) else None,
        "confirm_response_body": extracted.get("confirm_response_body"),
        "source_har_path": str(har_path.resolve()),
        "source_har_entry_index": extracted.get("flight_fares_entry_index"),
        "rows_count": len(extracted.get("rows") or []),
        "sample_rows": (extracted.get("rows") or [])[:3],
    }
    _json_dump(summary_path, summary)
    print(json.dumps({"ok": True, "run_dir": str(run_dir.resolve()), "summary_path": str(summary_path.resolve()), "rows_count": len(extracted.get("rows") or []), "sample_rows": (extracted.get("rows") or [])[:3]}, indent=2, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
