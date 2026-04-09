from __future__ import annotations

import argparse
import base64
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from modules import gozayaan as gz


DEFAULT_SESSION_ROOT = REPO_ROOT / "output" / "manual_sessions"
FARES_URL_TOKEN = "/api/flight/v4.0/search/legs/fares/"


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_UTC")


def _json_dump(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def _response_text_from_har_entry(entry: Dict[str, Any]) -> str:
    response = entry.get("response") or {}
    content = response.get("content") or {}
    text = content.get("text")
    if not isinstance(text, str):
        return ""
    if str(content.get("encoding") or "").lower() == "base64":
        try:
            return base64.b64decode(text).decode("utf-8", errors="replace")
        except Exception:
            return ""
    return text


def _payload_items_from_har(har_payload: Dict[str, Any], source_path: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    entries = ((har_payload.get("log") or {}).get("entries") or [])
    for idx, entry in enumerate(entries):
        if not isinstance(entry, dict):
            continue
        req = entry.get("request") or {}
        url = str(req.get("url") or "")
        if FARES_URL_TOKEN not in url:
            continue
        response_body = gz._safe_json_loads(_response_text_from_har_entry(entry))
        if not isinstance(response_body, dict):
            continue
        request_body = gz._safe_json_loads(((req.get("postData") or {}).get("text") or ""))
        out.append(
            {
                "source_type": "har",
                "source_path": source_path,
                "entry_index": idx,
                "request_url": url,
                "request_body": request_body,
                "search_id": (request_body or {}).get("search_id"),
                "leg_hash": (request_body or {}).get("leg_hash"),
                "response_body": response_body,
            }
        )
    return out


def _payload_items_from_raw_json(raw_payload: Dict[str, Any], source_path: str) -> List[Dict[str, Any]]:
    return [
        {
            "source_type": "raw_json",
            "source_path": source_path,
            "entry_index": 0,
            "request_url": None,
            "request_body": None,
            "search_id": None,
            "leg_hash": None,
            "response_body": raw_payload,
        }
    ]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Import Gozayaan HAR/raw legs-fares captures into reusable manual-session artifacts.",
    )
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--har", help="Path to Gozayaan HAR file")
    src.add_argument("--legs-fares-json", help="Path to raw Gozayaan search/legs/fares JSON response")
    parser.add_argument("--session-root", default=str(DEFAULT_SESSION_ROOT), help="Root manual-session directory")
    parser.add_argument("--cabin", default="Economy")
    parser.add_argument("--adt", type=int, default=1)
    parser.add_argument("--chd", type=int, default=0)
    parser.add_argument("--inf", type=int, default=0)
    args = parser.parse_args()

    source_path = Path(args.har or args.legs_fares_json or "")
    if not source_path.exists():
        raise SystemExit(f"Source not found: {source_path}")

    payload_items: List[Dict[str, Any]]
    if args.har:
        har_payload = json.loads(source_path.read_text(encoding="utf-8-sig"))
        payload_items = _payload_items_from_har(har_payload, str(source_path.resolve()))
    else:
        raw_payload = json.loads(source_path.read_text(encoding="utf-8-sig"))
        if not isinstance(raw_payload, dict):
            raise SystemExit("Raw legs-fares JSON must be a JSON object")
        payload_items = _payload_items_from_raw_json(raw_payload, str(source_path.resolve()))

    groups = gz.extract_capture_groups_from_payload_items(
        payload_items,
        requested_cabin=args.cabin,
        adt=args.adt,
        chd=args.chd,
        inf=args.inf,
    )
    if not groups:
        print(json.dumps({"ok": False, "error": "no_capture_groups_found", "source": str(source_path.resolve())}, indent=2))
        return 1

    session_root = Path(args.session_root)
    results: List[Dict[str, Any]] = []
    for key, group in sorted(groups.items()):
        airline, origin, destination, date = key
        run_dir = session_root / "runs" / f"gozayaan_{airline}_{origin}_{destination}_{date}_{_now_tag()}"
        rows_path = run_dir / "gozayaan_rows.json"
        payloads_path = run_dir / "gozayaan_leg_fares_payloads.json"
        summary_path = run_dir / "gozayaan_capture_summary.json"

        rows = list(group.get("rows") or [])
        payloads = list(group.get("payload_items") or [])
        _json_dump(rows_path, rows)
        _json_dump(payloads_path, payloads)

        summary = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_path": str(source_path.resolve()),
            "source_type": "har" if args.har else "raw_json",
            "airline": airline,
            "origin": origin,
            "destination": destination,
            "date": date,
            "cabin_hint": args.cabin,
            "adt": args.adt,
            "chd": args.chd,
            "inf": args.inf,
            "rows_count": len(rows),
            "payload_count": len(payloads),
            "rows_path": str(rows_path.resolve()),
            "payloads_path": str(payloads_path.resolve()),
            "sample_rows": rows[:3],
        }
        _json_dump(summary_path, summary)

        results.append(
            {
                "run_dir": str(run_dir.resolve()),
                "summary_path": str(summary_path.resolve()),
                "airline": airline,
                "origin": origin,
                "destination": destination,
                "date": date,
                "rows_count": len(rows),
            }
        )

    print(json.dumps({"ok": True, "groups_created": len(results), "results": results}, indent=2, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
