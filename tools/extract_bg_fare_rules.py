"""
Extract structured BG penalty fields from a captured getBookingFareRules response.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.penalties import extract_bg_penalties_from_graphql_response


def parse_args():
    p = argparse.ArgumentParser(description="Extract BG fare-rule penalties from GraphQL response JSON")
    p.add_argument("--input", required=True, help="Path to saved getBookingFareRules JSON response")
    p.add_argument("--output-dir", default="output/reports", help="Output directory")
    p.add_argument("--fare-basis", help="Optional fare-basis filter")
    return p.parse_args()


def _write_csv(path: Path, rows: List[Dict[str, Any]]):
    cols = sorted({k for r in rows for k in r.keys()}) if rows else []
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in cols})


def main():
    args = parse_args()
    p = Path(args.input)
    if not p.exists():
        raise SystemExit(f"Input file not found: {p}")

    payload = json.loads(p.read_text(encoding="utf-8"))
    rows = extract_bg_penalties_from_graphql_response(payload, fare_basis_filter=args.fare_basis)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    basis_token = (args.fare_basis or "all").replace("/", "_")
    json_path = out_dir / f"bg_fare_rule_penalties_{basis_token}_{ts}.json"
    csv_path = out_dir / f"bg_fare_rule_penalties_{basis_token}_{ts}.csv"
    latest_path = out_dir / "bg_fare_rule_penalties_latest.json"

    json_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    _write_csv(csv_path, rows)
    latest_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": str(p),
        "fare_basis_filter": args.fare_basis,
        "rows": len(rows),
        "json": str(json_path),
        "csv": str(csv_path),
    }
    latest_path.write_text(json.dumps(latest_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"bg_penalty_rows={len(rows)} -> {csv_path}")
    print(f"latest -> {latest_path}")


if __name__ == "__main__":
    raise SystemExit(main())

