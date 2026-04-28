from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.extraction_health import load_attempts_from_db, write_health_reports
from db import DATABASE_URL


def _parse_airlines(raw: str | None) -> list[str]:
    return [part.strip().upper() for part in str(raw or "").split(",") if part.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Regenerate extraction health reports from extraction_attempts.")
    parser.add_argument("--cycle-id")
    parser.add_argument("--scrape-id")
    parser.add_argument("--db-url", default=DATABASE_URL)
    parser.add_argument("--output-dir", default="output/reports")
    parser.add_argument("--expected-airlines", help="Comma-separated expected airline codes for coverage gating.")
    args = parser.parse_args()

    if not args.cycle_id and not args.scrape_id:
        parser.error("--cycle-id or --scrape-id is required")
    attempts = load_attempts_from_db(args.db_url, cycle_id=args.cycle_id, scrape_id=args.scrape_id)
    report = write_health_reports(
        attempts,
        output_dir=Path(args.output_dir),
        cycle_id=args.cycle_id or args.scrape_id,
        expected_airlines=_parse_airlines(args.expected_airlines),
    )
    print(
        "extraction_health_report "
        f"status={report.get('status')} attempts={report.get('attempt_count')} "
        f"failures={report.get('failure_count')} manual={report.get('manual_action_required_count')}"
    )
    print(json.dumps(report.get("artifacts") or {}, indent=2))
    return 0 if report.get("status") != "FAIL" else 2


if __name__ == "__main__":
    raise SystemExit(main())
