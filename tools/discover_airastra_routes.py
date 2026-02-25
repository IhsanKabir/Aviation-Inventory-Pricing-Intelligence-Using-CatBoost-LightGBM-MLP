"""
Discover Air Astra (2A) routes from the TTInteractive bootstrap config.

Examples:
  python tools/discover_airastra_routes.py --domestic-only
  python tools/discover_airastra_routes.py --origin DAC --origin CGP --output output/airastra_routes.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from modules import airastra


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--domestic-only", action="store_true", help="Only keep BD->BD routes")
    parser.add_argument("--origin", action="append", default=[], help="Filter to one or more origin airports")
    parser.add_argument("--output", help="Write JSON to file instead of stdout")
    args = parser.parse_args()

    entries = airastra.discover_route_entries(
        domestic_only=args.domestic_only,
        allowed_origins=args.origin,
    )

    payload = json.dumps(entries, indent=2, ensure_ascii=False)
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(payload, encoding="utf-8")
        print(f"Wrote {len(entries)} 2A routes to {out_path}")
        return

    print(payload)


if __name__ == "__main__":
    main()
