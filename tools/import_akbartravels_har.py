"""
Import AkbarTravels HAR captures into the offer cache used by the KSA report.

AkbarTravels is behind an AWS WAF (JS challenge) that blocks scripted requests,
so capture a HAR from a real browser search, then run this importer.

How to capture:
  1. Chrome > F12 > Network tab (tick "Preserve log").
  2. Search the route/date on https://www.akbartravels.com.
  3. Right-click the network list > "Save all as HAR with content".

Usage:
  python tools/import_akbartravels_har.py path\\to\\search.har [more.har ...]
  python tools/import_akbartravels_har.py downloads\\*.har
"""
from __future__ import annotations

import glob
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from modules.akbartravels import CACHE_PATH, import_hars


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__)
        return 1
    paths: list[str] = []
    for arg in argv:
        matches = glob.glob(arg)
        paths.extend(matches or [arg])
    paths = [p for p in paths if Path(p).is_file()]
    if not paths:
        print("No HAR files found.")
        return 1

    print(f"Importing {len(paths)} HAR file(s)...")
    stats = import_hars(paths)
    print(f"  offers imported : {stats['offers_imported']}")
    print(f"  route+date keys : {len(stats['keys_updated'])}")
    for k in stats["keys_updated"]:
        print(f"     - {k}")
    print(f"  total cache keys: {stats['total_cache_keys']}")
    print(f"  cache file      : {CACHE_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
