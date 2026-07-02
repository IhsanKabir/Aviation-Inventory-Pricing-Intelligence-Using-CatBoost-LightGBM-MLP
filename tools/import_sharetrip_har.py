"""
Import ShareTrip HAR captures into the offer cache used by the KSA report.

Capture a HAR from a browser search on https://sharetrip.net (Network tab,
"Save all as HAR with content"), then run this importer. Fares are BDT-native.
All airlines are parsed (incl. G9 / Air Arabia).

Usage:
  python tools/import_sharetrip_har.py path\\to\\search.har [more.har ...]
"""
from __future__ import annotations

import glob
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from modules.sharetrip_har import CACHE_PATH, import_hars


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__)
        return 1
    paths: list[str] = []
    for arg in argv:
        paths.extend(glob.glob(arg) or [arg])
    paths = [p for p in paths if Path(p).is_file()]
    if not paths:
        print("No HAR files found.")
        return 1
    print(f"Importing {len(paths)} ShareTrip HAR file(s)...")
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
