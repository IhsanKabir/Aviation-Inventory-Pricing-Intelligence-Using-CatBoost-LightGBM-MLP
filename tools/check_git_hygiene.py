from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]

# Runtime/operator-local paths that should never stay tracked.
RUNTIME_PREFIXES = (
    "output/latest/",
    "output/manual_sessions/",
    "output/reports/",
    "logs/",
)

# Runtime-like suffixes that often leak from manual reverse engineering.
RUNTIME_SUFFIXES = (
    ".har",
)


def _run_git(*args: str) -> str:
    proc = subprocess.run(
        ["git", *args],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"git {' '.join(args)} failed")
    return proc.stdout


def _tracked_runtime_files() -> list[str]:
    tracked = _run_git("ls-files").splitlines()
    out: list[str] = []
    for f in tracked:
        f_norm = f.replace("\\", "/")
        if any(f_norm.startswith(p) for p in RUNTIME_PREFIXES):
            out.append(f_norm)
            continue
        if f_norm.endswith(RUNTIME_SUFFIXES):
            out.append(f_norm)
    return out


def main() -> int:
    runtime_files = _tracked_runtime_files()
    payload = {
        "ok": len(runtime_files) == 0,
        "tracked_runtime_file_count": len(runtime_files),
        "tracked_runtime_files": runtime_files[:200],
    }
    print(json.dumps(payload, indent=2))
    return 0 if payload["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
