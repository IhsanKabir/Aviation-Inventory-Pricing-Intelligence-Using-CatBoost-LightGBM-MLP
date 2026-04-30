"""Atomic JSON / text file writers.

Direct ``Path.write_text`` is not atomic — a process killed mid-write leaves a
truncated file that downstream readers silently treat as valid. Capture
summaries, session blobs, and rate-limit state files are read by the pipeline
on every cycle, so a half-written ``*_latest.json`` corrupts the next run.

These helpers write to a temporary sibling file then ``os.replace`` over the
target. ``os.replace`` is atomic on NTFS and POSIX, so readers see either the
old content or the new content — never a partial write.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any


def atomic_write_text(path: Path, content: str, *, encoding: str = "utf-8") -> None:
    """Write ``content`` to ``path`` atomically via a temporary sibling file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding=encoding, newline="") as fh:
            fh.write(content)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, path)
    except Exception:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
        raise


def atomic_write_json(
    path: Path,
    payload: Any,
    *,
    indent: int | None = 2,
    ensure_ascii: bool = False,
    default: Any = None,
) -> None:
    """Serialize ``payload`` to JSON and write atomically to ``path``."""
    text_value = json.dumps(payload, indent=indent, ensure_ascii=ensure_ascii, default=default)
    atomic_write_text(Path(path), text_value)
