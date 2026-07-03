"""Best-effort sync outbox: sanitized report payloads queued while offline.

Sync must NEVER block or lose a report — on any network/auth failure the payload
is written here (one JSON file per report_date, newest wins) and retried on the
next launch / manual "Sync now". Only SANITIZED payloads are ever enqueued.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class Outbox:
    def __init__(self, directory: Path) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def _path_for(self, report_date_iso: str) -> Path:
        return self.directory / f"report_{report_date_iso}.json"

    def enqueue(self, report_date_iso: str, payload: dict[str, Any]) -> None:
        """Queue (or replace) the pending payload for a report date."""
        self._path_for(report_date_iso).write_text(
            json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    def pending(self) -> list[tuple[str, dict[str, Any]]]:
        """[(report_date_iso, payload)] oldest date first."""
        items: list[tuple[str, dict[str, Any]]] = []
        for path in sorted(self.directory.glob("report_*.json")):
            date_iso = path.stem.replace("report_", "", 1)
            try:
                items.append((date_iso, json.loads(path.read_text(encoding="utf-8"))))
            except (OSError, json.JSONDecodeError):
                continue    # unreadable entry: leave the file for inspection, skip
        return items

    def mark_done(self, report_date_iso: str) -> None:
        self._path_for(report_date_iso).unlink(missing_ok=True)

    def reject(self, report_date_iso: str) -> None:
        """Move a permanently-refused payload (403) out of the retry queue into
        rejected/ so it stops re-firing, but keep it for inspection."""
        src = self._path_for(report_date_iso)
        if not src.exists():
            return
        rejected_dir = self.directory / "rejected"
        rejected_dir.mkdir(parents=True, exist_ok=True)
        try:
            src.replace(rejected_dir / src.name)
        except OSError:
            src.unlink(missing_ok=True)

    def count(self) -> int:
        return len(list(self.directory.glob("report_*.json")))
