"""One progress readout for every long task in the app (discount run, schedule,
fare comparison), polled by the UI's progress bars.

A run happens on pywebview's worker thread while the page polls get_progress()
from another, so every update goes through a lock. The final state is kept after
a run ends - complete, failed or cancelled - so the bar can say plainly whether
the search finished, instead of just going quiet.
"""
from __future__ import annotations

import threading
import time
from typing import Any, Optional


class ProgressTracker:
    """Units done out of a total, the current step, and a final state."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._s: dict[str, Any] = {"task": "", "state": "idle", "done": 0, "total": 0,
                                   "label": "", "started": None, "ended": None}

    def start(self, task: str, total: int = 0, label: str = "Starting…") -> None:
        with self._lock:
            self._s = {"task": task, "state": "running", "done": 0,
                       "total": max(0, int(total)), "label": label,
                       "started": time.time(), "ended": None}

    def add_total(self, units: int) -> None:
        """Grow the total once more work is known (e.g. files found after a live pull)."""
        with self._lock:
            self._s["total"] = max(0, self._s["total"] + int(units))

    def tick(self, label: str, units: int = 1) -> None:
        with self._lock:
            if self._s["state"] != "running":
                return
            self._s["done"] += max(0, int(units))
            self._s["label"] = label

    def step(self, label: str, done: int, total: int) -> None:
        """Absolute position, for work that reports its own counts."""
        with self._lock:
            if self._s["state"] != "running":
                return
            self._s.update(done=max(0, int(done)), total=max(0, int(total)), label=label)

    def note(self, label: str) -> None:
        with self._lock:
            if self._s["state"] == "running":
                self._s["label"] = label

    def finish(self, state: str, label: str) -> None:
        """state: complete | failed | cancelled."""
        with self._lock:
            if self._s["state"] == "idle":
                return
            self._s.update(state=state, label=label, ended=time.time())
            if state == "complete":
                self._s["done"] = max(self._s["done"], self._s["total"])

    def snapshot(self, task: Optional[str] = None) -> dict[str, Any]:
        with self._lock:
            s = dict(self._s)
        if task and s["task"] != task:
            return {"task": task, "state": "idle", "percent": 0, "label": "", "elapsed_s": 0}
        total, done = s["total"], s["done"]
        if s["state"] == "complete":
            percent: Optional[int] = 100
        elif total:
            # Never show 100% until the run has actually finished.
            percent = min(99, round(done * 100 / total))
        else:
            percent = None                      # unknown yet: indeterminate bar
        end = s["ended"] or time.time()
        return {"task": s["task"], "state": s["state"], "done": done, "total": total,
                "percent": percent, "label": s["label"],
                "elapsed_s": round(end - s["started"]) if s["started"] else 0}
