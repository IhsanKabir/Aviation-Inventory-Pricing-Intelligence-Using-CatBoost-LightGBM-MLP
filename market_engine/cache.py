"""Query cache with caller-declared freshness.

A cached row means two different things depending on who is asking, and
collapsing that distinction would be a correctness bug rather than a
performance detail:

  * A SCHEDULE is stable week to week. A row fetched days ago still truthfully
    says "BS 361 operated DAC-JED at 17:20", so reusing it is honest.
  * A FARE moves daily. Reusing a week-old price and presenting it as today's
    market rate is simply wrong.

So the store keeps one copy and the CALLER states how fresh it needs the answer
to be (`max_age`). Every row also carries its own `fetched_at`, so the UI can
label what it is showing rather than implying it is live.

Cache failures are never fatal: a broken cache degrades to refetching.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional

from market_engine.rows import FlightRow

#: A schedule stays meaningful for weeks; a fare does not survive the day.
FRESH_SCHEDULE = timedelta(days=21)
FRESH_FARE = timedelta(hours=8)


def _key(source: str, origin: str, dest: str, day: str, cabin: str) -> str:
    return f"{source}|{origin.upper()}|{dest.upper()}|{day}|{cabin.lower()}"


def _encode(rows: Iterable[FlightRow]) -> str:
    out = []
    for r in rows:
        d = asdict(r)
        for k in ("departure_date", "arrival_date"):
            d[k] = d[k].isoformat() if d[k] else None
        d["fetched_at"] = d["fetched_at"].isoformat()
        out.append(d)
    return json.dumps(out, separators=(",", ":"))


def _decode(blob: str) -> list[FlightRow]:
    rows = []
    for d in json.loads(blob):
        for k in ("departure_date", "arrival_date"):
            d[k] = date.fromisoformat(d[k]) if d[k] else None
        d["fetched_at"] = datetime.fromisoformat(d["fetched_at"])
        rows.append(FlightRow(**d))
    return rows


class CacheStore:
    """SQLite-backed (stdlib, bundles cleanly into the exe)."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.hits = 0
        self.misses = 0
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._db: Optional[sqlite3.Connection] = sqlite3.connect(
                str(self.path), check_same_thread=False)
            self._db.execute(
                "create table if not exists q ("
                " k text primary key, fetched_at text not null, payload text not null)")
            self._db.commit()
        except Exception:                 # noqa: BLE001 — run without a cache rather than fail
            self._db = None

    def get(self, source: str, origin: str, dest: str, day: str, cabin: str,
            *, max_age: timedelta) -> Optional[list[FlightRow]]:
        if self._db is None:
            return None
        try:
            row = self._db.execute("select fetched_at, payload from q where k=?",
                                   (_key(source, origin, dest, day, cabin),)).fetchone()
            if not row:
                self.misses += 1
                return None
            if datetime.now() - datetime.fromisoformat(row[0]) > max_age:
                self.misses += 1          # present but too old for THIS purpose
                return None
            self.hits += 1
            return _decode(row[1])
        except Exception:                 # noqa: BLE001
            self.misses += 1
            return None

    def put(self, source: str, origin: str, dest: str, day: str, cabin: str,
            rows: Iterable[FlightRow]) -> None:
        if self._db is None:
            return
        try:
            self._db.execute(
                "insert or replace into q (k, fetched_at, payload) values (?,?,?)",
                (_key(source, origin, dest, day, cabin),
                 datetime.now().isoformat(), _encode(rows)))
            self._db.commit()
        except Exception:                 # noqa: BLE001
            pass

    def stats(self) -> dict[str, int]:
        n = 0
        if self._db is not None:
            try:
                n = int(self._db.execute("select count(*) from q").fetchone()[0])
            except Exception:             # noqa: BLE001
                n = 0
        return {"entries": n, "hits": self.hits, "misses": self.misses}
