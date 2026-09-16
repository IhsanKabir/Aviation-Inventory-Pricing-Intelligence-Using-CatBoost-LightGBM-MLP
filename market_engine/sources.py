"""Source registry — what each source can actually answer, and how fast.

Two things live here that the rest of the engine must never guess:

1. CAPABILITY. Sources are not interchangeable. A HAR channel that carries no
   flight number cannot support a schedule no matter how many rows it returns
   (the schedule engine drops those as `dropped_no_flight`), and one that
   carries no clock cannot either. Declaring this up front lets the UI grey the
   source out with a reason instead of running for minutes and showing nothing.

2. PACE. Rate limits are per-source-family, not global. FirstTrip tolerates a
   few workers; ShareTrip and the agent sources must stay single-threaded with a
   pause between queries — exceeding that is what draws blocks.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Optional

from market_engine.rows import FlightRow, from_connector


@dataclass(frozen=True)
class Source:
    key: str
    label: str
    kind: str                 # "live" | "har"
    can_schedule: bool        # emits flight_number + a real clock
    can_fare: bool            # emits a usable price
    workers: int = 1          # max parallel queries (family rule)
    sleep_s: float = 0.0      # pause between queries
    note: str = ""


#: Live connectors. FirstTrip is the backbone: one call returns every airline on
#: the route with real clocks and both gross and base fares.
LIVE: dict[str, Source] = {
    "firsttrip": Source("firsttrip", "FirstTrip", "live", True, True,
                        workers=3, sleep_s=0.0, note="Backbone — all airlines, gross + base."),
    "sharetrip": Source("sharetrip", "ShareTrip", "live", True, True,
                        workers=1, sleep_s=3.0, note="Private plugin; single-threaded by policy."),
    "biman": Source("biman", "Biman (direct)", "live", True, True,
                    workers=1, sleep_s=1.5, note="BG only; slow (Sabre)."),
    "amy": Source("amy", "Amy", "live", True, True,
                  workers=1, sleep_s=1.5, note="Best-effort; skips when its token is stale."),
}

#: HAR channels. Capability here is VERIFIED against the parsers on real captures,
#: not inferred: a channel whose rows carry no flight number cannot support a
#: schedule, because the schedule engine drops those rows by design.
HAR: dict[str, Source] = {
    "sharetrip": Source("sharetrip", "ShareTrip (HAR)", "har", True, True),
    "gozayaan": Source("gozayaan", "GoZayaan (HAR)", "har", True, True),
    "firsttrip_b2c": Source("firsttrip_b2c", "FirstTrip B2C (HAR)", "har", True, True,
                            note="Needs a SEARCH capture; a booking-page HAR has no offers."),
    "amy": Source("amy", "Amy (HAR)", "har", False, True,
                  note="Agent rows carry no flight number - fares only, no schedule."),
    "bdfare": Source("bdfare", "BDFare (HAR)", "har", False, False,
                     note="No flight-row parser - cannot feed these views."),
    "akij": Source("akij", "AKIJ (HAR)", "har", False, False,
                   note="No flight-row parser - cannot feed these views."),
}


def usable(sources: dict[str, Source], *, purpose: str) -> list[Source]:
    """Sources that can serve `purpose` ('schedule' or 'fare')."""
    want = (lambda s: s.can_schedule) if purpose == "schedule" else (lambda s: s.can_fare)
    return [s for s in sources.values() if want(s)]


def live_available() -> dict[str, bool]:
    """Which live sources this machine can actually run right now.

    ShareTrip is the private plugin — absent on a standard install, and the UI
    must simply not offer it rather than fail at run time.
    """
    out: dict[str, bool] = {}
    for key, mod in (("firsttrip", "modules.firsttrip"), ("biman", "modules.biman"),
                     ("amy", "modules.amyweb"), ("sharetrip", "modules.sharetrip_live")):
        try:
            __import__(mod)
            out[key] = True
        except Exception:      # noqa: BLE001 — a missing/broken source is just unavailable
            out[key] = False
    return out


class LiveFetcher:
    """One source's live fetch, normalized to FlightRow and stateful where it pays.

    ShareTrip mints a token that is reusable across many searches, so it is held
    here rather than re-minted per query (re-minting every call is both slow and
    exactly the hammering the module warns against).
    """

    def __init__(self, key: str) -> None:
        self.key = key
        self.source = LIVE[key]
        self._token: Optional[str] = None

    def __call__(self, origin: str, dest: str, day: str,
                 cabin: str = "Economy") -> tuple[list[FlightRow], str]:
        """-> (rows, reason). `reason` is '' on success, else why it returned nothing."""
        try:
            raw, reason = self._raw(origin, dest, day, cabin)
        except Exception as exc:            # noqa: BLE001 — one bad query must not kill a run
            return [], f"{type(exc).__name__}: {exc}"
        now = datetime.now()
        rows = [r for r in (from_connector(x, source=self.source.label, cabin=cabin,
                                           fetched_at=now) for x in raw) if r]
        if not rows and not reason:
            reason = "no flights returned"
        return rows, reason

    def _raw(self, origin: str, dest: str, day: str, cabin: str) -> tuple[list[dict], str]:
        if self.key == "sharetrip":
            from modules import sharetrip_live as st
            res = st.fetch_live(origin, dest, day, cabin=cabin, token=self._token)
            self._token = res.get("token") or self._token   # reuse across queries
            return list(res.get("rows") or []), str(res.get("reason") or "")
        mod = {"firsttrip": "modules.firsttrip", "biman": "modules.biman",
               "amy": "modules.amyweb"}[self.key]
        fetch = __import__(mod, fromlist=["fetch_flights"]).fetch_flights
        res = fetch(origin, dest, day, cabin=cabin) or {}
        return list(res.get("rows") or []), str(res.get("reason") or res.get("error") or "")
