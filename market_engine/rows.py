"""Normalized flight row — the single shape every source must emit.

Live connectors and HAR parsers disagree on field names and on how they express
a departure (FirstTrip returns an ISO datetime; the schedule engine wants a date
plus an "HH:MM" clock). Normalizing here means the schedule and fare layers never
learn anything source-specific.

Honesty rules baked into the row itself:
  * `base_is_real` records whether the SOURCE exposed a base fare or we derived
    it, so a derived number is never presented as observed.
  * `departure_time` is "" when the source had no real clock — never a synthetic
    midnight, which the schedule engine would otherwise read as a 00:00 flight.
  * `fetched_at` travels with the row so a cached fare can be shown with its age.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Optional

#: A departure at exactly midnight is treated as "no clock" — matches
#: engines.schedule_view.MIDNIGHT. A real 00:00 departure is vanishingly rare and
#: the cost of dropping one is far lower than placing a whole month at midnight.
MIDNIGHT = "00:00"


@dataclass(frozen=True)
class FlightRow:
    source: str
    airline: str
    flight_number: str
    origin: str
    destination: str
    departure_date: Optional[date]
    departure_time: str          # "HH:MM", or "" when the source had no clock
    arrival_time: str
    arrival_date: Optional[date]
    stops: Optional[int]
    via: str
    cabin: str
    gross_bdt: float
    base_bdt: float              # 0.0 when the source hides it
    base_is_real: bool
    seats: Optional[int]
    fetched_at: datetime

    @property
    def has_clock(self) -> bool:
        return bool(self.departure_time) and self.departure_time != MIDNIGHT

    @property
    def arrives_next_day(self) -> bool:
        """Overnight hop — the UI must show '+1d' or the duration reads negative."""
        return bool(self.arrival_date and self.departure_date
                    and self.arrival_date > self.departure_date)

    def as_schedule_row(self) -> dict[str, Any]:
        """The mapping engines.schedule_view.build() expects."""
        return {
            "airline": self.airline,
            "flight_number": self.flight_number,
            "departure_date": self.departure_date,
            "departure_time": self.departure_time,
            "arrival_time": self.arrival_time,
            "origin": self.origin,
            "destination": self.destination,
            "source": self.source,
            "stops": self.stops,
            "seats": self.seats,
            "via": self.via,
        }


def _split_dt(value: Any) -> tuple[Optional[date], str]:
    """'2026-10-07T17:20:00' -> (date(2026,10,7), '17:20'). Date-only -> (date, '').

    Returns ("" clock) rather than guessing whenever the time is absent, so a
    source that only knows the day can still contribute fares without inventing
    a schedule.
    """
    if value in (None, ""):
        return None, ""
    if isinstance(value, datetime):
        return value.date(), value.strftime("%H:%M")
    if isinstance(value, date):
        return value, ""
    text = str(value).strip().replace(" ", "T", 1)
    head, _, tail = text.partition("T")
    try:
        d = date.fromisoformat(head[:10])
    except ValueError:
        return None, ""
    clock = tail[:5] if len(tail) >= 4 and ":" in tail[:5] else ""
    return d, clock


def from_connector(raw: dict[str, Any], *, source: str, cabin: str,
                   fetched_at: Optional[datetime] = None) -> Optional[FlightRow]:
    """Adapt one connector/HAR row (the repo-wide offer shape) to FlightRow.

    Returns None when the row carries no airline or no price — there is nothing
    a schedule or a fare view could do with it.
    """
    airline = str(raw.get("airline") or "").upper().strip()
    gross = float(raw.get("price_total_bdt") or raw.get("gross_total_bdt") or 0)
    if not airline or gross <= 0:
        return None

    dep_d, dep_t = _split_dt(raw.get("departure"))
    arr_d, arr_t = _split_dt(raw.get("arrival"))
    base = float(raw.get("fare_amount") or raw.get("base_fare_bdt") or 0)
    stops = raw.get("stops")

    return FlightRow(
        source=source,
        airline=airline,
        flight_number=str(raw.get("flight_number") or "").strip(),
        origin=str(raw.get("origin") or "").upper().strip(),
        destination=str(raw.get("destination") or "").upper().strip(),
        departure_date=dep_d,
        departure_time="" if dep_t == MIDNIGHT else dep_t,
        arrival_time=arr_t,
        arrival_date=arr_d,
        stops=int(stops) if stops is not None else None,
        via=str(raw.get("via_airports") or raw.get("via") or ""),
        cabin=cabin,
        gross_bdt=gross,
        base_bdt=base,
        base_is_real=base > 0,
        seats=raw.get("seat_available"),
        fetched_at=fetched_at or datetime.now(),
    )
