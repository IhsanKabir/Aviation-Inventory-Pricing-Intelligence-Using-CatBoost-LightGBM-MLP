from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List


class ChangeDomain(str, Enum):
    PRICE = "price"
    SEAT = "seat"
    AVAILABILITY = "availability"
    CAPACITY = "capacity"
    SCHEDULE = "schedule"
    FIELD = "field"


class ChangeType(str, Enum):
    INCREASE = "increase"
    DECREASE = "decrease"
    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"
    STABLE = "stable"


class ChangeDirection(str, Enum):
    UP = "up"
    DOWN = "down"
    NONE = "none"


class ChangeVelocity(str, Enum):
    SLOW = "slow"
    NORMAL = "normal"
    FAST = "fast"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ChangeEvent:
    domain: ChangeDomain
    change_type: ChangeType
    direction: ChangeDirection
    velocity: ChangeVelocity

    magnitude: float | None
    percent_change: float | None

    airline: str
    flight_number: str
    origin: str
    destination: str
    cabin: str
    departure: datetime

    from_timestamp: datetime | None
    to_timestamp: datetime

    metadata: dict


class ComparisonEngine:
    """
    Compares snapshots and emits:
    - summary/domain events for strategy logic
    - detailed field-level changes for analytics table
    """

    def _value(self, o: Any, key: str):
        if isinstance(o, dict):
            return o.get(key)
        return getattr(o, key, None)

    def _as_row(self, o: Any) -> Dict[str, Any]:
        if isinstance(o, dict):
            return dict(o)
        if hasattr(o, "__dict__"):
            return {k: v for k, v in vars(o).items() if not k.startswith("_")}
        return {}

    def _to_datetime(self, value):
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None
        return None

    def _to_number(self, value):
        if value is None:
            return None
        try:
            return float(value)
        except Exception:
            return None

    def _json_safe(self, value):
        if isinstance(value, datetime):
            return value.isoformat()
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception:
                return str(value)
        if isinstance(value, (int, float, str, bool)) or value is None:
            return value
        try:
            return float(value)
        except Exception:
            return str(value)

    def _key(self, o):
        return (
            self._value(o, "airline"),
            self._value(o, "origin"),
            self._value(o, "destination"),
            self._value(o, "departure"),
            self._value(o, "flight_number"),
            self._value(o, "cabin"),
            self._value(o, "fare_basis"),
            self._value(o, "brand"),
        )

    def _index(self, offers):
        iterable = offers.values() if isinstance(offers, dict) else (offers or [])
        out = {}
        for o in iterable:
            row = self._as_row(o)
            out[self._key(row)] = row
        return out

    def compare(self, previous, current) -> List[ChangeEvent]:
        prev = self._index(previous)
        curr = self._index(current)

        events: List[ChangeEvent] = []
        events += self._availability(prev, curr)
        events += self._price(prev, curr)
        events += self._seats(prev, curr)
        events += self._capacity(prev, curr)
        events += self._schedule(prev, curr)
        return events

    def _availability(self, prev, curr):
        events = []

        prev_keys = set(prev.keys())
        curr_keys = set(curr.keys())

        for k in prev_keys - curr_keys:
            o = prev[k]
            events.append(
                self._event(
                    ChangeDomain.AVAILABILITY,
                    ChangeType.REMOVED,
                    ChangeDirection.DOWN,
                    o,
                    from_ts=o.get("scraped_at"),
                    meta={"reason": "disappeared"},
                )
            )

        for k in curr_keys - prev_keys:
            o = curr[k]
            events.append(
                self._event(
                    ChangeDomain.AVAILABILITY,
                    ChangeType.ADDED,
                    ChangeDirection.UP,
                    o,
                    from_ts=None,
                    meta={"reason": "newly_available"},
                )
            )

        return events

    def _price(self, prev, curr):
        events = []

        for k in prev.keys() & curr.keys():
            p = prev[k]
            c = curr[k]

            prev_price = self._to_number(p.get("price_total_bdt"))
            curr_price = self._to_number(c.get("price_total_bdt"))

            if prev_price is None or curr_price is None or prev_price == curr_price:
                continue

            delta = curr_price - prev_price
            pct = (delta / prev_price) * 100 if prev_price else None

            events.append(
                self._event(
                    ChangeDomain.PRICE,
                    ChangeType.INCREASE if delta > 0 else ChangeType.DECREASE,
                    ChangeDirection.UP if delta > 0 else ChangeDirection.DOWN,
                    c,
                    from_ts=p.get("scraped_at"),
                    magnitude=abs(delta),
                    percent=pct,
                    meta={"before": prev_price, "after": curr_price},
                )
            )

        return events

    def _seats(self, prev, curr):
        events = []

        for k in prev.keys() & curr.keys():
            p = prev[k]
            c = curr[k]

            prev_seats = self._to_number(p.get("seat_available"))
            curr_seats = self._to_number(c.get("seat_available"))

            if prev_seats is None or curr_seats is None or prev_seats == curr_seats:
                continue

            delta = curr_seats - prev_seats

            events.append(
                self._event(
                    ChangeDomain.SEAT,
                    ChangeType.INCREASE if delta > 0 else ChangeType.DECREASE,
                    ChangeDirection.UP if delta > 0 else ChangeDirection.DOWN,
                    c,
                    from_ts=p.get("scraped_at"),
                    magnitude=abs(delta),
                    meta={"before": prev_seats, "after": curr_seats},
                )
            )

        return events

    def _capacity(self, prev, curr):
        events = []

        for k in prev.keys() & curr.keys():
            p = prev[k]
            c = curr[k]

            prev_cap = self._to_number(p.get("seat_capacity"))
            curr_cap = self._to_number(c.get("seat_capacity"))

            if prev_cap is None or curr_cap is None or prev_cap == curr_cap:
                continue

            delta = curr_cap - prev_cap

            events.append(
                self._event(
                    ChangeDomain.CAPACITY,
                    ChangeType.INCREASE if delta > 0 else ChangeType.DECREASE,
                    ChangeDirection.UP if delta > 0 else ChangeDirection.DOWN,
                    c,
                    from_ts=p.get("scraped_at"),
                    magnitude=abs(delta),
                    meta={"aircraft_before": p.get("aircraft"), "aircraft_after": c.get("aircraft")},
                )
            )

        return events

    def _schedule(self, prev, curr):
        events = []

        for k in prev.keys() & curr.keys():
            p = prev[k]
            c = curr[k]

            if p.get("departure") == c.get("departure") and p.get("arrival") == c.get("arrival"):
                continue

            events.append(
                self._event(
                    ChangeDomain.SCHEDULE,
                    ChangeType.MODIFIED,
                    ChangeDirection.NONE,
                    c,
                    from_ts=p.get("scraped_at"),
                    meta={
                        "departure_before": self._json_safe(p.get("departure")),
                        "departure_after": self._json_safe(c.get("departure")),
                        "arrival_before": self._json_safe(p.get("arrival")),
                        "arrival_after": self._json_safe(c.get("arrival")),
                    },
                )
            )

        return events

    def _event(
        self,
        domain: ChangeDomain,
        change_type: ChangeType,
        direction: ChangeDirection,
        o: dict,
        *,
        from_ts: datetime | None,
        magnitude: float | None = None,
        percent: float | None = None,
        meta: dict | None = None,
    ):
        dep_dt = self._to_datetime(o.get("departure"))
        if dep_dt is None:
            dep_dt = self._to_datetime(o.get("scraped_at")) or datetime.utcnow()
        return ChangeEvent(
            domain=domain,
            change_type=change_type,
            direction=direction,
            velocity=ChangeVelocity.UNKNOWN,
            magnitude=magnitude,
            percent_change=percent,
            airline=o.get("airline"),
            flight_number=o.get("flight_number"),
            origin=o.get("origin"),
            destination=o.get("destination"),
            cabin=o.get("cabin"),
            departure=dep_dt,
            from_timestamp=from_ts,
            to_timestamp=o.get("scraped_at") or datetime.utcnow(),
            metadata=meta or {},
        )

    def _domain_for_field(self, field_name: str) -> str:
        if field_name == "price_total_bdt":
            return ChangeDomain.PRICE.value
        if field_name == "seat_available":
            return ChangeDomain.SEAT.value
        if field_name == "seat_capacity":
            return ChangeDomain.CAPACITY.value
        if field_name in ("departure", "arrival"):
            return ChangeDomain.SCHEDULE.value
        return ChangeDomain.FIELD.value

    def _direction_for_values(self, old, new) -> str:
        old_num = self._to_number(old)
        new_num = self._to_number(new)
        if old_num is None or new_num is None:
            return ChangeDirection.NONE.value
        if new_num > old_num:
            return ChangeDirection.UP.value
        if new_num < old_num:
            return ChangeDirection.DOWN.value
        return ChangeDirection.NONE.value

    def _change_type_for_values(self, old, new) -> str:
        if old is None and new is not None:
            return ChangeType.ADDED.value
        if old is not None and new is None:
            return ChangeType.REMOVED.value

        old_num = self._to_number(old)
        new_num = self._to_number(new)
        if old_num is not None and new_num is not None:
            if new_num > old_num:
                return ChangeType.INCREASE.value
            if new_num < old_num:
                return ChangeType.DECREASE.value
            return ChangeType.STABLE.value

        return ChangeType.MODIFIED.value

    def _magnitude_percent(self, old, new):
        old_num = self._to_number(old)
        new_num = self._to_number(new)
        if old_num is None or new_num is None:
            return None, None
        delta = new_num - old_num
        magnitude = abs(delta)
        percent = (delta / old_num) * 100 if old_num else None
        return magnitude, percent

    def compare_column_changes(self, previous, current) -> List[Dict[str, Any]]:
        """
        Returns rows aligned to airline_intel.column_change_events table.
        """
        prev = self._index(previous)
        curr = self._index(current)
        result: List[Dict[str, Any]] = []

        prev_keys = set(prev.keys())
        curr_keys = set(curr.keys())

        ignored = {"id", "_sa_instance_state"}

        def split_departure(row):
            dep = self._to_datetime(row.get("departure"))
            if dep is None:
                return None, None
            return dep.date(), dep.time().replace(microsecond=0)

        for key in sorted(prev_keys - curr_keys):
            p = prev[key]
            dep_day, dep_time = split_departure(p)
            result.append(
                {
                    "scrape_id": None,
                    "previous_scrape_id": p.get("scrape_id"),
                    "airline": p.get("airline"),
                    "departure_day": dep_day,
                    "departure_time": dep_time,
                    "origin": p.get("origin"),
                    "destination": p.get("destination"),
                    "flight_number": p.get("flight_number"),
                    "fare_basis": p.get("fare_basis"),
                    "brand": p.get("brand"),
                    "cabin": p.get("cabin"),
                    "domain": ChangeDomain.AVAILABILITY.value,
                    "change_type": ChangeType.REMOVED.value,
                    "direction": ChangeDirection.DOWN.value,
                    "field_name": "__row_presence__",
                    "old_value": {"present": True},
                    "new_value": {"present": False},
                    "magnitude": None,
                    "percent_change": None,
                    "event_meta": {"identity_key": [self._json_safe(x) for x in key]},
                }
            )

        for key in sorted(curr_keys - prev_keys):
            c = curr[key]
            dep_day, dep_time = split_departure(c)
            result.append(
                {
                    "scrape_id": c.get("scrape_id"),
                    "previous_scrape_id": None,
                    "airline": c.get("airline"),
                    "departure_day": dep_day,
                    "departure_time": dep_time,
                    "origin": c.get("origin"),
                    "destination": c.get("destination"),
                    "flight_number": c.get("flight_number"),
                    "fare_basis": c.get("fare_basis"),
                    "brand": c.get("brand"),
                    "cabin": c.get("cabin"),
                    "domain": ChangeDomain.AVAILABILITY.value,
                    "change_type": ChangeType.ADDED.value,
                    "direction": ChangeDirection.UP.value,
                    "field_name": "__row_presence__",
                    "old_value": {"present": False},
                    "new_value": {"present": True},
                    "magnitude": None,
                    "percent_change": None,
                    "event_meta": {"identity_key": [self._json_safe(x) for x in key]},
                }
            )

        for key in sorted(prev_keys & curr_keys):
            p = prev[key]
            c = curr[key]

            dep_day, dep_time = split_departure(c)

            fields = sorted((set(p.keys()) | set(c.keys())) - ignored)
            for field_name in fields:
                old = p.get(field_name)
                new = c.get(field_name)

                if old == new:
                    continue

                magnitude, pct = self._magnitude_percent(old, new)

                result.append(
                    {
                        "scrape_id": c.get("scrape_id"),
                        "previous_scrape_id": p.get("scrape_id"),
                        "airline": c.get("airline"),
                        "departure_day": dep_day,
                        "departure_time": dep_time,
                        "origin": c.get("origin"),
                        "destination": c.get("destination"),
                        "flight_number": c.get("flight_number"),
                        "fare_basis": c.get("fare_basis"),
                        "brand": c.get("brand"),
                        "cabin": c.get("cabin"),
                        "domain": self._domain_for_field(field_name),
                        "change_type": self._change_type_for_values(old, new),
                        "direction": self._direction_for_values(old, new),
                        "field_name": field_name,
                        "old_value": self._json_safe(old),
                        "new_value": self._json_safe(new),
                        "magnitude": magnitude,
                        "percent_change": pct,
                        "event_meta": {"identity_key": [self._json_safe(x) for x in key]},
                    }
                )

        return result
