"""A route schedule over a date range, from sources the operator chose.

What this answers: "for these routes, over these dates, from these sources,
what does the schedule look like?" -- which nothing currently answers.
`get_route_date_availability` keys off a `cycle_id`, and date ranges exist only
for `report_day` and `detected_at`, never for `departure`.

Four rules are enforced here because each one, left alone, produces a schedule
that reads as authoritative and is wrong:

* A row with no real departure clock is NOT a flight at midnight. Those rows
  are excluded and counted, because a channel served BS and 2A for five months
  with a synthetic `T00:00:00` and any schedule built from them would have
  placed every flight at the start of the day.
* Sources that disagree on a departure time are REPORTED, not silently
  resolved. "First non-empty wins" hides the disagreement; 1,403 collected
  flight-days carry more than one departure time.
* A marketed origin-destination is not an operated leg. An OTA sells DAC->DXB
  for a flight that actually operates DAC-CGP then CGP-DXB, so rows with a
  stop are labelled itineraries and kept out of the leg schedule.
* Flight numbers are joined on a canonical key. The raw column holds
  'BS 109', '101' and 'BS 341,BS 344' at once.

The caller always chooses the sources. There is no implicit "all" -- a report
that silently includes a degraded channel is how the midnight clock reached
five months of data.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from core import field_quality as fq
from core.flight_number import canonical, key as flight_key

#: Rows whose departure sits exactly at midnight are treated as having no
#: clock. A real 00:00 departure is possible but vanishingly rare, and the
#: cost of dropping one is far lower than placing a whole month at midnight.
MIDNIGHT = "00:00"


@dataclass
class ScheduleLeg:
    """One flight on one date over one leg, merged across sources."""

    airline: str
    flight: str                 # canonical number, e.g. '109'
    flight_date: date
    origin: str
    destination: str
    departure: str = ""         # HH:MM local
    arrival: str = ""
    aircraft: str = ""
    seats: int | None = None
    stops: int = 0
    via: str = ""
    #: Operating carrier on a codeshare, when the source distinguishes it.
    operated_by: str = ""
    sources: set = field(default_factory=set)
    #: Every distinct departure time the sources reported for this leg.
    reported_times: set = field(default_factory=set)

    @property
    def key(self) -> str:
        return flight_key(self.airline, self.flight)

    @property
    def route(self) -> str:
        return f"{self.origin}-{self.destination}"

    @property
    def is_direct(self) -> bool:
        return (self.stops or 0) == 0

    @property
    def kind(self) -> str:
        """An operated leg, or a marketed itinerary that hides its legs."""
        return "leg" if self.is_direct else "itinerary"

    @property
    def sources_disagree(self) -> bool:
        return len(self.reported_times) > 1

    @property
    def confidence(self) -> str:
        if self.sources_disagree:
            return "sources disagree"
        if len(self.sources) > 1:
            return "corroborated"
        return "single source"


@dataclass
class ScheduleResult:
    legs: list = field(default_factory=list)
    rows_read: int = 0
    dropped_no_clock: int = 0        # rows excluded for a synthetic clock
    dropped_no_flight: int = 0       # rows with no readable flight number
    itineraries: int = 0             # rows with a stop, kept out of the legs
    sources_used: set = field(default_factory=set)
    sources_requested: tuple = ()
    sources_refused: dict = field(default_factory=dict)   # source -> why
    date_from: date | None = None
    date_to: date | None = None

    @property
    def dates_covered(self) -> int:
        return len({l.flight_date for l in self.legs})

    @property
    def routes(self) -> set:
        return {l.route for l in self.legs}

    @property
    def flights(self) -> int:
        return len({(l.key, l.flight_date, l.route) for l in self.legs})

    @property
    def disagreements(self) -> list:
        return [l for l in self.legs if l.sources_disagree]

    @property
    def missing_dates(self) -> list:
        """Dates in the requested range with no legs at all.

        A gap is a fact about collection, not an empty day of flying, so it is
        listed rather than left for the reader to notice.
        """
        if not (self.date_from and self.date_to):
            return []
        got = {l.flight_date for l in self.legs}
        out, d = [], self.date_from
        while d <= self.date_to:
            if d not in got:
                out.append(d)
            d = date.fromordinal(d.toordinal() + 1)
        return out

    def summary(self) -> str:
        return (f"{len(self.legs):,} leg(s) · {self.flights:,} flight-days · "
                f"{len(self.routes)} route(s) · {self.dates_covered} date(s) "
                f"from {len(self.sources_used)} source(s)")


def choose_sources(quality: fq.QualityReport, requested=None,
                   *, purpose: str = "schedule") -> tuple:
    """Resolve the operator's choice against what each source can support.

    Returns (accepted, refused) where refused maps source -> reason. Passing
    `requested=None` accepts every source that can support `purpose` -- it
    never accepts a source that cannot, so "all" can't quietly readmit a
    degraded channel.
    """
    grade = (lambda s: s.schedule_grade) if purpose == "schedule" \
        else (lambda s: s.fare_grade)
    wanted = ([s for s in quality.sources if s.source in set(requested)]
              if requested is not None else list(quality.sources))
    accepted, refused = [], {}
    for s in wanted:
        if grade(s):
            accepted.append(s.source)
        else:
            refused[s.source] = s.verdict
    if requested is not None:
        for name in requested:
            if quality.by_name(name) is None:
                refused[name] = "not collected"
    return tuple(accepted), refused


def build(rows, *, sources_requested=None, sources_refused=None,
          date_from: date | None = None, date_to: date | None = None,
          include_itineraries: bool = False) -> ScheduleResult:
    """Merge offer rows into a schedule.

    `rows` is an iterable of mappings: airline, flight_number, departure_date,
    departure_time, arrival_time, origin, destination, source, and optionally
    aircraft, seats, stops, via.
    """
    res = ScheduleResult(date_from=date_from, date_to=date_to,
                         sources_requested=tuple(sources_requested or ()),
                         sources_refused=dict(sources_refused or {}))
    merged: dict = {}
    for r in rows:
        res.rows_read += 1
        airline = str(r.get("airline") or "").upper().strip()
        flight = canonical(r.get("flight_number"))
        if not airline or not flight:
            res.dropped_no_flight += 1
            continue
        dep = str(r.get("departure_time") or "").strip()
        # A synthetic midnight is not a departure time. Dropping the row is
        # the only honest option: there is nothing to put in the column.
        if not dep or dep == MIDNIGHT:
            res.dropped_no_clock += 1
            continue
        d = r.get("departure_date")
        if not isinstance(d, date):
            res.dropped_no_flight += 1
            continue
        stops = int(r.get("stops") or 0)
        if stops and not include_itineraries:
            res.itineraries += 1
            continue

        src = str(r.get("source") or "(unnamed)")
        res.sources_used.add(src)
        o = str(r.get("origin") or "").upper().strip()
        dst = str(r.get("destination") or "").upper().strip()
        k = (airline, flight, d, o, dst)
        leg = merged.get(k)
        if leg is None:
            leg = merged[k] = ScheduleLeg(
                airline=airline, flight=flight, flight_date=d,
                origin=o, destination=dst, departure=dep,
                arrival=str(r.get("arrival_time") or "").strip(),
                aircraft=str(r.get("aircraft") or "").strip(),
                seats=r.get("seats"), stops=stops,
                via=str(r.get("via") or "").strip(),
                operated_by=str(r.get("operated_by") or "").strip())
        leg.sources.add(src)
        leg.reported_times.add(dep)
        # Keep the first time seen as the headline, but never overwrite it
        # silently -- `reported_times` carries the disagreement.
        if leg.seats is None and r.get("seats") is not None:
            leg.seats = r.get("seats")
        if not leg.aircraft:
            leg.aircraft = str(r.get("aircraft") or "").strip()
        if not leg.arrival:
            leg.arrival = str(r.get("arrival_time") or "").strip()

    res.legs = sorted(merged.values(),
                      key=lambda l: (l.flight_date, l.departure, l.airline,
                                     l.flight, l.route))
    return res


# ---------------------------------------------------------------------------
# SQL helpers -- the only part that needs a live connection
# ---------------------------------------------------------------------------

_QUALITY_SQL = """
select m.source_endpoint as source,
       count(*) as rows,
       sum(case when to_char(o.departure,'HH24:MI') <> '00:00'
                then 1 else 0 end) as with_clock,
       sum(case when m.fare_amount is not null then 1 else 0 end) as with_base,
       sum(case when m.arrival is not null then 1 else 0 end) as with_arrival,
       sum(case when o.seat_available is not null then 1 else 0 end) as with_seats,
       max(date(o.scraped_at)) as last_scraped,
       max(date(o.departure)) as last_departure
from flight_offers o
join flight_offer_raw_meta m on m.flight_offer_id = o.id
group by 1
"""

_OFFERS_SQL = """
select o.airline,
       o.flight_number,
       date(o.departure) as departure_date,
       to_char(o.departure,'HH24:MI') as departure_time,
       to_char(m.arrival,'HH24:MI') as arrival_time,
       o.origin, o.destination,
       m.source_endpoint as source,
       m.aircraft, o.seat_available as seats,
       m.stops, m.via_airports as via
from flight_offers o
join flight_offer_raw_meta m on m.flight_offer_id = o.id
where date(o.departure) between :date_from and :date_to
  and m.source_endpoint = any(:sources)
"""


def fetch_quality(conn) -> fq.QualityReport:
    """Per-source field quality, straight from a GROUP BY."""
    from sqlalchemy import text
    rows = [dict(r._mapping) for r in conn.execute(text(_QUALITY_SQL))]
    return fq.judge(rows)


def fetch_offers(conn, *, date_from: date, date_to: date, sources,
                 airlines=None, routes=None) -> list:
    """Offer rows for the chosen sources and window.

    `routes` are 'DAC-CGP' strings. Filtering happens in SQL so a wide range
    never loads the whole table.
    """
    from sqlalchemy import text
    sql = _OFFERS_SQL
    params = {"date_from": date_from, "date_to": date_to,
              "sources": list(sources)}
    if airlines:
        sql += " and o.airline = any(:airlines)"
        params["airlines"] = [a.upper() for a in airlines]
    if routes:
        pairs = [tuple(r.upper().split("-", 1)) for r in routes if "-" in r]
        if pairs:
            sql += " and (o.origin, o.destination) in :pairs"
            params["pairs"] = tuple(pairs)
    stmt = text(sql)
    if routes and params.get("pairs"):
        stmt = stmt.bindparams(__import__("sqlalchemy").bindparam(
            "pairs", expanding=True))
    return [dict(r._mapping) for r in conn.execute(stmt, params)]


def run(conn, *, date_from: date, date_to: date, sources=None,
        airlines=None, routes=None,
        include_itineraries: bool = False) -> tuple:
    """The whole thing: judge sources, resolve the choice, build the schedule.

    Returns (result, quality) so a caller can show WHY a source was refused.
    """
    quality = fetch_quality(conn)
    accepted, refused = choose_sources(quality, sources, purpose="schedule")
    if not accepted:
        return (ScheduleResult(date_from=date_from, date_to=date_to,
                               sources_requested=tuple(sources or ()),
                               sources_refused=refused), quality)
    rows = fetch_offers(conn, date_from=date_from, date_to=date_to,
                        sources=accepted, airlines=airlines, routes=routes)
    res = build(rows, sources_requested=sources or accepted,
                sources_refused=refused, date_from=date_from, date_to=date_to,
                include_itineraries=include_itineraries)
    return res, quality
