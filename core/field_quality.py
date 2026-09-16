"""Whether a source's rows are usable, not merely present.

`extraction_health` already answers "did the source return rows?". It cannot
answer "are the rows any good?", because `classify_attempt` keys off row counts
and error classes only -- `no_inventory` means zero rows. So a source that
returns a full result set with a synthetic field passes every gate.

That is exactly what happened. From April a channel began serving BS and 2A
with `departure` set to midnight, because its segment join stopped resolving
and the row builder fell back to a date with no time. Rows kept arriving, so
nothing failed. The share of rows platform-wide carrying no departure clock
went 0.0% in March to 14.6% in August, unnoticed.

A schedule cannot be built from a flight with no departure time, and a fare
comparison cannot be trusted from a row whose base was never parsed. This
module measures those fields per source and returns a verdict, so the next
silent substitution fails loudly instead of degrading a report.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

#: Below this share of rows carrying a real departure clock, a source cannot
#: support a schedule at all. Set from the observed failure: the degraded
#: channel sat at 0.0%, every healthy one at 100% -- there was no middle.
MIN_CLOCK_SHARE = 0.80

#: Below this share carrying a base fare, fare comparison is unsupported.
MIN_BASE_SHARE = 0.80

#: A source not collected within this many days is stale rather than broken.
STALE_DAYS = 14

OK = "ok"
NO_CLOCK = "no departure clock"
NO_BASE = "no base fare"
STALE = "stale"
EMPTY = "empty"


@dataclass
class SourceQuality:
    """One source, judged on the fields a report actually needs."""

    source: str
    rows: int = 0
    with_clock: int = 0
    with_base: int = 0
    with_arrival: int = 0
    with_seats: int = 0
    airlines: tuple = ()
    last_scraped: date | None = None
    last_departure: date | None = None

    def _share(self, n: int) -> float:
        return (n / self.rows) if self.rows else 0.0

    @property
    def clock_share(self) -> float:
        return self._share(self.with_clock)

    @property
    def base_share(self) -> float:
        return self._share(self.with_base)

    @property
    def age_days(self) -> int | None:
        if self.last_scraped is None:
            return None
        return (date.today() - self.last_scraped).days

    @property
    def blocking(self) -> list:
        """Problems that make this source unusable, worst first.

        Staleness is NOT here. Old rows still describe the days they cover,
        so age is a caveat on the period, not a reason to refuse the data --
        and conflating the two produced a report reading "[OK] NOT usable".
        """
        if not self.rows:
            return [EMPTY]
        out = []
        if self.clock_share < MIN_CLOCK_SHARE:
            out.append(NO_CLOCK)
        if self.base_share < MIN_BASE_SHARE:
            out.append(NO_BASE)
        return out

    @property
    def caveats(self) -> list:
        """True of the source but not disqualifying."""
        age = self.age_days
        return [STALE] if (age is not None and age > STALE_DAYS) else []

    @property
    def problems(self) -> list:
        """Everything worth saying about this source, blocking first."""
        return self.blocking + self.caveats

    @property
    def verdict(self) -> str:
        got = self.blocking
        return got[0] if got else OK

    @property
    def schedule_grade(self) -> bool:
        """Can a schedule be built from this source?

        Freshness is deliberately NOT part of this: old rows still describe
        the days they cover, and refusing them would hide history. Staleness
        is reported separately so a reader knows the period is not current.
        """
        return bool(self.rows) and self.clock_share >= MIN_CLOCK_SHARE

    @property
    def fare_grade(self) -> bool:
        return bool(self.rows) and self.base_share >= MIN_BASE_SHARE

    def explain(self) -> str:
        """One line a non-technical reader can act on."""
        if not self.rows:
            return f"{self.source}: no rows"
        bits = [f"{self.rows:,} rows",
                f"clock {self.clock_share:.0%}",
                f"base {self.base_share:.0%}"]
        age = self.age_days
        if age is not None:
            bits.append(f"last collected {age}d ago")
        head = ("usable" if self.verdict == OK
                else f"NOT usable — {self.verdict}")
        if self.caveats:
            head += f" [{', '.join(self.caveats)}]"
        return f"{self.source}: {head} ({', '.join(bits)})"


@dataclass
class QualityReport:
    sources: list = field(default_factory=list)
    checked_at: datetime | None = None

    def by_name(self, name: str) -> SourceQuality | None:
        for s in self.sources:
            if s.source == name:
                return s
        return None

    @property
    def schedule_grade(self) -> list:
        return [s for s in self.sources if s.schedule_grade]

    @property
    def fare_grade(self) -> list:
        return [s for s in self.sources if s.fare_grade]

    @property
    def failing(self) -> list:
        """Sources that cannot be used. A stale source is not failing."""
        return [s for s in self.sources if s.blocking]

    @property
    def stale(self) -> list:
        return [s for s in self.sources if s.caveats and not s.blocking]

    @property
    def clock_regression(self) -> list:
        """Sources returning rows with no usable departure time.

        This is the specific failure that ran for five months: rows present,
        every existing gate green, the departure clock synthetic.
        """
        return [s for s in self.sources
                if s.rows and s.clock_share < MIN_CLOCK_SHARE]


def judge(rows) -> QualityReport:
    """Build the report from per-source tallies.

    `rows` is an iterable of mappings with: source, rows, with_clock,
    with_base, and optionally with_arrival, with_seats, airlines,
    last_scraped, last_departure. Taking tallies rather than raw offers keeps
    this callable from a SQL GROUP BY without loading millions of rows.
    """
    out = QualityReport(checked_at=datetime.now())
    for r in rows:
        out.sources.append(SourceQuality(
            source=str(r.get("source") or "(unnamed)"),
            rows=int(r.get("rows") or 0),
            with_clock=int(r.get("with_clock") or 0),
            with_base=int(r.get("with_base") or 0),
            with_arrival=int(r.get("with_arrival") or 0),
            with_seats=int(r.get("with_seats") or 0),
            airlines=tuple(r.get("airlines") or ()),
            last_scraped=r.get("last_scraped"),
            last_departure=r.get("last_departure"),
        ))
    out.sources.sort(key=lambda s: -s.rows)
    return out
