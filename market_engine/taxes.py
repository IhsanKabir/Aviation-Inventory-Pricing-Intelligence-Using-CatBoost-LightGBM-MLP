"""Tax view: what each airline's fare carries in taxes, code by code.

Only FirstTrip itemises taxes (passengerFares.adt.taxesBreakdown); every other
source gives a total at best. Choices that change the numbers:

  * FOLD BY THE FIRST TWO CHARACTERS. Suppliers suffix codes (OW2, UT3, YQI,
    YRF) and those are the same tax. Digits are NOT stripped: E5, P7, P8, G4,
    R9, M6, I2, S6 and T6 are real codes. Duplicates after folding are summed
    (PZ + PZ2), and the folded sum must equal the total or the fare says "check".
  * MISSING IS NOT ZERO. An empty breakdown (some FirstTrip suppliers send none)
    is "not captured", and its code cells stay blank.
  * NOT ITEMISED. Some carriers put the whole tax under one code (6E files
    17,316 under BD, which is normally a few hundred). A code above its limit in
    config/tax_codes.json marks the fare "not itemised" instead of reporting a
    17,316 BD tax.
  * GROUP BY ROUTING, NOT AMOUNT. Taxes differ by routing (via DOH adds G4/QA/PZ/
    R9) and Bangladesh taxes are not uniform (UT 4,000/6,000/8,000), so fares are
    grouped by route + airline + via + operator + the set of codes present, and
    each code is shown as its low-high range across the group.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

from discount_engine.grid import DOMESTIC_AIRPORTS
from market_engine.rows import FlightRow

#: Matrix columns, in order; everything else goes under "Other / transit".
FIXED_CODES = ("YQ", "YR", "BD", "UT", "E5", "OW", "P7", "P8", "ZR")
SURCHARGES = ("YQ", "YR")

ITEMISED = "itemised"
NOT_CAPTURED = "not captured"
NOT_ITEMISED = "not itemised"
CHECK = "check"
#: A site that never itemises (ShareTrip, GoZayaan): its total is all there is.
TOTAL_ONLY = "total only"

#: Folded codes may differ from the stated total by rounding only.
SUM_TOLERANCE_BDT = 1.0
#: Two OTAs' tax for the same flight differing by more than this is reported.
OTA_TOLERANCE_BDT = 50.0
OTA_TOLERANCE_PCT = 1.0

#: Bangladeshi carriers first, then the rest A-Z.
AIRLINE_FIRST = ("BS", "BG")


def fold(code: str) -> str:
    return str(code or "").upper().strip()[:2]


def load_config() -> dict:
    """{'names': {code: name}, 'limits': {code: BDT}} from config/tax_codes.json."""
    from market_engine.timetable import _load
    data = _load("tax_codes.json")
    names = {fold(k): str(v) for k, v in (data.get("names") or {}).items() if v}
    limits = {}
    for k, v in (data.get("itemisation_limits") or {}).items():
        try:
            limits[fold(k)] = float(v)
        except (TypeError, ValueError):
            continue
    return {"names": names, "limits": limits}


@dataclass(frozen=True)
class TaxRead:
    codes: tuple             # folded ((code, BDT), ...), sorted by code
    total: float             # the stated total, else the folded sum; 0.0 if unknown
    status: str


def read(row: FlightRow, limits: Optional[dict] = None) -> TaxRead:
    """One fare's taxes, folded and checked."""
    if not row.taxes:
        return TaxRead((), row.tax_total, NOT_CAPTURED)
    sums: dict = {}
    for code, amount in row.taxes:
        key = fold(code)
        sums[key] = sums.get(key, 0.0) + float(amount)
    codes = tuple(sorted((c, round(a, 2)) for c, a in sums.items()))
    folded = sum(a for _, a in codes)
    total = row.tax_total or folded
    if any(a > (limits or {}).get(c, float("inf")) for c, a in codes):
        status = NOT_ITEMISED
    elif row.tax_total and abs(folded - row.tax_total) > SUM_TOLERANCE_BDT:
        status = CHECK
    else:
        status = ITEMISED
    return TaxRead(codes, total, status)


#: A fare whose whole tax sits under one code is "not itemised" when other fares on
#: the same route carry at least this many codes (6E PEN-DAC: all 13,032 under MY,
#: where every other airline itemises MY 1,514-1,938 alongside G1, H8...).
ITEMISED_PEERS_MIN_CODES = 3


def read_all(rows: Iterable[FlightRow], limits: Optional[dict] = None) -> dict:
    """id(row) -> TaxRead, with the route-aware "one code holds it all" check that
    a single fare cannot make on its own."""
    rows = list(rows)
    reads = {id(r): read(r, limits) for r in rows}
    richest: dict = {}
    for r in rows:
        t = reads[id(r)]
        if t.status == ITEMISED:
            key = (r.origin, r.destination)
            richest[key] = max(richest.get(key, 0), len(t.codes))
    for r in rows:
        t = reads[id(r)]
        lone = (t.status == ITEMISED and len(t.codes) == 1
                and t.codes[0][0] not in SURCHARGES)
        if lone and richest.get((r.origin, r.destination), 0) >= ITEMISED_PEERS_MIN_CODES:
            reads[id(r)] = TaxRead(t.codes, t.total, NOT_ITEMISED)
    return reads


def tax_of(row: FlightRow) -> Optional[float]:
    """The fare's tax total: stated, else gross - an observed base; None if unknown."""
    if row.tax_total > 0:
        return row.tax_total
    if row.base_is_real and row.gross_bdt > row.base_bdt > 0:
        return round(row.gross_bdt - row.base_bdt, 2)
    return None


def surcharge(read_: TaxRead) -> Optional[float]:
    """YQ + YR, or None when the fare's codes cannot be trusted."""
    if read_.status != ITEMISED:
        return None
    return sum(a for c, a in read_.codes if c in SURCHARGES)


def is_domestic(origin: str, dest: str) -> bool:
    return origin in DOMESTIC_AIRPORTS and dest in DOMESTIC_AIRPORTS


def _outbound(origin: str, dest: str) -> bool:
    """Leaving Bangladesh (or leaving DAC, domestically) comes first."""
    if is_domestic(origin, dest):
        return origin == "DAC" or (dest != "DAC" and origin < dest)
    return origin in DOMESTIC_AIRPORTS or (dest not in DOMESTIC_AIRPORTS and origin < dest)


def _airline_rank(code: str) -> tuple:
    return (AIRLINE_FIRST.index(code), "") if code in AIRLINE_FIRST else (len(AIRLINE_FIRST), code)


def order_key(origin: str, dest: str, airline: str, via: str = "") -> tuple:
    market = tuple(sorted((origin, dest)))
    return (not is_domestic(origin, dest), market, not _outbound(origin, dest),
            _airline_rank(airline), via)


@dataclass(frozen=True)
class TaxGroup:
    origin: str
    destination: str
    airline: str
    via: str
    operated_by: str
    status: str
    flights: int                 # distinct flight + date
    codes: tuple                 # ((code, low, high, fares_carrying), ...) sorted
    total_low: float
    total_high: float
    surcharge_low: Optional[float]
    surcharge_high: Optional[float]
    share_low: Optional[float]   # YQ+YR as a share of that same fare's tax, 0-1
    share_high: Optional[float]
    rbds: tuple
    flight_numbers: tuple
    sources: tuple = ()
    fares: int = 0               # distinct fares behind the group (a code's 4th field
                                 # is how many of these carry it)

    @property
    def route(self) -> str:
        return f"{self.origin}-{self.destination}"

    @property
    def status_label(self) -> str:
        """'total only (ShareTrip)': which site it was matters for a total-only row."""
        if self.status == TOTAL_ONLY and self.sources:
            return "{} ({})".format(self.status, ", ".join(self.sources))
        return self.status

    @property
    def domestic(self) -> bool:
        return is_domestic(self.origin, self.destination)

    def carried_by(self, code: str) -> int:
        """How many of the group's fares carry `code`."""
        return next((n for c, *_, n in self.codes if c == code), 0)

    def code_range(self, code: str) -> Optional[tuple]:
        for c, lo, hi, _ in self.codes:
            if c == code:
                return lo, hi
        return None

    def other_range(self) -> Optional[tuple]:
        """Summed range of the non-matrix codes, or None when there are none."""
        others = [(lo, hi) for c, lo, hi, _ in self.codes if c not in FIXED_CODES]
        if not others:
            return None
        return sum(lo for lo, _ in others), sum(hi for _, hi in others)

    def other_codes(self) -> tuple:
        return tuple(c for c, *_ in self.codes if c not in FIXED_CODES)


def group(rows: Iterable[FlightRow], limits: Optional[dict] = None) -> list:
    """Fares -> sorted TaxGroups.

    A fare from an itemising source (FirstTrip) with no breakdown joins a "not
    captured" group of its own routing, so the sheet says the codes were not sent
    rather than leaving the airline out. A site that never itemises (ShareTrip)
    is not "not captured": its tax total is shown as "total only", and only for an
    airline + route no itemising source covered - otherwise the codes say more."""
    rows = list(rows)
    reads = read_all(rows, limits)
    itemising = {r.source for r in rows if r.taxes}
    covered = {(r.origin, r.destination, r.airline) for r in rows if r.source in itemising}
    buckets: dict = {}
    seen: set = set()
    for r in rows:
        if r.source in itemising:
            # no codes and no total either: still "not captured", never dropped
            t, src = reads[id(r)], ""
        else:
            total = tax_of(r)
            if total is None or (r.origin, r.destination, r.airline) in covered:
                continue
            t, src = TaxRead((), total, TOTAL_ONLY), r.source
        fingerprint = (r.airline, r.flight_number, r.departure_date, r.departure_time,
                       r.origin, r.destination, t.codes, t.total, src)
        if fingerprint in seen:
            continue                       # the same fare sent twice (another class/page)
        seen.add(fingerprint)
        # Not keyed on the codes present: fares of one routing that differ by a
        # small code (H8 on some MH fares) stay in one column, and the code says
        # how many of the fares carry it.
        key = (r.origin, r.destination, r.airline, r.via, r.operated_by, t.status, src)
        buckets.setdefault(key, []).append((r, t))

    out = []
    for (o, d, airline, via, op, status, _src), items in buckets.items():
        code_set = sorted({c for _, t in items for c, _ in t.codes})
        codes = []
        for code in code_set:
            amounts = [a for _, t in items for c, a in t.codes if c == code]
            codes.append((code, min(amounts), max(amounts), len(amounts)))
        totals = [t.total for _, t in items if t.total > 0]
        surch = [s for s in (surcharge(t) for _, t in items) if s is not None]
        shares = [surcharge(t) / t.total for _, t in items
                  if surcharge(t) is not None and t.total > 0]
        out.append(TaxGroup(
            origin=o, destination=d, airline=airline, via=via, operated_by=op,
            status=status,
            flights=len({(r.flight_number, r.departure_date) for r, _ in items}),
            codes=tuple(codes),
            total_low=min(totals) if totals else 0.0,
            total_high=max(totals) if totals else 0.0,
            surcharge_low=min(surch) if surch else None,
            surcharge_high=max(surch) if surch else None,
            share_low=min(shares) if shares else None,
            share_high=max(shares) if shares else None,
            rbds=tuple(sorted({r.rbd for r, _ in items if r.rbd})),
            flight_numbers=tuple(sorted({r.flight_number for r, _ in items if r.flight_number})),
            sources=tuple(sorted({r.source for r, _ in items})),
            fares=len(items)))
    out.sort(key=lambda g: order_key(g.origin, g.destination, g.airline, g.via)
             + (g.status != ITEMISED, g.operated_by, g.codes, g.sources))
    return out


@dataclass(frozen=True)
class SiteCheck:
    """One flight seen on 2+ sites: each site's cheapest fare and its tax."""
    origin: str
    destination: str
    airline: str
    flight_number: str
    departure_date: object
    departure_time: str
    reference: str               # the site the others are compared against
    grosses: tuple               # ((site, gross), ...), reference first
    taxes: tuple                 # ((site, tax or None), ...), same order
    flagged: tuple               # sites whose tax differs from the reference's
    rbds: tuple = ()             # ((site, booking class), ...) where sent

    @property
    def route(self) -> str:
        return f"{self.origin}-{self.destination}"

    def gross(self, site: str) -> Optional[float]:
        return dict(self.grosses).get(site)

    def tax(self, site: str) -> Optional[float]:
        return dict(self.taxes).get(site)

    @property
    def cheapest(self) -> str:
        return min(self.grosses, key=lambda sg: sg[1])[0]

    def cause(self, site: str) -> str:
        """Why a site's tax differs: a different booking class explains it (YQ
        follows the class); the same class points at the site's own charge."""
        classes = dict(self.rbds)
        mine, ref = classes.get(site), classes.get(self.reference)
        if mine and ref:
            if mine != ref:
                return f"different fare class ({mine} vs {ref})"
            return "same class - OTA charge in the tax"
        return "OTA charge or different fare"


def _differs(a: float, b: float) -> bool:
    return abs(a - b) > max(OTA_TOLERANCE_BDT, abs(b) * OTA_TOLERANCE_PCT / 100)


def ota_check(rows: Iterable[FlightRow], reference: str = "FirstTrip") -> list:
    """The same flight (airline, number, date, departure clock, route) seen on 2+
    sites: each site's cheapest fare and the tax on it, against the reference's.

    The reference is FirstTrip when present (it itemises), else the first site
    that itemises, else the first by name."""
    rows = list(rows)
    flights: dict = {}
    for r in rows:
        if not (r.flight_number and r.departure_date and r.has_clock) or r.gross_bdt <= 0:
            continue                       # without all three it is not an exact match
        key = (r.airline, r.flight_number, r.departure_date, r.departure_time,
               r.origin, r.destination)
        best = flights.setdefault(key, {})
        cur = best.get(r.source)
        if cur is None or r.gross_bdt < cur.gross_bdt:
            best[r.source] = r
    itemising = sorted({r.source for r in rows if r.taxes})

    out = []
    for (airline, number, day, clock, o, d), by_site in flights.items():
        if len(by_site) < 2:
            continue
        ref = next((s for s in [reference] + itemising if s in by_site), sorted(by_site)[0])
        order = [ref] + sorted(s for s in by_site if s != ref)
        taxes = [(s, tax_of(by_site[s])) for s in order]
        ref_tax = taxes[0][1]
        out.append(SiteCheck(
            origin=o, destination=d, airline=airline, flight_number=number,
            departure_date=day, departure_time=clock, reference=ref,
            grosses=tuple((s, by_site[s].gross_bdt) for s in order),
            taxes=tuple(taxes),
            flagged=tuple(s for s, t in taxes[1:]
                          if t is not None and ref_tax is not None and _differs(t, ref_tax)),
            rbds=tuple((s, by_site[s].rbd) for s in order if by_site[s].rbd)))
    out.sort(key=lambda c: order_key(c.origin, c.destination, c.airline)
             + (c.departure_date, c.departure_time, c.flight_number))
    return out


@dataclass(frozen=True)
class CheapestFare:
    """What the Fares / Fare Comparison sheets say about a pair's cheapest fare."""
    tax: Optional[float]
    surcharge: Optional[float]   # YQ+YR; from the same flight's itemised fare if needed
    flight: str                  # "OD 161 01:10"
    day: object
    rbd: str
    baggage: str
    refundable: Optional[bool]
    source: str


def cheapest_fares(rows: Iterable[FlightRow], limits: Optional[dict] = None) -> dict:
    """(route, airline) -> CheapestFare.

    YQ+YR: when the cheapest fare came without codes (a FirstTrip supplier that
    sends none, or ShareTrip), the cheapest ITEMISED fare of the same flight and
    day is used - the same service, so the carrier charge is the one that applies."""
    rows = [r for r in rows if r.gross_bdt > 0]
    reads = read_all(rows, limits)
    best: dict = {}
    itemised: dict = {}
    for r in rows:
        key = (f"{r.origin}-{r.destination}", r.airline)
        if key not in best or r.gross_bdt < best[key].gross_bdt:
            best[key] = r
        if reads[id(r)].status == ITEMISED:
            fk = (key, r.flight_number, r.departure_date)
            if fk not in itemised or r.gross_bdt < itemised[fk].gross_bdt:
                itemised[fk] = r
    out = {}
    for key, r in best.items():
        yq = surcharge(reads[id(r)])
        if yq is None:
            twin = itemised.get((key, r.flight_number, r.departure_date))
            yq = surcharge(reads[id(twin)]) if twin is not None else None
        flight = " ".join(x for x in (r.airline, r.flight_number, r.departure_time) if x)
        out[key] = CheapestFare(tax=tax_of(r), surcharge=yq, flight=flight,
                                day=r.departure_date, rbd=r.rbd, baggage=r.baggage,
                                refundable=r.refundable, source=r.source)
    return out


def cheapest_fare_tax(rows: Iterable[FlightRow], limits: Optional[dict] = None) -> dict:
    """(route, airline) -> (tax, YQ+YR) of that pair's cheapest fare."""
    return {k: (c.tax, c.surcharge) for k, c in cheapest_fares(rows, limits).items()}


#: Upper cabins first, as the GDS fare report lays them out.
CABIN_RANK = {"First": 0, "Business": 1, "Premium Economy": 2, "Premium": 2}


def is_upper(cabin: str) -> bool:
    return CABIN_RANK.get(cabin, 9) < 3


@dataclass(frozen=True)
class ClassLine:
    """One booking class: its cheapest fare, every figure from that same fare."""
    rbd: str
    cabin: str
    fares: int                   # fares seen in this class
    gross: float
    base: Optional[float]        # None when the site hid the base
    surcharge: Optional[float]   # YQ + YR; None when the fare's codes are unknown
    tax: Optional[float]

    @property
    def base_with_yq(self) -> Optional[float]:
        if self.base is None or self.surcharge is None:
            return None
        return self.base + self.surcharge


@dataclass(frozen=True)
class ClassTable:
    origin: str
    destination: str
    airline: str
    via: str
    operated_by: str
    lines: tuple                 # ClassLine: upper cabins first, highest fare first
    codes: tuple = ()            # ((code, low, high, fares carrying it), ...) folded
    fares: int = 0
    sources: tuple = ()
    total_low: Optional[float] = None
    total_high: Optional[float] = None

    @property
    def route(self) -> str:
        return f"{self.origin}-{self.destination}"

    @property
    def split_cabins(self) -> bool:
        return any(is_upper(l.cabin) for l in self.lines) and any(
            not is_upper(l.cabin) for l in self.lines)


def by_class(rows: Iterable[FlightRow], limits: Optional[dict] = None) -> list:
    """Per airline + routing, one line per booking class (the GDS fare report's
    layout): YQ follows the class, so a routing's YQ range (MH 371-7,336) only
    reads once it is split this way; the routing's tax codes come with it.

    From the itemising site(s) when the run has one - a class's taxes are the
    point - else from every site (totals only)."""
    rows = [r for r in rows if r.gross_bdt > 0]
    reads = read_all(rows, limits)
    itemising = {r.source for r in rows if r.taxes}
    use = [r for r in rows if r.source in itemising] if itemising else rows
    tables: dict = {}
    for r in use:
        key = (r.origin, r.destination, r.airline, r.via, r.operated_by)
        tables.setdefault(key, {}).setdefault((r.cabin_class, r.rbd or "?"), []).append(r)
    out = []
    for (o, d, airline, via, op), classes in tables.items():
        lines = []
        for (cabin, rbd), rs in classes.items():
            best = min(rs, key=lambda r: r.gross_bdt)
            lines.append(ClassLine(
                rbd=rbd, cabin=cabin, fares=len(rs), gross=best.gross_bdt,
                base=best.base_bdt if best.base_is_real else None,
                surcharge=surcharge(reads[id(best)]), tax=tax_of(best)))
        lines.sort(key=lambda l: (CABIN_RANK.get(l.cabin, 9), -l.gross))
        every = [r for rs in classes.values() for r in rs]
        codes: dict = {}
        for r in every:
            t = reads[id(r)]
            if t.status == ITEMISED:
                for c, a in t.codes:
                    codes.setdefault(c, []).append(a)
        totals = [t for t in (tax_of(r) for r in every) if t is not None]
        out.append(ClassTable(
            o, d, airline, via, op, tuple(lines),
            codes=tuple((c, min(a), max(a), len(a)) for c, a in sorted(codes.items())),
            fares=len(every), sources=tuple(sorted({r.source for r in every})),
            total_low=min(totals) if totals else None,
            total_high=max(totals) if totals else None))
    out.sort(key=lambda t: order_key(t.origin, t.destination, t.airline, t.via)
             + (t.operated_by,))
    return out
