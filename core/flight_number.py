"""One canonical flight number, whatever a channel called it.

The same column currently holds three shapes, because each channel names a
flight differently and one of them is a fallback:

    'BS 109'          BDFare — carrier prefix, space separated
    '101'             GoZayaan — bare digits, from the hash_str hint
    'BS 341,BS 344'   two flights joined into one cell

Joining on the raw value silently matches nothing. That is not hypothetical:
comparing a month of collected offers against the airline's own schedule
returned 0 matches until this normalisation existed, which read as "the OTA
data is worthless" rather than "the key is wrong".

`canonical` returns ONE number and is what storage and reports should use.
`numbers` returns every number in the cell, for the rows that pack more than
one — so a multi-flight cell is expanded rather than dropped or half-read.
"""
from __future__ import annotations

import re

#: A flight number is 1-4 digits with an optional single-letter suffix
#: ('157', '341A'). The carrier prefix is matched separately so 'BS 109' and
#: '109' collapse to the same key.
_NUM = re.compile(r"(?<![A-Z0-9])(?P<num>\d{1,4})(?P<suffix>[A-Z])?(?![0-9])")

#: Two-or-three character IATA/ICAO-ish carrier code.
_CARRIER = re.compile(r"^(?P<code>[A-Z]{1,3}[0-9]?)\s*[- ]?\s*(?=\d)")


def numbers(value) -> list[str]:
    """Every flight number in the cell, in the order written.

    'BS 341,BS 344' -> ['341', '344'];  'BS 109' -> ['109'];  '' -> [].
    Deduplicated, because 'BS109 / BS 109' is one flight written twice.
    """
    text = str(value or "").upper()
    out: list[str] = []
    for m in _NUM.finditer(text):
        num = m.group("num").lstrip("0") or m.group("num")
        token = num + (m.group("suffix") or "")
        if token not in out:
            out.append(token)
    return out


def canonical(value) -> str:
    """The single flight number for this cell, or '' when there is none.

    A cell holding several numbers yields the FIRST, because that is the
    flight the row's own price and times belong to. Callers that need the
    others must ask for `numbers` explicitly rather than get them by accident.
    """
    got = numbers(value)
    return got[0] if got else ""


def carrier(value) -> str:
    """The carrier code written in the cell, or '' when it is bare.

    GoZayaan's fallback path writes bare digits, so absence here is normal and
    means "the cell did not say", never "no carrier".
    """
    m = _CARRIER.match(str(value or "").upper().strip())
    return m.group("code") if m else ""


def key(airline, value) -> str:
    """The join key: airline + canonical number, e.g. ('BS','BS 109') -> 'BS109'.

    The airline is taken from the row's own airline column rather than from
    the flight cell, because the cell is bare on every GoZayaan row and a
    number alone is not unique across carriers -- '101' is a real flight for
    more than one airline.
    """
    code = str(airline or "").upper().strip()
    num = canonical(value)
    return f"{code}{num}" if code and num else ""


def same_flight(airline_a, value_a, airline_b, value_b) -> bool:
    """Do two cells name the same flight, allowing for the three shapes?

    True when the airlines agree and the number sets overlap, so
    ('BS','BS 341,BS 344') matches ('BS','341').
    """
    a, b = str(airline_a or "").upper(), str(airline_b or "").upper()
    if not a or a != b:
        return False
    sa, sb = set(numbers(value_a)), set(numbers(value_b))
    return bool(sa and sb and (sa & sb))
