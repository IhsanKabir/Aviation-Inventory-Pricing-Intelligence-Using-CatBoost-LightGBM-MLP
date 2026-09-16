"""Schedule and fare views over live flight sources.

Collection is deliberately separate from interpretation:

    sources  -> what each source can answer, and how fast it may be asked
    rows     -> the one normalized FlightRow every source must emit
    cache    -> reuse, with freshness declared by the caller (a schedule keeps,
                a fare does not)
    collect  -> routes x dates x sources, paced, estimable, cancellable
    schedule -> operated legs and their weekly pattern
    fares    -> what each airline charges on a route
    render   -> the workbook, carrying its own caveats

Nothing here imports pandas or a database: the desktop build excludes them, and
these views run on a teammate's machine from live queries alone.
"""
