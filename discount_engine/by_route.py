"""Per-route discount grids: which OTA gives which discount, route by route.

The summary grid collapses every capture into DOMESTIC / INTERNATIONAL. This view
keeps the route. It adds no discount logic of its own: each channel's EXISTING cell
rule (highest commission, cheapest offer, judged coupons, fee notes...) is run on
ONE route's rows at a time. For a single route the market is fixed, so a channel's
(market, airline) cell over that route's rows IS its per-route cell. The summary
grid is built separately and is untouched by this module.

Rows come from the parses build_report already did (memoized per run), so no HAR
is read twice. Blank means "not captured on this route", never an inferred value.
"""
from __future__ import annotations

from typing import Any, Callable, Optional

from . import grid as g

Route = tuple[str, str]
# {route: {row_label: {airline: cell_text}}}
RouteTable = dict[Route, dict[str, dict[str, str]]]


def _group(rows: list[dict[str, Any]]) -> dict[Route, list[dict[str, Any]]]:
    """Rows by (origin, destination); rows without a route are left out."""
    out: dict[Route, list[dict[str, Any]]] = {}
    for r in rows:
        o = str(r.get("origin") or "").upper()
        d = str(r.get("destination") or "").upper()
        if o and d:
            out.setdefault((o, d), []).append(r)
    return out


def _put(table: RouteTable, label: str, route: Route,
         cells: dict[tuple[str, str], str]) -> None:
    """Store one route's (market, airline) cells under table[route][label]."""
    for (_market, airline), text in cells.items():
        if text:
            table.setdefault(route, {}).setdefault(label, {}).setdefault(airline, text)


def _rows_from(parser: Callable, paths: Optional[list[str]], **kw) -> list[dict[str, Any]]:
    """Every file's parsed rows (memoized); a file that failed to parse is skipped —
    the summary pass has already reported it in the run log."""
    rows: list[dict[str, Any]] = []
    for h in paths or []:
        try:
            rows += g._parsed(parser, h, **kw)
        except Exception:  # noqa: BLE001
            continue
    return rows


def _akij(table: RouteTable, hars, true_base) -> None:
    rows = _rows_from(g.akijair_har.parse_commissions, hars)
    for route, rr in _group(rows).items():
        _put(table, "AKIJ AIR-B2B", route, g._akij_cells(rr, "realized_discount_pct", true_base))


def _bdfare(table: RouteTable, hars, true_base, base_index) -> None:
    rows = _rows_from(g.bdfare_har.parse_commissions, hars, base_index=base_index)
    for route, rr in _group(rows).items():
        _put(table, "BDFare", route, g._bdfare_cells(rr, true_base, verbose=False))


def _firsttrip_b2b(table: RouteTable, rows_per_har) -> None:
    rows = [r for rows in rows_per_har or [] for r in rows]
    for route, rr in _group(rows).items():
        _put(table, "USBA OTA B2B", route, g._collect_firsttrip_b2b_rows(rr))


def _amy(table: RouteTable, rows) -> None:
    for route, rr in _group(rows or []).items():
        _put(table, "Amy", route, g._collect_amy_rows(rr))


def _firsttrip_b2c(table: RouteTable, rows_by_route, fees: dict[str, Any]) -> None:
    by_od: dict[Route, dict] = {}
    for (o, d, when), rows in (rows_by_route or {}).items():
        by_od.setdefault((o.upper(), d.upper()), {})[(o, d, when)] = rows
    for route, subset in by_od.items():
        _put(table, "Firsttrip-B2C", route,
             g._collect_firsttrip_b2c_rows(subset, fees.get("common"), fees.get("card")))


def _sharetrip(table: RouteTable, hars) -> None:
    details: list[dict[str, Any]] = []
    search: list[dict[str, Any]] = []
    for h in hars or []:
        d, s = g._recall("sharetrip_routed", h, ([], []))
        details += d
        search += s
    gateways = g._recall("sharetrip_gateways", "all", {}) or {}
    det_by, search_by = _group(details), _group(search)
    # Coupon TERMS are market-uniform and the live pull fetches each airline's booking
    # once per MARKET, so every route is judged with the whole market's terms at its
    # own fare; the automatic discount comes from that route's own search.
    for route in sorted(set(det_by) | set(search_by)):
        _put(table, "ShareTrip-B2C", route, g._assemble_sharetrip_cells(
            det_by.get(route, []), search_by.get(route, []), gateways, terms_rows=details))


def _gozayaan(table: RouteTable, hars) -> None:
    rows: list[dict[str, Any]] = []
    surcharge: dict[str, float] = {}
    for h in hars or []:
        r, s = g._recall("gozayaan_routed", h, ([], {}))
        rows += r
        surcharge = {**s, **surcharge}
    for route, rr in _group(rows).items():
        _put(table, "Go Zayaan", route, g._gozayaan_cells(rr, surcharge))


def route_table(*, true_base=None, base_index=None,
                akij_hars=None, bdfare_hars=None, ft_b2b_rows_per_har=None,
                sharetrip_hars=None, gozayaan_hars=None, amy_rows=None,
                b2c_rows_by_route=None, b2c_fees=None) -> RouteTable:
    """{route: {row_label: {airline: cell}}} across every channel. Must run inside
    build_report (the parse memo is live). One channel failing only blanks that
    channel's rows in this view; the summary grid is unaffected."""
    table: RouteTable = {}
    steps = [
        ("AKIJ", lambda: _akij(table, akij_hars, true_base)),
        ("BDFare", lambda: _bdfare(table, bdfare_hars, true_base, base_index)),
        ("FT-B2B", lambda: _firsttrip_b2b(table, ft_b2b_rows_per_har)),
        ("ShareTrip", lambda: _sharetrip(table, sharetrip_hars)),
        ("GoZayaan", lambda: _gozayaan(table, gozayaan_hars)),
        ("Amy", lambda: _amy(table, amy_rows)),
        ("FT-B2C", lambda: _firsttrip_b2c(table, b2c_rows_by_route, b2c_fees or {})),
    ]
    for name, step in steps:
        try:
            step()
        except Exception as exc:  # noqa: BLE001 — surfaced in the run log
            print(f"  ! by-route view: {name} skipped: {exc}")
    return table


def route_blocks(table: RouteTable) -> list[dict[str, Any]]:
    """Grid-shaped blocks, one per route: domestic first, then international, A-Z.

    Each block has the SAME shape as report["grids"][market] (columns + rows in the
    report's row order), so highlighting and rendering reuse the summary code.
    `coverage` names the OTAs with data on the route and those without, so a blank
    row reads as "not captured here" rather than a 0% discount.
    """
    blocks = []
    order = sorted(table, key=lambda r: (g._route_type(*r) != "DOM", r))
    labels = [lab for lab, kind in g.ROW_ORDER if kind != "sep"]
    for route in order:
        by_label = table[route]
        market = g._route_type(*route)
        base = g.DOM_COLUMNS if market == "DOM" else g.INTL_COLUMNS
        present = {a for cells in by_label.values() for a in cells}
        cols = [a for a in base if a in present] + sorted(present - set(base))
        rows = []
        for label, kind in g.ROW_ORDER:
            if kind == "sep":
                rows.append({"label": "__sep__", "kind": "sep", "cells": {}})
                continue
            cells = by_label.get(label, {})
            rows.append({"label": label, "kind": kind,
                         "cells": {a: cells.get(a, "") for a in cols}})
        blocks.append({
            "route": f"{route[0]}-{route[1]}",
            "market": market,
            "columns": cols,
            "rows": rows,
            "coverage": {"with_data": [lab for lab in labels if by_label.get(lab)],
                         "without": [lab for lab in labels if not by_label.get(lab)]},
        })
    return blocks


def with_preferred(report: dict[str, Any], preferred: Optional[list[str]]) -> dict[str, Any]:
    """A copy of `report` whose route blocks put the user's preferred routes first,
    in the user's order, flagged `preferred: True`; other routes follow unchanged.
    `preferred_missing` names preferred routes no OTA had data for, so they can be
    shown as "not captured yet" instead of silently vanishing."""
    preferred = [str(r).upper() for r in (preferred or [])]
    blocks = list(report.get("by_route") or [])
    by_name = {b["route"]: b for b in blocks}
    first = [{**by_name[r], "preferred": True} for r in preferred if r in by_name]
    rest = [b for b in blocks if b["route"] not in set(preferred)]
    return {**report, "by_route": first + rest,
            "preferred_missing": [r for r in preferred if r not in by_name]}


def highlighted(blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Best / runner-up flags and a Best row per route block, via the summary's own
    highlighter (no previous-report diff: change tracking stays on the summary)."""
    from .highlight import apply_highlights
    out = []
    for block in blocks:
        colored = apply_highlights({"grids": {"R": block}})["grids"]["R"]
        out.append(colored)
    return out
