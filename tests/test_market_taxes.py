"""Fare Comparison tax sheets: folding, honesty about missing data, grouping, OTA check.

Amounts are the real ones from a FirstTrip DAC-DXB / DAC-CXB search capture
(2026-10): BS DAC-DXB nonstop, BG via DOH (adds G4/QA/PZ/R9), 6E filing its whole
tax under BD, QR fares whose supplier sent no breakdown at all.
"""
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from market_engine import cache as C  # noqa: E402
from market_engine import taxes as T  # noqa: E402
from market_engine.rows import FlightRow, from_connector  # noqa: E402
from modules import firsttrip  # noqa: E402

LIMITS = {"BD": 1000.0}
BS_DXB = (("BD", 500), ("E5", 443), ("OW2", 2500), ("P7", 1237), ("P8", 1237),
          ("UT3", 4000), ("YQI", 618), ("ZR", 168))
BG_DOH = (("BD", 500), ("E5", 447), ("G4", 2038), ("OW", 2500), ("P7", 1237),
          ("P8", 1237), ("PZ", 100), ("PZ2", 86), ("QA", 2038), ("R9", 340),
          ("UT", 4000), ("ZR", 169))


def _row(airline="BS", number="341", taxes=BS_DXB, total=10703.0, via="", source="FirstTrip",
         o="DAC", d="DXB", day=date(2026, 10, 31), clock="20:15", gross=36671.0,
         rbd="S") -> FlightRow:
    return FlightRow(source=source, airline=airline, flight_number=number, origin=o,
                     destination=d, departure_date=day, departure_time=clock,
                     arrival_time="23:40", arrival_date=day, stops=1 if via else 0,
                     via=via, aircraft="", operated_by="", cabin="Economy",
                     gross_bdt=gross, base_bdt=gross - total, base_is_real=True,
                     seats=None, fetched_at=datetime(2026, 10, 25, 9, 0),
                     taxes=tuple(taxes), tax_total=total, rbd=rbd)


def test_codes_fold_by_their_first_two_characters_and_keep_digit_codes():
    t = T.read(_row(), LIMITS)
    codes = dict(t.codes)
    assert codes["OW"] == 2500 and codes["UT"] == 4000 and codes["YQ"] == 618
    # E5 / P7 / P8 are real codes, not suffixed ones: stripping digits destroys them
    assert codes["E5"] == 443 and codes["P7"] == 1237 and codes["P8"] == 1237
    assert t.status == T.ITEMISED and t.total == 10703


def test_suffixed_duplicates_are_summed():
    t = T.read(_row("BG", "147", BG_DOH, 14692.0, via="CCU|DOH"), LIMITS)
    assert dict(t.codes)["PZ"] == 186 and t.status == T.ITEMISED


def test_a_missing_breakdown_is_not_captured_never_zero():
    t = T.read(_row("QR", "641", (), 21093.0, via="DOH"), LIMITS)
    assert t.status == T.NOT_CAPTURED and t.codes == () and t.total == 21093


def test_whole_tax_under_one_code_is_not_itemised():
    t = T.read(_row("6E", "1102", (("BD", 17316),), 17316.0, via="BOM"), LIMITS)
    assert t.status == T.NOT_ITEMISED
    assert T.surcharge(t) is None              # its codes say nothing about YQ/YR


def test_codes_that_do_not_add_up_are_flagged():
    t = T.read(_row(total=11000.0), LIMITS)
    assert t.status == T.CHECK


def test_groups_split_by_routing_and_show_ranges():
    rows = [_row(), _row(number="343", taxes=BS_DXB[:-1] + (("ZR", 169),), total=10704.0),
            _row("BG", "147", BG_DOH, 14692.0, via="CCU|DOH"),
            _row("BG", "347", BG_DOH[:-1] + (("ZR", 169),), 14692.0),
            _row("QR", "641", (), 21093.0, via="DOH")]
    groups = T.group(rows, LIMITS)
    bs = [g for g in groups if g.airline == "BS"]
    assert len(bs) == 1 and bs[0].code_range("ZR") == (168, 169) and bs[0].flights == 2
    bg = {g.via for g in groups if g.airline == "BG"}
    assert bg == {"", "CCU|DOH"}               # via DOH carries other taxes: own row
    doh = next(g for g in groups if g.via == "CCU|DOH")
    assert set(doh.other_codes()) == {"G4", "PZ", "QA", "R9"}
    assert doh.share_high == 0                 # via DOH: no YQ/YR on this fare
    # each fare's own YQ over its own total, not the group's highs
    assert (bs[0].share_low, bs[0].share_high) == (618 / 10704, 618 / 10703)
    qr = next(g for g in groups if g.airline == "QR")
    assert qr.status == T.NOT_CAPTURED and qr.total_low == 21093
    # BS, BG, then A-Z
    assert [g.airline for g in groups][:3] == ["BS", "BG", "BG"]


def test_domestic_first_and_outbound_before_inbound():
    rows = [_row(o="DXB", d="DAC"), _row(), _row("BS", "141", (("BD", 25),), 25.0,
                                                o="DAC", d="CXB")]
    routes = [g.route for g in T.group(rows, LIMITS)]
    assert routes == ["DAC-CXB", "DAC-DXB", "DXB-DAC"]


def test_a_source_that_never_itemises_stays_off_the_tax_sheets():
    rows = [_row(), _row(source="ShareTrip", taxes=(), total=10703.0)]
    assert {g.status for g in T.group(rows, LIMITS)} == {T.ITEMISED}


def test_ota_check_matches_the_exact_flight_and_flags_a_difference():
    rows = [_row(), _row(source="ShareTrip", taxes=(), total=10703.0),
            _row(source="GoZayaan", taxes=(), total=12100.0),
            _row(source="ShareTrip", taxes=(), total=9000.0, clock="09:00", number="343")]
    checks = T.ota_check(rows)
    assert len(checks) == 1                    # the 09:00 flight is not on FirstTrip
    c = checks[0]
    assert c.reference == "FirstTrip" and c.tax("ShareTrip") == 10703
    assert c.flagged == ("GoZayaan",)


def test_fares_sheet_tax_comes_from_the_cheapest_fare():
    rows = [_row(gross=40000.0, total=12000.0, taxes=()), _row()]
    tax, yqyr = T.cheapest_fare_tax(rows, LIMITS)[("DAC-DXB", "BS")]
    assert tax == 10703 and yqyr == 618


def test_firsttrip_rows_carry_breakdown_routing_and_rbd():
    offer = {"marketingCarrierCode": "BG", "finalTotalPrice": 40000, "finalBasePrice": 25308,
             "finalTaxPrice": 14692,
             "passengerFares": {"adt": {"taxes": 14692, "taxesBreakdown": [
                 {"category": c, "amount": a} for c, a in BG_DOH]}},
             "directions": [[{"stops": 2, "segments": [
                 {"originAirportCode": "DAC", "destinationAirportCode": "CCU",
                  "departureTime": "2026-10-31T10:00:00", "flightNumber": "147", "rbd": "k"},
                 {"originAirportCode": "CCU", "destinationAirportCode": "DOH"},
                 {"originAirportCode": "DOH", "destinationAirportCode": "DXB",
                  "arrivalTime": "2026-10-31T22:00:00"}]}]]}
    raw = firsttrip._b2c_rows_from_offers([offer])[0]
    row = from_connector(raw, source="FirstTrip B2C (HAR)", cabin="Economy")
    assert row.stops == 2 and row.via == "CCU|DOH" and row.rbd == "K"
    assert row.tax_total == 14692 and ("PZ2", 86.0) in row.taxes


def test_cache_round_trips_taxes_and_reads_old_rows(tmp_path):
    store = C.CacheStore(tmp_path / "c.sqlite")
    store.put("FirstTrip", "DAC", "DXB", "2026-10-31", "Economy", [_row()])
    back = store.get("FirstTrip", "DAC", "DXB", "2026-10-31", "Economy",
                     max_age=C.FRESH_FARE)[0]
    assert back.taxes == _row().taxes and back.rbd == "S"
    old = C._encode([_row()]).replace(',"taxes"', ',"x_taxes"')
    import json
    legacy = [{k: v for k, v in d.items() if k not in ("x_taxes", "tax_total", "rbd")}
              for d in json.loads(old)]
    assert C._decode(json.dumps(legacy))[0].taxes == ()


def test_firsttrip_rows_cached_before_the_tax_fields_are_fetched_again(tmp_path):
    store = C.CacheStore(tmp_path / "c.sqlite")
    store._db.execute("insert into q values (?,?,?)",
                      ("FirstTrip|DAC|DXB|2026-10-31|economy", datetime.now().isoformat(),
                       C._encode([_row()])))
    assert store.get("FirstTrip", "DAC", "DXB", "2026-10-31", "Economy",
                     max_age=C.FRESH_FARE) is None


def test_workbook_gets_the_tax_sheets(tmp_path):
    from openpyxl import load_workbook

    from market_engine.collect import CollectResult
    from market_engine.fares import build_fares
    from market_engine.render import write_workbook
    rows = [_row(), _row("BG", "147", BG_DOH, 14692.0, via="CCU|DOH"),
            _row("QR", "641", (), 21093.0, via="DOH")]
    table = build_fares(CollectResult(rows=rows))
    out = write_workbook(tmp_path / "m.xlsx", table=table, best_by_route={},
                         rows=list(table.rows))
    wb = load_workbook(out)
    # one site: no OTA Check (it could only say "nothing to compare")
    assert wb.sheetnames == ["Fares", "Fare Comparison", "Fares by Class",
                             "Missing - Re-run", "Tax Comparison"]
    values = [v for row in wb["Tax Comparison"].iter_rows(values_only=True) for v in row]
    assert T.NOT_CAPTURED in values            # QR: blank-not-zero, said in words
    fares = [v for row in wb["Fares"].iter_rows(values_only=True) for v in row]
    assert "Tax (cheapest)" in fares and "YQ+YR (cheapest)" in fares


def _sheet_rows(ws):
    return [[v for v in row] for row in ws.iter_rows(values_only=True)]


def test_comparison_tables_sit_side_by_side(tmp_path):
    """One route reads ACROSS: each airline/routing is a column, codes are rows."""
    from openpyxl import load_workbook

    from market_engine.collect import CollectResult
    from market_engine.fares import build_fares
    from market_engine.render import write_workbook
    rows = [_row(), _row("BG", "147", BG_DOH, 14692.0, via="CCU|DOH"),
            _row("EK", "585", (("YQ", 9000), ("BD", 500)), 9500.0)]
    table = build_fares(CollectResult(rows=rows))
    wb = load_workbook(write_workbook(tmp_path / "m.xlsx", table=table, best_by_route={},
                                      rows=list(table.rows)))
    grid = _sheet_rows(wb["Tax Comparison"])
    airlines = next(r for r in grid if r[0] == "Airline")
    assert airlines[1:4] == ["BS", "BG", "EK"]          # BS, BG, then A-Z, across
    yq = next(r for r in grid if r[0] == "YQ")
    assert yq[1:4] == [618, None, 9000]                 # blank: BG via DOH has no YQ
    g4 = next(r for r in grid if r[0] == "G4")          # a transit code gets its own row
    assert g4[1:4] == [None, 2038, None]
    # Fares by Class: the three airline tables share one row, a blank column between
    titles = next(r for r in _sheet_rows(wb["Fares by Class"])
                  if r and r[0] == "BS / DAC-DXB")
    assert titles[7] == "BG / DAC-DXB" and titles[14] == "EK / DAC-DXB"
    # YQ + YR and its share sit under each column (no separate YQ-YR sheet)
    both = next(r for r in grid if r[0] == "YQ + YR")
    assert both[1:4] == [618, 0, 9000]
    fares = _sheet_rows(wb["Fare Comparison"])
    assert next(r for r in fares if r[0] == "Airline")[1:4] == ["BS", "BG", "EK"]


def test_a_run_without_firsttrip_shows_totals_and_says_why(tmp_path):
    """Field report 2026-10-25: a ShareTrip-only run left every tax sheet empty."""
    from openpyxl import load_workbook

    from market_engine.collect import CollectResult
    from market_engine.fares import build_fares
    from market_engine.render import write_workbook
    rows = [_row(source="ShareTrip", taxes=(), total=10703.0),
            _row("MH", "197", (), 16307.0, source="ShareTrip", via="KUL")]
    groups = T.group(rows, LIMITS)
    assert {g.status for g in groups} == {T.TOTAL_ONLY}
    assert groups[0].status_label == "total only (ShareTrip)"
    table = build_fares(CollectResult(rows=rows))
    wb = load_workbook(write_workbook(tmp_path / "m.xlsx", table=table, best_by_route={},
                                      rows=list(table.rows)))
    text = " ".join(str(v) for r in _sheet_rows(wb["Tax Comparison"]) for v in r if v)
    assert "Tick FirstTrip" in text and "not sent" in text and "16,307" not in text
    total = next(r for r in _sheet_rows(wb["Tax Comparison"]) if r[0] == "Total tax")
    assert 10703 in total and 16307 in total
    assert "OTA Check" not in wb.sheetnames
    missing = " ".join(str(v) for r in _sheet_rows(wb["Missing - Re-run"]) for v in r if v)
    assert "only ShareTrip searched" in missing


def test_fares_of_one_routing_share_a_column_even_when_a_small_code_differs():
    """MH via KUL: H8 on some fares only. One column, the code says how many carry it."""
    from market_engine.render_taxes import _partial
    base = (("BD", 500), ("YQ", 6177), ("MY", 212))
    rows = [_row("MH", "197", base + (("H8", 31),), 6920.0, via="KUL"),
            _row("MH", "103", base, 6889.0, via="KUL", clock="12:30")]
    (mh,) = T.group(rows, LIMITS)
    assert mh.fares == 2 and mh.carried_by("H8") == 1
    assert _partial(mh, "H8") == "31 (1 of 2)" and _partial(mh, "YQ") == 6177


def _workbook(tmp_path, rows):
    from openpyxl import load_workbook

    from market_engine.collect import CollectResult
    from market_engine.fares import build_fares
    from market_engine.render import write_workbook
    table = build_fares(CollectResult(rows=rows))
    return load_workbook(write_workbook(tmp_path / "m.xlsx", table=table, best_by_route={},
                                        rows=list(table.rows)))


def test_one_code_holding_the_whole_tax_is_not_itemised_when_peers_itemise():
    """6E PEN-DAC filed all 13,032 under MY; the others itemise MY 1,726 + G1 + H8."""
    peers = (("G1", 606), ("H8", 31), ("MY", 1726), ("YQ", 371))
    rows = [_row("MH", "1137", peers, 2734.0, o="PEN", d="DAC"),
            _row("6E", "1046", (("MY", 13032),), 13032.0, o="PEN", d="DAC", via="MAA")]
    statuses = {g.airline: g.status for g in T.group(rows, LIMITS)}
    assert statuses == {"MH": T.ITEMISED, "6E": T.NOT_ITEMISED}
    # alone on its route there is nothing to compare with: left as itemised
    assert T.group(rows[1:], LIMITS)[0].status == T.ITEMISED


def test_by_class_splits_a_routing_so_yq_reads_per_class():
    rows = [_row("MH", "1137", (("YQ", 371), ("MY", 1726)), 2097.0, o="PEN", d="DAC",
                 gross=31016.0, rbd="M"),
            _row("MH", "1143", (("YQ", 7336), ("MY", 1726)), 9062.0, o="PEN", d="DAC",
                 gross=45000.0, rbd="S", clock="10:00")]
    (table,) = T.by_class(rows, LIMITS)
    assert [(l.rbd, l.surcharge) for l in table.lines] == [("S", 7336), ("M", 371)]
    s_line = table.lines[0]                    # every figure from the same fare
    assert s_line.base_with_yq == s_line.base + 7336
    assert s_line.gross == 45000 and s_line.tax == 9062


def test_site_check_compares_fares_and_explains_tax_by_class():
    rows = [_row(gross=36671.0), _row(source="ShareTrip", taxes=(), total=12100.0,
                                      gross=36000.0, rbd="V")]
    (c,) = T.ota_check(rows)
    assert c.cheapest == "ShareTrip" and c.gross("FirstTrip") == 36671
    assert c.flagged == ("ShareTrip",)
    assert c.cause("ShareTrip") == "different fare class (V vs S)"


def test_cheapest_fare_borrows_yq_from_the_same_flights_itemised_fare():
    rows = [_row(taxes=(), total=10703.0, gross=36000.0), _row(gross=36671.0)]
    fare = T.cheapest_fares(rows, LIMITS)[("DAC-DXB", "BS")]
    assert fare.surcharge == 618 and fare.flight == "BS 341 20:15"


def test_missing_sheet_lists_one_direction_one_site_and_outliers(tmp_path):
    from market_engine import gaps
    from market_engine.collect import CollectResult
    from market_engine.fares import build_fares
    rows = [_row(), _row("BG", "147", BG_DOH, 14692.0, via="CCU|DOH"),
            _row("EK", "585", (("YQ", 9000),), 9000.0, gross=38000.0),
            _row("SQ", "447", (("YQ", 9000),), 9000.0, gross=600000.0),
            _row("BS", "342", BS_DXB, 10703.0, o="DXB", d="DAC")]
    table = build_fares(CollectResult(rows=rows))
    found = {(g.route, g.airline, g.kind) for g in
             gaps.find(table, rows, T.group(rows, LIMITS))}
    assert ("DAC-DXB", "BG", "one direction") in found
    assert ("DAC-DXB", "SQ", "outlier") in found
    assert ("DAC-DXB", "", "one site") in found
    assert ("DAC-DXB", "BS", "one direction") not in found


def test_fare_comparison_shows_the_cheapest_flight_and_each_site(tmp_path):
    rows = [_row(gross=36671.0), _row(source="ShareTrip", taxes=(), total=10703.0,
                                      gross=36000.0)]
    sheet = _sheet_rows(_workbook(tmp_path, rows)["Fare Comparison"])
    line = {r[0]: r[1] for r in sheet if r and r[0]}
    assert line["Cheapest flight"] == "BS 341 20:15" and line["Date"] == "31 Oct"
    assert line["Lowest on FirstTrip"] == 36671 and line["Lowest on ShareTrip"] == 36000
    assert line["Cheapest site"] == "ShareTrip"


def test_inbound_tax_rows_show_only_the_codes_the_route_has(tmp_path):
    rows = [_row("MH", "1137", (("G1", 606), ("MY", 1726), ("YQ", 371)), 2703.0,
                 o="PEN", d="DAC")]
    labels = [r[0] for r in _sheet_rows(_workbook(tmp_path, rows)["Tax Comparison"])]
    assert "BD" not in labels and labels.index("YQ") < labels.index("G1")


def test_only_sheets_with_something_to_say_are_created(tmp_path):
    """Two sites, an itemising one included, one route both ways, nothing odd: no
    Missing sheet; OTA Check present because the same flight is on both sites."""
    rows = [_row(), _row(source="ShareTrip", taxes=(), total=10703.0),
            _row(number="342", o="DXB", d="DAC"),
            _row(number="342", o="DXB", d="DAC", source="ShareTrip", taxes=(), total=10703.0)]
    names = _workbook(tmp_path, rows).sheetnames
    assert names == ["Fares", "Fare Comparison", "Fares by Class", "Tax Comparison",
                     "OTA Check"]


def test_tax_legend_lists_only_what_the_sheet_uses(tmp_path):
    rows = [_row(), _row("6E", "1102", (("BD", 17316),), 17316.0, via="BOM")]
    labels = [r[0] for r in _sheet_rows(_workbook(tmp_path, rows)["Tax Comparison"]) if r]
    legend = labels[labels.index("Legend") + 1:]
    assert T.ITEMISED in legend and T.NOT_ITEMISED in legend
    assert T.TOTAL_ONLY not in legend and T.CHECK not in legend
    assert "YQ" in legend                      # named in config/tax_codes.json


def test_a_firsttrip_fare_with_no_codes_and_no_total_is_still_listed():
    rows = [_row(), _row("QR", "641", (), 0.0, via="DOH")]
    qr = next(g for g in T.group(rows, LIMITS) if g.airline == "QR")
    assert qr.status == T.NOT_CAPTURED


def test_refundable_left_out_by_the_site_stays_unknown():
    offer = {"marketingCarrierCode": "BS", "finalTotalPrice": 4849, "finalBasePrice": 3724,
             "directions": [[{"segments": [{"originAirportCode": "DAC",
                                            "destinationAirportCode": "CXB",
                                            "departureTime": "2026-11-01T07:00:00"}]}]]}
    raw = firsttrip._normalize(offer, "Economy", None)
    assert raw["fare_refundable"] is None
    assert from_connector(raw, source="FirstTrip", cabin="Economy").refundable is None


def test_fares_by_class_puts_upper_cabins_first_then_a_blank_line(tmp_path):
    from dataclasses import replace
    rows = [replace(_row(gross=36671.0, rbd="S"), cabin_class="Economy"),
            replace(_row(gross=41000.0, rbd="Y", clock="21:00"), cabin_class="Economy"),
            replace(_row(gross=95000.0, rbd="J", clock="22:00"), cabin_class="Business")]
    (table,) = T.by_class(rows, LIMITS)
    assert [l.rbd for l in table.lines] == ["J", "Y", "S"] and table.split_cabins
    sheet = _sheet_rows(_workbook(tmp_path, rows)["Fares by Class"])
    first = next(i for i, r in enumerate(sheet) if r and r[0] == "RBD")
    assert [r[0] for r in sheet[first + 1:first + 5]] == ["J", None, "Y", "S"]
    assert sheet[first][3] == "Base with YQ"
    taxes = [r[0] for r in sheet[first + 5:] if r and r[0]]
    assert taxes[:3] == ["Taxes (BDT)", "YQ", "BD"] and "Total" in taxes


def test_fares_by_class_without_an_itemising_site_still_lists_classes(tmp_path):
    rows = [_row(source="ShareTrip", taxes=(), total=10703.0, rbd="V")]
    (table,) = T.by_class(rows, LIMITS)
    assert table.lines[0].tax == 10703 and table.lines[0].surcharge is None


def test_tax_blocks_line_up_across_a_band(tmp_path):
    rows = [_row(rbd="S"), _row(gross=41000.0, rbd="Y", clock="21:00"),
            _row("EK", "585", (("YQ", 9000), ("BD", 500)), 9500.0, rbd="K")]
    sheet = _sheet_rows(_workbook(tmp_path, rows)["Fares by Class"])
    row = next(r for r in sheet if r and r[0] == "Taxes (BDT)")
    assert row[7] == "Taxes (BDT)"            # EK's block starts on the same row
