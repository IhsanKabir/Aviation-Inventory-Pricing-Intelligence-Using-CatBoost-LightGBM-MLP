"""Round trips on the Fare tab: parsing, collection, and the RT columns.

Shapes and amounts are the real ones from a 2026-10-27 DAC-CXB round-trip capture
that searched all three sites: FirstTrip (out 17 Nov, back 26 Nov), ShareTrip
(23 -> 26 Nov), GoZayaan (14 -> 16 Dec). Before this, the parsers read such a
capture as one-way fares at the round-trip price - and ShareTrip as DAC-DAC.
"""
import json
import sys
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from market_engine import cache as C  # noqa: E402
from market_engine import har as H  # noqa: E402
from market_engine import taxes as T  # noqa: E402
from market_engine.collect import CollectPlan, CollectResult, collect  # noqa: E402
from market_engine.fares import build_fares  # noqa: E402
from market_engine.rows import FlightRow, from_connector  # noqa: E402
from modules import firsttrip, gozayaan_har, sharetrip_har  # noqa: E402

RT_TAXES = (("BD", 50), ("OW", 1400), ("E5", 60), ("UT", 400), ("P7", 200), ("P8", 140),
            ("YR", 1000))


def _row(airline="BS", number="141", gross=4849.0, total=1625.0, rbd="Q",
         day=date(2026, 11, 17), trip="OW", back=None, back_airline="", back_rbd="",
         source="FirstTrip", taxes=(("BD", 25), ("OW", 700), ("YR", 500))) -> FlightRow:
    return FlightRow(source=source, airline=airline, flight_number=number, origin="DAC",
                     destination="CXB", departure_date=day, departure_time="07:10",
                     arrival_time="08:15", arrival_date=day, stops=0, via="", aircraft="",
                     operated_by="", cabin="Economy", gross_bdt=gross,
                     base_bdt=gross - total, base_is_real=True, seats=None,
                     fetched_at=datetime(2026, 10, 27, 17, 0), taxes=tuple(taxes),
                     tax_total=total, rbd=rbd, trip=trip, return_date=back,
                     return_time="18:35" if back else "",
                     return_flight="142" if back else "", return_rbd=back_rbd,
                     return_airline=back_airline or (airline if back else ""))


def _rt(airline="BS", gross=9698.0, stay=9, **kw) -> FlightRow:
    day = kw.pop("day", date(2026, 11, 17))
    kw.setdefault("taxes", RT_TAXES)
    kw.setdefault("total", 3250.0)
    return _row(airline, gross=gross, day=day, trip="RT", back=date.fromordinal(
        day.toordinal() + stay), **kw)


# ------------------------------------------------------------------ parsers
def _st_leg(o, d, day, clock, number, airline="BS", rbd="Q"):
    seg = {"flightNumber": number, "marketingAirline": airline, "operatedBy": airline,
           "origin": {"code": o}, "destination": {"code": d},
           "departureDateTime": {"date": day, "time": clock}, "resBookDesigCode": rbd,
           "cabin": rbd, "baggage": {"unit": "KG", "weight": 20}, "transitTime": 0}
    return {"marketingAirline": airline, "airlines": {"code": airline},
            "origin": {"code": o}, "destination": {"code": d},
            "departureDateTime": {"date": day, "time": clock},
            "arrivalDateTime": {"date": day, "time": "08:15"}, "layoverIataList": [],
            "segments": [seg]}


def _st_offer(legs):
    return {"sequenceCode": "usbrq", "isRefundable": True, "legs": legs,
            "displayPrice": {"totalFare": {"base": 6448, "tax": 3250, "total": 9698}}}


def test_sharetrip_round_trip_is_one_outbound_row_not_dac_to_dac():
    row = sharetrip_har._normalize(_st_offer([
        _st_leg("DAC", "CXB", "2026-11-23", "07:10:00", "141"),
        _st_leg("CXB", "DAC", "2026-11-26", "08:45:00", "142")]))
    assert (row["origin"], row["destination"]) == ("DAC", "CXB")
    assert row["stops"] == 0 and row["price_total_bdt"] == 9698
    assert row["trip_type"] == "RT" and row["return_flight_number"] == "142"
    r = from_connector(row, source="ShareTrip", cabin="Economy")
    assert r.is_round_trip and r.stay_days == 3 and r.return_time == "08:45"
    assert not r.mixed_carrier


def test_sharetrip_one_way_is_unchanged_and_multi_city_is_skipped():
    one = sharetrip_har._normalize(_st_offer([_st_leg("DAC", "CXB", "2026-11-23", "07:10:00", "141")]))
    assert "trip_type" not in one
    assert from_connector(one, source="ShareTrip", cabin="Economy").trip == "OW"
    multi = _st_offer([_st_leg("DAC", "CXB", "2026-11-23", "07:10:00", "141"),
                       _st_leg("CXB", "CGP", "2026-11-26", "08:45:00", "142")])
    assert sharetrip_har._normalize(multi) is None


def _ft_offer(combo=False):
    def seg(o, d, when, number, carrier):
        return {"originAirportCode": o, "destinationAirportCode": d, "departureTime": when,
                "arrivalTime": when, "flightNumber": number, "rbd": "Q",
                "operatingCarrierCode": carrier, "cabinClass": "Economy"}
    return {"marketingCarrierCode": "BS", "tripTypeId": 2, "isComboFare": combo,
            "returnMarketingCarrierCode": "2A" if combo else None,
            "finalTotalPrice": 9698.0, "finalBasePrice": 6448.0, "finalTaxPrice": 3250.0,
            "passengerFares": {"adt": {"taxes": 3250.0, "taxesBreakdown": [
                {"category": c, "amount": a} for c, a in RT_TAXES]}},
            "directions": [[{"stops": 0, "segments": [
                seg("DAC", "CXB", "2026-11-17T07:10:00", "141", "BS")]}],
                [{"stops": 0, "segments": [
                    seg("CXB", "DAC", "2026-11-26T18:35:00", "464" if combo else "142",
                        "2A" if combo else "BS")]}]]}


def test_firsttrip_round_trip_reads_the_return_direction():
    raw = firsttrip._normalize(_ft_offer(), "Economy", None)
    r = from_connector(raw, source="FirstTrip", cabin="Economy")
    assert r.is_round_trip and r.stay_days == 9 and r.return_flight == "142"
    assert r.gross_bdt == 9698 and dict(r.taxes)["OW"] == 1400
    # the HAR path (B2C discount rows) carries the same round-trip fields
    (b2c,) = firsttrip._b2c_rows_from_offers([_ft_offer()])
    assert from_connector(b2c, source="FirstTrip B2C (HAR)", cabin="Economy").is_round_trip


def test_a_combo_back_on_another_airline_is_mixed():
    raw = firsttrip._normalize(_ft_offer(combo=True), "Economy", None)
    r = from_connector(raw, source="FirstTrip", cabin="Economy")
    assert r.return_airline == "2A" and r.mixed_carrier


def test_gozayaan_round_trip_fare_has_two_legs():
    segs = [{"hash": "s1", "origin": "DAC", "destination": "CXB", "flight_number": "463",
             "marketing_carrier": "2A", "operating_carrier": "2A",
             "departure_date_time": "2026-12-14T17:00:00"},
            {"hash": "s2", "origin": "CXB", "destination": "DAC", "flight_number": "444",
             "marketing_carrier": "2A", "operating_carrier": "2A",
             "departure_date_time": "2026-12-16T09:35:00"}]
    legs = {"l1": {"hash": "l1", "segment_hashes": ["s1"], "marketing_carrier": "2A",
                   "departure_date_time": "2026-12-14T17:00:00"},
            "l2": {"hash": "l2", "segment_hashes": ["s2"], "marketing_carrier": "2A",
                   "departure_date_time": "2026-12-16T09:35:00"}}
    fare = {"id": "f", "leg_hashes": ["l1", "l2"], "total_fare_amount": 11198,
            "total_base_amount": 7948, "total_tax_amount": 3250}
    row = gozayaan_har._normalize_fare(fare, legs, {s["hash"]: s for s in segs}, "economy")
    r = from_connector(row, source="GoZayaan", cabin="Economy")
    assert (r.origin, r.destination, r.stay_days, r.return_flight) == ("DAC", "CXB", 2, "444")
    assert r.gross_bdt == 11198


def test_a_round_trip_without_its_return_day_is_dropped():
    raw = {"airline": "BS", "price_total_bdt": 9698, "departure": "2026-11-17T07:10:00",
           "origin": "DAC", "destination": "CXB", "trip_type": "RT"}
    assert from_connector(raw, source="X", cabin="Economy") is None


def test_one_har_holding_several_sites_feeds_every_parser(tmp_path):
    entries = [{"request": {"url": "https://b2c-api.firsttrip.com/flight/api/v1/Search",
                            "method": "POST"},
                "response": {"content": {"text": "data: " + json.dumps(
                    {"data": {"airSearchResponseWithFilters": {
                        "airSearchResponses": [_ft_offer()]}}})}}},
               {"request": {"url": "https://api.sharetrip.net/api/v2/flight/search/"
                                   "available-flights?searchId=x", "method": "POST"},
                "response": {"content": {"text": json.dumps({"response": {"matchedFlights": [
                    _st_offer([_st_leg("DAC", "CXB", "2026-11-23", "07:10:00", "141"),
                               _st_leg("CXB", "DAC", "2026-11-26", "08:45:00", "142")])]}})}}}]
    path = tmp_path / "RT Har.har"
    path.write_text(json.dumps({"log": {"entries": entries}}), encoding="utf-8")
    assert H.detect_all(path) == ["firsttrip_b2c", "sharetrip"]
    rows, notes = H.collect_har_rows(tmp_path)
    assert {r.source for r in rows} == {"FirstTrip B2C (HAR)", "ShareTrip (HAR)"}
    assert all(r.is_round_trip for r in rows) and not notes


# ------------------------------------------------------------------ HAR cut
def test_a_one_way_run_leaves_round_trips_out_and_says_so():
    kept, note = H.select_rows([_row(), _rt()])
    assert [r.trip for r in kept] == ["OW"] and "tick Round trip" in note


def test_a_round_trip_run_keeps_its_stay_only():
    kept, note = H.select_rows([_row(), _rt(stay=9), _rt(stay=3)], stay=9)
    assert sorted((r.trip, r.stay_days) for r in kept) == [("OW", None), ("RT", 9)]
    assert "return 3 day(s)" in note and "asks for 9" in note


def test_the_schedule_keeps_round_trip_rows():
    kept, note = H.select_rows([_row(), _rt()], schedule=True)
    assert len(kept) == 2 and note is None


# ------------------------------------------------------------------ fares
def test_one_way_and_round_trip_never_share_a_cell():
    res = CollectResult(rows=[_row(), _rt(), _rt(gross=24100.0, back_airline="2A")])
    ow, rt = build_fares(res), build_fares(res, trip="RT", stay=9)
    assert [(c.airline, c.gross_low) for c in ow.cells] == [("BS", 4849)]
    assert [(c.airline, c.gross_low) for c in rt.cells] == [("BS", 9698)]
    assert rt.mixed == 1 and rt.stay == 9 and ow.mixed == 0


def test_two_return_flights_are_two_round_trips():
    res = CollectResult(rows=[_rt(), replace(_rt(gross=10698.0), return_flight="148")])
    (cell,) = build_fares(res, trip="RT").cells
    assert cell.flights == 2


# ------------------------------------------------------------------ collection
def test_a_round_trip_plan_also_searches_one_way_where_a_site_can():
    plan = CollectPlan(routes=[("DAC", "CXB")], dates=[date(2026, 11, 17)],
                       source_keys=["firsttrip", "biman"], stay=7)
    assert plan.queries() == [("firsttrip", "DAC", "CXB", "2026-11-17", ""),
                              ("firsttrip", "DAC", "CXB", "2026-11-17", "2026-11-24"),
                              ("biman", "DAC", "CXB", "2026-11-17", "")]
    assert plan.no_round_trip() == ["Biman (direct)"]


def test_round_trips_are_cached_apart_from_one_way(tmp_path, monkeypatch):
    from market_engine import sources as S
    calls = []

    def fake(self, o, d, day, cabin="Economy", return_day=""):
        calls.append((day, return_day))
        return ([_rt()] if return_day else [_row()]), ""

    monkeypatch.setattr(S.LiveFetcher, "__call__", fake)
    store = C.CacheStore(tmp_path / "c.sqlite")
    plan = CollectPlan(routes=[("DAC", "CXB")], dates=[date(2026, 11, 17)],
                       source_keys=["firsttrip"], stay=9)
    first = collect(plan, store)
    again = collect(plan, store)
    # FirstTrip runs 3 searches at a time, so the two calls can come in either order
    assert sorted(calls) == [("2026-11-17", ""), ("2026-11-17", "2026-11-26")]
    assert again.from_cache == 2 and sorted(r.trip for r in again.rows) == ["OW", "RT"]
    (back,) = [r for r in again.rows if r.is_round_trip]
    assert back.return_date == date(2026, 11, 26) and back.return_flight == "142"
    assert first.fetched == 2


# ------------------------------------------------------------------ class view
def test_class_lines_carry_the_round_trip_of_the_same_class():
    rows = [_row(rbd="Q"), _row(gross=5849.0, rbd="K")]
    trips = [_rt(rbd="Q", back_rbd="Q"), _rt(gross=12097.0, rbd="K", back_rbd="Q")]
    (table,) = T.by_class(rows, {}, rt_rows=trips)
    lines = {l.rbd: l for l in table.lines}
    assert lines["Q"].rt_gross == 9698 and lines["Q"].rt_base_with_yq == 6448 + 1000
    assert lines["K"].rt_gross is None          # K out / Q back is neither class's fare
    assert table.codes_trip == "OW" and table.has_round_trip


def test_a_route_seen_only_as_round_trips_labels_its_taxes_so():
    (table,) = T.by_class([], {}, rt_rows=[_rt()])
    assert table.lines[0].gross is None and table.lines[0].rt_gross == 9698
    assert table.codes_trip == "RT"


def _sheet(wb, name):
    return [list(r) for r in wb[name].iter_rows(values_only=True)]


def test_the_workbook_puts_each_round_trip_under_its_one_way_line(tmp_path):
    from openpyxl import load_workbook

    from market_engine.render import write_workbook
    res = CollectResult(rows=[_row(), _row("2A", "441", gross=4849.0), _rt(),
                              _rt(gross=24100.0, back_airline="2A")])
    ow, rt = build_fares(res), build_fares(res, trip="RT", stay=9)
    wb = load_workbook(write_workbook(tmp_path / "rt.xlsx", table=ow, best_by_route={},
                                      rows=list(ow.rows), rt_table=rt))
    # Fares by Class: OW figures then RT figures across each class line
    cls = _sheet(wb, "Fares by Class")
    head = next(r for r in cls if r and r[0] == "RBD")
    figures = ["Base", "YQ+YR", "Base with YQ", "Tax", "Gross"]
    assert head[:11] == ["RBD"] + figures + figures
    group = cls[cls.index(head) - 1]
    assert group[1] == "OW" and group[6] == "RT"
    q = cls[cls.index(head) + 1]
    assert q[0] == "Q" and q[5] == 4849 and q[10] == 9698 and q[8] == 6448 + 1000
    # Fare Comparison: each airline has OW | RT columns under its name
    cmp_ = _sheet(wb, "Fare Comparison")
    labels = [r[0] for r in cmp_ if r and r[0]]
    assert not any("(RT)" in str(x) or str(x).startswith("ROUND TRIP") for x in labels)
    head = next(r for r in cmp_ if r and r[0] == "Airline")
    sub = cmp_[cmp_.index(head) + 1]
    bs, ta = head.index("BS"), head.index("2A")
    assert sub[bs:bs + 2] == ["OW", "RT"] and head[bs + 1] is None   # merged over both
    gross = next(r for r in cmp_ if r and r[0] == "Gross low")
    assert gross[bs] == 4849 and gross[bs + 1] == 9698
    assert gross[ta] == 4849 and gross[ta + 1] is None              # 2A: one-way only
    back = next(r for r in cmp_ if r and r[0] == "Return flight")
    assert back[bs] is None and back[bs + 1] == "BS 142 18:35"
    # Fares: one table with a Trip column, the RT line under its airline's OW line
    fares = _sheet(wb, "Fares")
    head = next(r for r in fares if r and r[0] == "Route")
    assert head[:3] == ["Route", "Airline", "Trip"]
    body = [tuple(r[1:3]) for r in fares[fares.index(head) + 1:] if r and r[0]]
    assert body.index(("BS", "RT")) == body.index(("BS", "OW")) + 1
    assert ("2A", "RT") not in body
    missing = [(r[1], r[2]) for r in _sheet(wb, "Missing - Re-run")[4:] if r and r[2]]
    assert ("2A", "no round trip") in missing and ("all", "mixed round trips") in missing
    assert "Tax Comparison" in wb.sheetnames     # the tax sheets stay one-way


def test_one_way_only_sheets_have_no_trip_column(tmp_path):
    from openpyxl import load_workbook

    from market_engine.render import write_workbook
    ow = build_fares(CollectResult(rows=[_row()]))
    wb = load_workbook(write_workbook(tmp_path / "ow.xlsx", table=ow, best_by_route={},
                                      rows=list(ow.rows)))
    head = next(r for r in _sheet(wb, "Fares") if r and r[0] == "Route")
    assert head[:3] == ["Route", "Airline", "Flights"]
    cmp_ = _sheet(wb, "Fare Comparison")
    head = next(r for r in cmp_ if r and r[0] == "Airline")
    assert cmp_[cmp_.index(head) + 1][0] == "Flights"            # no OW / RT row


def test_pair_trips_puts_round_trips_under_their_one_way_cells():
    from market_engine.fares import pair_trips
    res = CollectResult(rows=[_row(), _row("2A", "441", gross=4000.0), _rt()])
    ow, rt = build_fares(res), build_fares(res, trip="RT", stay=9)
    order = [(t, c.airline) for t, c in pair_trips(ow.cells, rt.cells)]
    assert order == [("OW", "2A"), ("OW", "BS"), ("RT", "BS")]
    # an airline seen only as a round trip still gets its line, after the route's one-way ones
    only_rt = [(t, c.airline) for t, c in pair_trips([], rt.cells)]
    assert only_rt == [("RT", "BS")]


def test_cached_round_trips_survive_the_encoding():
    (back,) = C._decode(C._encode([_rt()]))
    assert back == _rt()
    (old,) = C._decode(json.dumps([{k: v for k, v in json.loads(C._encode([_row()]))[0].items()
                                     if not k.startswith("return_") and k != "trip"}]))
    assert old.trip == "OW" and old.return_date is None      # rows cached before RT


# ------------------------------------------------------------------ desktop plan
def _host():
    from desktop.market_api import MarketApiMixin

    class _Host(MarketApiMixin):
        def __init__(self):
            self._config = {"har_dir": ""}

        def _live_ok(self):
            return True

    return _Host()


def test_the_stay_is_validated_and_ignored_by_the_schedule():
    host = _host()
    plan, err = host._market_plan("fare", "DAC-CXB", "2026-11-17", "2026-11-17",
                                  ["firsttrip"], "Economy", None, "7")
    assert err is None and plan.stay == 7
    _, err = host._market_plan("fare", "DAC-CXB", "2026-11-17", "2026-11-17",
                               ["firsttrip"], "Economy", None, "a week")
    assert "Stay must be" in err["error"]
    plan, _ = host._market_plan("schedule", "DAC-CXB", "2026-11-17", "2026-11-17",
                                ["firsttrip"], "Economy", None, "7")
    assert plan.stay is None
    plan, _ = host._market_plan("fare", "DAC-CXB", "2026-11-17", "2026-11-17",
                                ["firsttrip"], "Economy", None, "")
    assert plan.stay is None


def test_a_round_trip_code_is_judged_against_twice_the_one_way_ceiling():
    trip = _rt(taxes=(("BD", 1500), ("YQ", 800)), total=2300.0, gross=12000.0)
    (table,) = T.by_class([], {"BD": 1000.0}, rt_rows=[trip])
    assert table.lines[0].rt_surcharge == 800        # BD 1,500 < 2 x 1,000: itemised


def test_the_one_way_tax_sheets_ignore_round_trip_rows():
    rows = [_row(), _rt()]
    (group,) = T.group(rows, {})
    assert group.fares == 1 and group.total_high == 1625
    twin = replace(_rt(), source="ShareTrip", taxes=())
    assert T.ota_check([_rt(), twin]) == []


def test_the_offer_caches_do_not_take_round_trips(tmp_path, monkeypatch):
    stored = {}
    monkeypatch.setattr(sharetrip_har, "load_cache", lambda: {})
    monkeypatch.setattr(sharetrip_har, "save_cache", stored.update)
    monkeypatch.setattr(sharetrip_har, "parse_har", lambda p: [
        {**sharetrip_har._normalize(_st_offer([
            _st_leg("DAC", "CXB", "2026-11-23", "07:10:00", "141"),
            _st_leg("CXB", "DAC", "2026-11-26", "08:45:00", "142")]))},
        sharetrip_har._normalize(_st_offer([_st_leg("DAC", "CXB", "2026-11-23", "07:10:00",
                                                    "141")]))])
    out = sharetrip_har.import_hars(["x.har"])
    assert out["offers_imported"] == 1


def test_one_way_source_trust_is_judged_on_one_way_rows():
    # a source with only round trips has no one-way rows to judge, and vice versa
    res = CollectResult(rows=[_row(), _rt(source="GoZayaan")])
    assert [c.sources for c in build_fares(res).cells] == [("FirstTrip",)]
    assert [c.sources for c in build_fares(res, trip="RT").cells] == [("GoZayaan",)]
