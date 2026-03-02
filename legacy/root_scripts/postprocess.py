# postprocess.py
import json
from pathlib import Path
import datetime
import subprocess
import sys
import sqlite3
import csv
import copy
import os

CONF = Path("config/settings.json")
conf = json.loads(CONF.read_text())
LATEST = Path(conf.get("output_latest_dir", "output/latest"))
ARCHIVE = Path(conf.get("output_archive_dir", "output/archive"))
DB_PATH = Path(conf.get("sqlite_db", "output/combined_results.db"))
MAX_SHIFT = int(conf.get("max_shift_days", 3))
RERUN_ATTEMPTS = int(conf.get("rerun_attempts", 2))
DEBUG = Path(conf.get("debug_dir", "debug"))

COMBINED_JSON = LATEST / "combined_results.json"
COMBINED_CSV = LATEST / "combined_results.csv"

def load_json():
    txt = COMBINED_JSON.read_text(encoding="utf-8")
    # try parse as list
    try:
        data = json.loads(txt)
        if isinstance(data, list):
            return data
    except Exception:
        pass
    # fallback: newline-delimited JSON objects
    rows = []
    for line in txt.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            pass
    return rows

def has_flight_info(item):
    # heuristics: check for numeric total or presence of offers
    # adapt this function if your run_all produces different structure
    if not isinstance(item, dict):
        return False
    # common keys observed: 'total', 'fare', 'offers', 'result', 'price'
    # Check 'total' numeric nested alternatives -> handle both dict and list
    def find_total(d):
        if isinstance(d, dict):
            for k,v in d.items():
                if k.lower() in ("total","price","fare","amount"):
                    # numeric check
                    if isinstance(v, (int, float)):
                        return True
                    if isinstance(v, dict):
                        # alternatives pattern: {"alternatives":[[[{"amount":7024,"currency":"BDT"}]]]}
                        if "alternatives" in v:
                            try:
                                alt = v["alternatives"]
                                # try to find any numeric "amount"
                                import itertools
                                def find_amount(x):
                                    if isinstance(x, dict) and "amount" in x and isinstance(x["amount"], (int,float)):
                                        return True
                                    if isinstance(x, list):
                                        for i in x:
                                            if find_amount(i):
                                                return True
                                    if isinstance(x, dict):
                                        for vv in x.values():
                                            if find_amount(vv):
                                                return True
                                    return False
                                return find_amount(alt)
                            except Exception:
                                pass
        return False

    # quick checks
    if find_total(item):
        return True
    for k in ("unbundledOffers","bundledOffers","brandedResults","result","offers","fares"):
        if k in item and item[k]:
            return True
    # fallback: if item contains 'ok' True and 'result' not empty
    if item.get("ok") and item.get("result"):
        return True
    # otherwise: assume no flights
    return False

def rerun_route(origin, dest, date_str, cabin="Economy"):
    # run biman module directly as a subprocess and capture JSON stdout
    cmd = [sys.executable, "-m", "modules.biman", "--origin", origin, "--destination", dest, "--date", date_str, "--curl", "--verbose"]
    try:
        print("Rerun cmd:", cmd)
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        out = proc.stdout.strip()
        err = proc.stderr.strip()
        if err:
            print("biman stderr:", err[:400])
        # modules.biman prints JSON to stdout at the end - find last JSON blob
        last_json = None
        for part in reversed(out.splitlines()):
            part = part.strip()
            if part.startswith("{") and part.endswith("}"):
                try:
                    last_json = json.loads(part)
                    break
                except Exception:
                    continue
        return last_json
    except Exception as e:
        print("Rerun exception", e)
        return None

def shift_and_rerun_bad_rows(rows):
    cleaned = []
    added = 0
    for row in rows:
        if has_flight_info(row):
            cleaned.append(row)
        else:
            # attempt to auto-shift dates up to MAX_SHIFT
            origin = row.get("origin") or row.get("from")
            dest = row.get("destination") or row.get("to")
            date = row.get("date") or row.get("when") or row.get("departure_date")
            cabin = row.get("cabin") or row.get("cabinClass") or "Economy"
            if not origin or not dest or not date:
                # can't rerun, skip
                continue
            # parse date
            try:
                d = datetime.datetime.fromisoformat(date)
            except Exception:
                try:
                    d = datetime.datetime.strptime(date, "%Y-%m-%d")
                except Exception:
                    # unknown format, skip
                    continue
            rerun_success = False
            for shift in range(1, MAX_SHIFT + 1):
                new_date = (d + datetime.timedelta(days=shift)).strftime("%Y-%m-%d")
                print(f"Attempting auto-shift for {origin}->{dest} from {date} -> {new_date}")
                for attempt in range(RERUN_ATTEMPTS):
                    out = rerun_route(origin, dest, new_date, cabin)
                    if out and out.get("ok") and out.get("result"):
                        print("Rerun success for", origin, dest, new_date)
                        # attach a metadata field so we know it was auto-shifted
                        res = out["result"]
                        if isinstance(res, dict):
                            res["_auto_shifted_from"] = date
                            res["_auto_shifted_to"] = new_date
                        cleaned.append(out)
                        rerun_success = True
                        added += 1
                        break
                if rerun_success:
                    break
            if not rerun_success:
                # no flights found even after shifting; skip adding (per your request)
                print(f"No flights found for {origin}->{dest} after shifting; skipping row.")
                continue
    print("Added", added, "rerun results")
    return cleaned

def write_json(rows):
    COMBINED_JSON.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")

def write_csv_from_json(rows):
    # If there's an existing CSV, try to update; otherwise create a simple CSV from keys
    if not rows:
        return
    # normalize first object keys for headers
    first = rows[0]
    # if wrapped response {"ok":True,"result":{...}} extract
    if isinstance(first, dict) and "ok" in first and "result" in first:
        # flatten to result items
        items = []
        for r in rows:
            if isinstance(r, dict) and r.get("ok") and r.get("result"):
                items.append({"raw_result": json.dumps(r["result"], ensure_ascii=False)})
        rows = items
    # create CSV with single column raw_result to be safe
    headers = ["raw_result"]
    with COMBINED_CSV.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers)
        writer.writeheader()
        for r in rows:
            if isinstance(r, dict) and "raw_result" in r:
                writer.writerow({"raw_result": r["raw_result"]})
            else:
                writer.writerow({"raw_result": json.dumps(r, ensure_ascii=False)})

def save_to_sqlite(rows):
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # one simple table storing JSON and metadata
    c.execute("""
    CREATE TABLE IF NOT EXISTS combined (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT,
        origin TEXT,
        destination TEXT,
        date TEXT,
        cabin TEXT,
        payload_json TEXT
    )
    """)
    for r in rows:
        # try to extract origin/dest/date
        origin = r.get("origin") or (r.get("result") and r.get("result").get("itinerary") and r.get("result").get("itinerary").get("from")) or None
        destination = r.get("destination") or None
        date = r.get("date") or None
        cabin = r.get("cabin") or None
        payload = json.dumps(r, ensure_ascii=False)
        c.execute("INSERT INTO combined(created_at, origin, destination, date, cabin, payload_json) VALUES (?, ?, ?, ?, ?, ?)",
                  (datetime.datetime.now().isoformat(), origin, destination, date, cabin, payload))
    conn.commit()
    conn.close()
    print("Saved to sqlite:", DB_PATH)

def main():
    if not COMBINED_JSON.exists():
        print("No combined_results.json found at", COMBINED_JSON)
        return
    rows = load_json()
    print("Loaded", len(rows), "rows from combined_results.json")
    # Filter and rerun missing flights
    cleaned = shift_and_rerun_bad_rows(rows)
    # combine original rows that already had flights + cleaned rerun results
    final = []
    for r in rows:
        if has_flight_info(r):
            final.append(r)
    # Add cleaned (new) rerun rows
    final.extend(cleaned)
    print("Final rows after cleaning:", len(final))
    # write back
    write_json(final)
    # write CSV (safe fallback)
    write_csv_from_json(final)
    # save to sqlite optional
    save_to_sqlite(final)

if __name__ == "__main__":
    main()
