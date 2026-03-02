# full_auto_runner.py
import json, importlib, os
from core.date_utils import resolve_relative_date
from core.excel_writer import append_rows_to_excel

CONFIG = "config.json"

def load_config(path=CONFIG):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def run_once():
    cfg = load_config()
    output = cfg.get("output_file", "competitor_fares.xlsx")
    cookies_file = cfg.get("cookies_file", "state.json")
    routes = cfg.get("routes", [])
    date_tokens = cfg.get("dates", ["+0"])
    airlines = cfg.get("airlines", [])

    for origin, dest in routes:
        route_code = f"{origin}-{dest}"
        for dt in date_tokens:
            date_iso = resolve_relative_date(dt)
            all_rows = []
            for airline_name in airlines:
                try:
                    module = importlib.import_module(f"modules.{airline_name}")
                except Exception as e:
                    print(f"Could not import module {airline_name}: {e}")
                    continue
                try:
                    rows = module.search(origin, dest, date_iso, cookies_file=cookies_file)
                    print(f"[{airline_name}] {origin}->{dest} {date_iso}: {len(rows)} rows")
                    for r in rows:
                        r["airline_module"] = airline_name
                    all_rows.extend(rows)
                except Exception as e:
                    print(f"Error searching {airline_name} {origin}->{dest} {date_iso}: {e}")
            if all_rows:
                append_rows_to_excel(output, route_code, all_rows)
            else:
                print(f"No rows for {route_code} on {date_iso}")

if __name__ == "__main__":
    run_once()
