# run_wrapper.py
import json
import shutil
import time
from pathlib import Path
import subprocess
import datetime
import os
import sys

ROOT = Path(".")
CONF = Path("config/settings.json")
if not CONF.exists():
    raise SystemExit("Missing config/settings.json — create it from template.")

conf = json.loads(CONF.read_text())
LATEST = Path(conf.get("output_latest_dir", "output/latest"))
ARCHIVE = Path(conf.get("output_archive_dir", "output/archive"))
DEBUG = Path(conf.get("debug_dir", "debug"))

LATEST.mkdir(parents=True, exist_ok=True)
ARCHIVE.mkdir(parents=True, exist_ok=True)
DEBUG.mkdir(parents=True, exist_ok=True)

COMBINED_JSON = LATEST / "combined_results.json"
COMBINED_CSV = LATEST / "combined_results.csv"

def timestamp():
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

def archive_previous():
    # move previous combined files to archive with timestamp
    ts = timestamp()
    for p in (COMBINED_JSON, COMBINED_CSV):
        if p.exists():
            dest = ARCHIVE / f"{p.stem}_{ts}{p.suffix}"
            print(f"Archiving {p} -> {dest}")
            shutil.copy2(p, dest)

def call_run_all():
    cmd = conf.get("run_all_command", "python run_all.py")
    print("Calling:", cmd)
    res = subprocess.run(cmd, shell=True)
    return res.returncode

def save_debug_raw(name, data_bytes):
    path = DEBUG / f"{timestamp()}_{name}.raw"
    path.write_bytes(data_bytes)
    return path

def main():
    archive_previous()
    rc = call_run_all()
    print("run_all returned", rc)
    # If run_all produced combined_results.json, attempt postprocessing
    if COMBINED_JSON.exists():
        print("Found combined_results.json — running postprocess")
        subprocess.run([sys.executable, "postprocess.py"])
    else:
        print("No combined_results.json found. Check run_all logs.")
    return rc

if __name__ == "__main__":
    rc = main()
    sys.exit(rc)
