# scheduler.py
import time
import datetime
import subprocess
import json
import os
import sys
from pathlib import Path

CONFIG_PATH = Path("config/settings.json")

def load_config():
    with CONFIG_PATH.open() as fh:
        return json.load(fh)

def in_active_window(conf, now=None):
    if now is None: now = datetime.datetime.now()
    if not conf.get("run_between_hours_enabled", True):
        return True
    sh = int(conf.get("active_start_hour", 10))
    eh = int(conf.get("active_end_hour", 17))
    return sh <= now.hour < eh

def run_once(conf):
    # call wrapper so we get archiving & postprocessing
    cmd = conf.get("run_all_command", "python run_all.py")
    # run via subprocess shell so user can use windows paths in config
    print(f"[{datetime.datetime.now()}] Running wrapper: {cmd}")
    res = subprocess.run(cmd, shell=True)
    return res.returncode

def main():
    conf = load_config()
    interval = conf.get("interval_hours", 1)
    interval_seconds = max(30, int(interval * 3600))  # min 30s safety

    print("Scheduler started. Interval (hours):", interval)
    try:
        while True:
            now = datetime.datetime.now()
            if in_active_window(conf, now):
                rc = run_once(conf)
                print(f"[{datetime.datetime.now()}] run_all exit code: {rc}")
            else:
                print(f"[{datetime.datetime.now()}] Outside active window; skipping this run.")
            # sleep until next interval tick
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("Scheduler stopped by user.")
        sys.exit(0)

if __name__ == "__main__":
    main()
