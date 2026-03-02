# run_scheduler.py (dev)
import time, subprocess
while True:
    subprocess.run(["python","run_all.py"])
    # wait one hour
    time.sleep(3600)
