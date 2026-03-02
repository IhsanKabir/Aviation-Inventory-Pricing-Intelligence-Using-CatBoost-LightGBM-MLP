import json
import os

state_file = "state.json"

print("Exists:", os.path.exists(state_file))

with open(state_file, "r") as f:
    data = json.load(f)

print("Loaded cookies:", len(data.get("cookies", [])))
print("Loaded origins:", data.keys())
