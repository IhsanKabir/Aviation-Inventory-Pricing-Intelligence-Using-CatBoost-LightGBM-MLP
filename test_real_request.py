from modules.requester import Requester
import json

# Load the REAL working payload
with open("payload.json", "r", encoding="utf-8") as f:
    payload = json.load(f)

url = "https://booking.biman-airlines.com/api/graphql"

req = Requester(timeout=30)
ok, body, status = req.post(url, payload)
print({"ok": ok, "status": status, "body": body})
