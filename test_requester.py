from modules.requester import Requester

dummy_url = "https://booking.biman-airlines.com/api/graphql"
dummy_payload = {"query": "query { __typename }"}

req = Requester(timeout=30)
ok, body, status = req.post(dummy_url, dummy_payload)
print({"ok": ok, "status": status, "body": body})
