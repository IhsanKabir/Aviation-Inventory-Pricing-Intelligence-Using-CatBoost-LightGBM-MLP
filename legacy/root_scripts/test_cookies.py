from pathlib import Path

import requests

from modules.requester import Requester


def load_cookies(path: str = "cookies/biman.json") -> dict:
    req = Requester(cookies_path=Path(path))
    return requests.utils.dict_from_cookiejar(req.session.cookies)


try:
    cookies = load_cookies()
    print("\nCookie module works correctly.")
    print(f"Loaded {len(cookies)} cookies:")
    for k, v in cookies.items():
        print(f"  {k} = {str(v)[:20]}...")
except Exception as e:
    print("\nCookie module has an error:")
    print(e)
