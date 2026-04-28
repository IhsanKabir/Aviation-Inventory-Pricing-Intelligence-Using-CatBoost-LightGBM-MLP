"""Cookie capture documentation helpers.

Runtime cookie files belong under ``cookies/*.json`` and are intentionally
ignored by git. This module keeps only a redacted shape example so importing or
compiling ``core`` never depends on machine-local session data.
"""

from __future__ import annotations


SAMPLE_COOKIE_CAPTURE = {
    "cookies": {
        "DCSESSIONID": "<redacted>",
        "CID": "<redacted>",
        "AWSALB": "<redacted>",
        "AWSALBCORS": "<redacted>",
        "cf_clearance": "<redacted>",
        "reese84": "<redacted>",
    },
    "meta": {
        "conversation-id": "<redacted>",
        "message-id": "<redacted>",
        "execution": "<redacted>",
        "ref-to-message-id": "<redacted>",
    },
}
