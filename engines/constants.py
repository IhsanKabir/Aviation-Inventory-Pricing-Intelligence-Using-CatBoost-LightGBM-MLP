# engines/constants.py
from pathlib import Path
from urllib.request import localhost

IDENTITY_COLS = [
    "airline",
    "origin",
    "destination",
    "flight_number",
    "departure",
    "cabin",
    "brand",
]
REPORT_OUTPUT_DIR = Path("reports")
REPORT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

import os
from dotenv import load_dotenv

load_dotenv()

DB_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
)
