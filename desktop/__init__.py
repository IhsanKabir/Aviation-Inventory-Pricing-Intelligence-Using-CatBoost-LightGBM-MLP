"""OTA Discount Report desktop app.

pywebview shell around the discount_engine library: pick the HAR capture folder,
run the comparison LOCALLY (HARs never leave the machine), view the colored grid,
export the xlsx, and sync the sanitized result JSON to the team dashboard.
"""

__version__ = "0.1.8"

APP_ID = "ota-discount-report"
