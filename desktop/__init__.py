"""OTA Discount Report desktop app.

pywebview shell around the discount_engine library: pick the HAR capture folder,
run the comparison LOCALLY (HARs never leave the machine), view the colored grid,
export the xlsx, and sync the sanitized result JSON to the team dashboard.

Copyright (c) 2026 Ihsan Kabir. All Rights Reserved. Proprietary software;
see the LICENSE file at the repository root. Not for copying or redistribution.
"""

__version__ = "0.1.19"

APP_ID = "ota-discount-report"
