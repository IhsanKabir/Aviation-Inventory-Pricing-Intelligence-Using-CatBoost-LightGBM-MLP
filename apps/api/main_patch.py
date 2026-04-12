# ── Patch for apps/api/main.py ────────────────────────────────────────────────
# Add these two lines alongside the other router imports and registrations.
#
# IMPORT (add near the other "from app.routers import ..." lines):
from app.routers import gds as gds_router
from app.routers import travelport_feedback as travelport_feedback_router

# REGISTER (add near the other app.include_router(...) calls):
app.include_router(gds_router.router, prefix="/gds", tags=["GDS"])
app.include_router(
    travelport_feedback_router.router,
    prefix="/travelport-agent",
    tags=["Travelport Feedback"],
)
