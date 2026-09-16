"""Schedule + Fare bridge for the desktop UI.

Kept out of backend.py (already at its size limit) and mixed into DesktopApi.
Every import of market_engine is deferred into the method that needs it, so the
app still starts instantly and a machine missing the extra engine degrades to
the discount tab rather than failing to launch.

The same sign-in wall as the discount run applies. These runs are NOT metered:
quota is tied to syncing a report, and a teammate checking a schedule should
never burn it.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional


def _bundled(rel: str) -> Optional[Path]:
    """Locate a file shipped with the app, in the exe bundle or the repo."""
    import sys
    roots = []
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        roots.append(Path(meipass))
    roots.append(Path(__file__).resolve().parents[1])
    for root in roots:
        candidate = root / rel
        if candidate.is_file():
            return candidate
    return None


class MarketApiMixin:
    """Schedule and fare views over live sources."""

    #: A month is the usual ask; the ceiling stops a typo becoming hours of live
    #: queries. The estimate is shown before anything runs regardless.
    MARKET_MAX_DAYS = 62

    # ------------------------------------------------------------------ helpers
    def _market_cache(self):
        from desktop.backend import config_dir
        from market_engine.cache import CacheStore
        return CacheStore(config_dir() / "cache" / "market.sqlite")

    def route_presets(self) -> dict:
        """Preset route groups; the operator's own file wins over the bundled one."""
        from desktop.backend import config_dir
        candidates = (config_dir() / "route_presets.json",
                      _bundled("config/route_presets.json"))
        for path in candidates:
            try:
                if path and Path(path).is_file():
                    data = json.loads(Path(path).read_text(encoding="utf-8"))
                    return {k: v for k, v in data.items() if not str(k).startswith("_")}
            except (OSError, ValueError):
                continue
        return {}

    def market_state(self) -> dict:
        """What these tabs can offer on THIS machine.

        Capability travels with each source so the UI can grey one out with a
        reason, instead of running for minutes and then showing an empty table.
        """
        from market_engine import sources as S
        available = S.live_available()
        return {
            "presets": self.route_presets(),
            "max_days": self.MARKET_MAX_DAYS,
            "sources": [{"key": k, "label": s.label, "available": bool(available.get(k)),
                         "can_schedule": s.can_schedule, "can_fare": s.can_fare,
                         "note": s.note} for k, s in S.LIVE.items()],
        }

    def _market_plan(self, kind: str, routes: str, date_from: str, date_to: str,
                     sources: list, cabin: str):
        """-> (plan, error_payload). Everything is validated before any network work."""
        from market_engine import sources as S
        from market_engine.collect import CollectPlan

        pairs = []
        for chunk in str(routes or "").replace(";", ",").split(","):
            chunk = chunk.strip().upper()
            if not chunk:
                continue
            origin, _, dest = chunk.partition("-")
            if len(origin) != 3 or len(dest) != 3 or not origin.isalpha() or not dest.isalpha():
                return None, {"ok": False,
                              "error": "Route must look like DAC-CGP (got {}).".format(chunk)}
            pairs.append((origin, dest))
        if not pairs:
            return None, {"ok": False, "error": "Enter at least one route, e.g. DAC-CGP."}

        try:
            start = datetime.strptime(date_from, "%Y-%m-%d").date()
            end = datetime.strptime(date_to, "%Y-%m-%d").date()
        except (TypeError, ValueError):
            return None, {"ok": False, "error": "Dates must be YYYY-MM-DD."}
        if end < start:
            return None, {"ok": False, "error": "The end date is before the start date."}
        span = (end - start).days + 1
        if span > self.MARKET_MAX_DAYS:
            return None, {"ok": False,
                          "error": "{} days is beyond the {}-day limit.".format(
                              span, self.MARKET_MAX_DAYS)}

        want = "schedule" if kind == "schedule" else "fare"
        picked = [k for k in (sources or []) if k in S.LIVE]
        if not picked:
            return None, {"ok": False, "error": "Pick at least one source."}
        picked = [k for k in picked
                  if (S.LIVE[k].can_schedule if want == "schedule" else S.LIVE[k].can_fare)]
        if not picked:
            return None, {"ok": False,
                          "error": "None of the chosen sources can answer a {}.".format(want)}

        dates = [start + timedelta(days=i) for i in range(span)]
        return CollectPlan(routes=pairs, dates=dates, source_keys=picked,
                           cabin=cabin or "Economy", purpose=want), None

    # --------------------------------------------------------------- operations
    def market_estimate(self, kind: str, routes: str, date_from: str, date_to: str,
                        sources: list, cabin: str = "Economy") -> dict:
        """What the run will cost, before committing to it."""
        from market_engine.collect import estimate
        plan, err = self._market_plan(kind, routes, date_from, date_to, sources, cabin)
        if err:
            return err
        out = estimate(plan, self._market_cache())
        out["ok"] = True
        return out

    def cancel_market(self) -> dict:
        self._market_cancel = True
        return {"ok": True}

    def run_market(self, kind: str, routes: str, date_from: str, date_to: str,
                   sources: list, cabin: str = "Economy",
                   direct_only: bool = False) -> dict:
        """Collect once, then answer either the schedule or the fare question."""
        if getattr(self, "_busy", False):
            return {"ok": False, "error": "A run is already in progress."}
        plan, err = self._market_plan(kind, routes, date_from, date_to, sources, cabin)
        if err:
            return err
        blocked = self._require_access()          # same wall as the discount run
        if blocked:
            return blocked

        from market_engine.collect import collect
        from market_engine.fares import build_fares, cheapest_by_route
        from market_engine.schedule import build_schedule, weekly_pattern

        self._busy, self._market_cancel = True, False
        self._status = "Collecting..."
        try:
            def progress(message: str, done: int, total: int) -> None:
                self._status = "{}  ({}/{})".format(message, done, total)

            result = collect(plan, self._market_cache(), progress=progress,
                             should_cancel=lambda: getattr(self, "_market_cancel", False))
            first, last = plan.dates[0], plan.dates[-1]
            payload: dict[str, Any] = {
                "ok": True, "kind": kind, "cancelled": result.cancelled,
                "rows": len(result.rows), "fetched": result.fetched,
                "from_cache": result.from_cache,
                "seconds": round(result.seconds, 1), "errors": result.errors[:20],
                "date_from": first.isoformat(), "date_to": last.isoformat()}

            if kind == "schedule":
                sched = build_schedule(result, date_from=first, date_to=last)
                patterns = weekly_pattern(sched, date_from=first, date_to=last)
                self._market_last = {"kind": kind, "sched": sched, "patterns": patterns}
                payload.update({
                    "patterns": [{"route": p.route, "airline": p.airline, "flight": p.flight,
                                  "operates": p.weekday_label, "departure": p.departure,
                                  "arrival": p.arrival, "dates": p.dates,
                                  "sources": list(p.sources), "varies": p.varies,
                                  "time_varies": p.time_varies,
                                  "disagreement": p.disagreement} for p in patterns],
                    "held_back": sched.itineraries,
                    "dropped_no_clock": sched.dropped_no_clock,
                    "dropped_no_flight": sched.dropped_no_flight,
                    "refused": dict(sched.sources_refused or {})})
            else:
                table = build_fares(result, direct_only=direct_only)
                best = cheapest_by_route(table.cells)
                self._market_last = {"kind": kind, "table": table, "best": best}
                payload.update({
                    "cells": [{"route": c.route, "airline": c.airline, "flights": c.flights,
                               "nonstop": c.nonstop, "dates": c.dates,
                               "gross_low": c.gross_low, "gross_high": c.gross_high,
                               "gross_avg": c.gross_avg, "base_low": c.base_low,
                               "base_high": c.base_high, "base_avg": c.base_avg,
                               "base_available": c.base_available,
                               "base_derived": c.base_derived,
                               "age_hours": round(c.age_hours, 1),
                               "cheapest": best.get(c.route) == c.airline}
                              for c in table.cells],
                    "gross_only": list(table.gross_only),
                    "refused": dict(table.refused or {})})

            self._log_usage("market_" + kind, count=len(result.rows),
                            target="{}..{}".format(first, last))
            return payload
        except Exception as exc:                  # noqa: BLE001 — surface, never crash the UI
            return {"ok": False, "error": "{}: {}".format(type(exc).__name__, exc)}
        finally:
            self._busy, self._status = False, ""

    def export_market(self) -> dict:
        """Write the last schedule/fare result to a workbook."""
        last = getattr(self, "_market_last", None)
        if not last:
            return {"ok": False, "error": "Run the Schedule or Fare tab first."}
        from market_engine.render import write_workbook

        default_name = "Market_{}_{}.xlsx".format(last["kind"],
                                                  datetime.now().strftime("%Y%m%d"))
        target = None
        if getattr(self, "_window", None):
            import webview
            target = self._window.create_file_dialog(webview.SAVE_DIALOG,
                                                     save_filename=default_name)
        # pywebview returns a tuple/list on some platforms and a string on others;
        # Path(str(tuple)) once wrote to a garbage filename in the exe's temp cwd.
        if isinstance(target, (tuple, list)):
            target = target[0] if target else None
        if not target:
            return {"ok": False, "error": "Export cancelled."}
        path = Path(str(target))
        if path.suffix.lower() != ".xlsx":
            path = path.with_suffix(".xlsx")
        try:
            written = write_workbook(path, patterns=last.get("patterns"),
                                     sched=last.get("sched"), table=last.get("table"),
                                     best_by_route=last.get("best"))
        except OSError as exc:
            return {"ok": False, "error": "Could not write the file: {}".format(exc)}
        self._log_usage("market_export", target=last["kind"])
        try:      # open Explorer with the file selected so it is impossible to miss
            import subprocess
            subprocess.Popen(["explorer", "/select,", str(written)])
        except OSError:
            pass
        return {"ok": True, "path": str(written)}
