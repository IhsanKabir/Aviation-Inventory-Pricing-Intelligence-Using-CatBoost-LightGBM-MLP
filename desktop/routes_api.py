"""Route catalogue + the user's saved preferred routes, for the desktop UI.

Mixed into DesktopApi (backend.py is at its size limit). Preferred routes live in
the app's config.json, so they survive restarts and app updates, and can be edited
at any time from the Preferred routes picker. They order the By-route view and
Excel sheet, and on request fill the live-search boxes.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Iterable

#: More than this is a pasted list gone wrong, not a working set of routes.
MAX_PREFERRED = 100
_ROUTE = re.compile(r"^([A-Z]{3})-([A-Z]{3})$")


def normalize_routes(raw: Any) -> tuple[list[str], list[str]]:
    """Clean a route list from the UI -> (valid routes in order, rejected entries).

    Accepts a list or a comma/space/newline separated string; upper-cases, drops
    duplicates, and rejects anything that isn't ORIGIN-DEST with two different
    3-letter codes. Rejections are returned so the UI can say what it ignored.
    """
    items: Iterable[Any] = raw if isinstance(raw, (list, tuple)) else re.split(r"[\s,;]+", str(raw or ""))
    valid: list[str] = []
    rejected: list[str] = []
    for item in items:
        text = str(item or "").strip().upper()
        if not text:
            continue
        m = _ROUTE.match(text)
        if not m or m.group(1) == m.group(2):
            rejected.append(text)
        elif text not in valid:
            valid.append(text)
    return valid, rejected


class RoutesApiMixin:
    """Route catalogue + preferred routes (needs self._config / self._save_config)."""

    def route_catalog(self) -> dict[str, list[str]]:
        """Routes by region; the operator's own copy wins over the bundled file."""
        from desktop.backend import config_dir
        from desktop.market_api import _bundled
        for path in (config_dir() / "route_catalog.json", _bundled("config/route_catalog.json")):
            try:
                if path and Path(path).is_file():
                    data = json.loads(Path(path).read_text(encoding="utf-8"))
                    return {k: list(v) for k, v in data.items()
                            if not str(k).startswith("_") and isinstance(v, list)}
            except (OSError, ValueError):
                continue
        return {}

    def routes_state(self) -> dict[str, Any]:
        return {"catalog": self.route_catalog(),
                "preferred": list(self._config.get("preferred_routes") or [])}

    def save_preferred_routes(self, routes: Any, use_for_live: bool = False) -> dict[str, Any]:
        """Save the preferred list (replaces the old one; [] clears it). With
        use_for_live, also put it in the FirstTrip B2C box and every live-plugin
        box - an explicit choice, so those boxes never change behind the user."""
        valid, rejected = normalize_routes(routes)
        if len(valid) > MAX_PREFERRED:
            return {"ok": False, "error": f"At most {MAX_PREFERRED} preferred routes "
                                          f"({len(valid)} given)."}
        self._config["preferred_routes"] = valid
        if use_for_live and valid:
            joined = ",".join(valid)
            self._config["routes"] = joined
            plugins = getattr(self, "_live_plugins", {}) or {}
            self._config["live_routes"] = {c: joined for c in plugins}
        self._save_config()
        return {"ok": True, "preferred": valid, "rejected": rejected,
                "used_for_live": bool(use_for_live and valid)}
