"""Optional local live-search extensions (a generic plugin hook).

Standard installs ship NO plugins, so the app is HAR-only. An operator can drop a
live-capture extension for a channel into a ``plugins/`` folder (next to the app, in the
config dir, or at ``$OTA_PLUGINS_DIR``) on selected machines — without changing or
republishing the app. A plugin writes a channel HAR into the capture folder, which the
normal HAR pipeline then reads, so nothing channel-specific is baked into the app.

A plugin is a ``.py`` that defines::

    LIVE_PLUGIN = {
        "channel":  "<key matching auto_detect_hars, e.g. 'sharetrip'>",
        "label":    "<UI label, e.g. 'ShareTrip'>",
        "write_har": callable(routes: list[str], date: str, out_path: str) -> int,
    }

``write_har`` fetches live for the given ``ORIG-DEST`` routes and writes a HAR to
``out_path``, returning the number of entries written (0 = nothing captured).
"""
from __future__ import annotations

import importlib.util
import inspect
import os
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List


def _accepts_on_route(fn: Callable) -> bool:
    """True if ``fn`` takes an ``on_route`` parameter or ``**kwargs`` — so progress
    callbacks can be passed. Detected from the signature, NOT by catching TypeError
    (which would also swallow real TypeErrors raised inside the fetch)."""
    try:
        params = inspect.signature(fn).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(p.name == "on_route" or p.kind == inspect.Parameter.VAR_KEYWORD
               for p in params)


def _prune_stale_live_hars(har_dir: Path, channel: str, keep: Path) -> None:
    """Remove this channel's OTHER (older-dated) live HARs so auto_detect won't merge a
    stale capture with the fresh one just written (which would take the best discount
    across expired promotions). Only pruned AFTER a successful write, so a failed live
    run still falls back to the last good capture."""
    for stale in har_dir.glob(f"{channel}_live_*.har"):
        try:
            if stale.resolve() != keep.resolve():
                stale.unlink()
        except OSError:
            pass


def _plugin_dirs(config_dir: Path) -> List[Path]:
    dirs: List[Path] = []
    env = os.environ.get("OTA_PLUGINS_DIR")
    if env:
        dirs.append(Path(env))
    dirs.append(config_dir / "plugins")
    exe_dir = (Path(sys.executable).parent if getattr(sys, "frozen", False)
               else Path(__file__).resolve().parent)
    dirs.append(exe_dir / "plugins")
    seen: set = set()
    out: List[Path] = []
    for d in dirs:
        rd = d.resolve()
        if rd not in seen and d.is_dir():
            seen.add(rd)
            out.append(d)
    return out


def load_live_plugins(config_dir: Path) -> Dict[str, Dict[str, Any]]:
    """Discover live-search plugins. Returns {channel: {label, write_har}}. Never raises."""
    found: Dict[str, Dict[str, Any]] = {}
    for d in _plugin_dirs(config_dir):
        if str(d) not in sys.path:
            sys.path.insert(0, str(d))
        for py in sorted(d.glob("*.py")):
            try:
                spec = importlib.util.spec_from_file_location(f"live_plugin_{py.stem}", py)
                if spec is None or spec.loader is None:
                    continue
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                p = getattr(mod, "LIVE_PLUGIN", None)
                if isinstance(p, dict) and p.get("channel") and callable(p.get("write_har")):
                    found.setdefault(str(p["channel"]), {
                        "label": str(p.get("label") or p["channel"]),
                        "write_har": p["write_har"],
                    })
            except Exception:  # noqa: BLE001 — a broken plugin must never break the app
                continue
    return found


def write_live_hars(plugins: Dict[str, Dict[str, Any]], live_routes: Dict[str, str],
                    har_dir: Path, date: str, log=print) -> List[str]:
    """For each plugin channel with routes entered, fetch live and write a channel HAR
    into ``har_dir``. Returns the list of written HAR paths. Failures are logged, not raised."""
    written: List[str] = []
    for channel, spec in plugins.items():
        rstr = (live_routes.get(channel) or "").strip()
        if not rstr:
            continue
        routes = [r.strip().upper() for r in rstr.split(",") if r.strip()]
        if not routes:
            continue
        out = har_dir / f"{channel}_live_{date or 'latest'}.har"
        write_har: Callable = spec["write_har"]
        try:
            def _on_route(route, n, reason, _c=channel):
                log(f"  {_c} live {route}: {n}" + (f" ({reason})" if reason else ""))
            if _accepts_on_route(write_har):
                n = write_har(routes, date, str(out), on_route=_on_route)
            else:
                n = write_har(routes, date, str(out))
            if n:
                written.append(str(out))
                _prune_stale_live_hars(har_dir, channel, keep=out)
                log(f"  {channel} live: wrote {out.name} ({n} entries)")
            else:
                log(f"  {channel} live: no data (key rotated? see the plugin's --recover-key)")
        except Exception as exc:  # noqa: BLE001
            log(f"  {channel} live FAILED: {exc}")
    return written
