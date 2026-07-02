"""
Tiny FX helper — convert foreign-currency OTA fares to BDT.

Live rates are fetched once per day from a free no-key API (open.er-api.com),
cached to output/manual_sessions/fx_rates.json, with constant fallbacks and
per-currency env overrides (FX_USD_BDT, FX_INR_BDT, ...).
"""
from __future__ import annotations

import datetime
import json
import os
from pathlib import Path
from typing import Dict, Optional

import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
CACHE_PATH = REPO_ROOT / "output" / "manual_sessions" / "fx_rates.json"
ENDPOINT = "https://open.er-api.com/v6/latest/USD"

# Fallbacks (BDT per 1 unit of currency) used if live fetch fails and no env override.
FALLBACK_BDT = {"BDT": 1.0, "USD": 122.74, "INR": 1.29}

_cache: Optional[Dict[str, float]] = None


def _today() -> str:
    return datetime.date.today().isoformat()


def _load_disk() -> Optional[Dict[str, float]]:
    if not CACHE_PATH.exists():
        return None
    try:
        data = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    if data.get("date") == _today() and isinstance(data.get("bdt_per"), dict):
        return {k: float(v) for k, v in data["bdt_per"].items()}
    return None


def _fetch_live() -> Optional[Dict[str, float]]:
    try:
        r = requests.get(ENDPOINT, timeout=20)
        rates = (r.json() or {}).get("rates") or {}
        usd_per = {c: float(v) for c, v in rates.items() if v}  # units of c per 1 USD
        bdt_usd = usd_per.get("BDT")
        if not bdt_usd:
            return None
        # BDT per 1 unit of currency c = (BDT per USD) / (c per USD)
        bdt_per = {"BDT": 1.0}
        for c, per_usd in usd_per.items():
            if per_usd:
                bdt_per[c] = bdt_usd / per_usd
        return bdt_per
    except Exception:  # noqa: BLE001 — FX is best-effort; fall back to constants
        return None


def _rates() -> Dict[str, float]:
    global _cache
    if _cache is not None:
        return _cache
    _cache = _load_disk()
    if _cache is None:
        live = _fetch_live()
        if live:
            _cache = live
            try:
                CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
                CACHE_PATH.write_text(json.dumps({"date": _today(), "bdt_per": live}, indent=1),
                                      encoding="utf-8")
            except OSError:
                pass
        else:
            _cache = dict(FALLBACK_BDT)
    return _cache


def rate(currency: str) -> float:
    """BDT per 1 unit of `currency`. Env override FX_<CUR>_BDT wins."""
    cur = str(currency or "BDT").upper()
    env = os.getenv(f"FX_{cur}_BDT")
    if env:
        try:
            return float(env)
        except ValueError:
            pass
    return _rates().get(cur, FALLBACK_BDT.get(cur, 1.0))


def to_bdt(amount: float, currency: str) -> float:
    return float(amount or 0) * rate(currency)
