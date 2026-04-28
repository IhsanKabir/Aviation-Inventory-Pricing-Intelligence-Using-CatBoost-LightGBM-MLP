from __future__ import annotations

import json
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_SWITCHES_FILE = REPO_ROOT / "config" / "source_switches.json"
ENV_SOURCE_SWITCHES_FILE = "AIRLINE_SOURCE_SWITCHES_FILE"

TRUE_VALUES = {"1", "true", "yes", "on", "y"}
FALSE_VALUES = {"0", "false", "no", "off", "n"}

LEGACY_ENV_BY_SOURCE = {
    "sharetrip": "SHARETRIP_ENABLED",
}

CANONICAL_SOURCE_BY_COMPACT_NAME = {
    "airarabia": "airarabia",
    "airasia": "airasia",
    "airastra": "airastra",
    "amybd": "amybd",
    "bdfare": "bdfare",
    "biman": "biman",
    "bs": "bs",
    "gozayaan": "gozayaan",
    "indigo": "indigo",
    "maldivian": "maldivian",
    "novoair": "novoair",
    "salamair": "salamair",
    "sharetrip": "sharetrip",
    "ttinteractive": "ttinteractive",
    "usbangla": "bs",
    "usbanglaairlines": "bs",
}


def normalize_source_name(value: Any) -> str:
    raw = str(value or "").strip().lower()
    compact = re.sub(r"[^a-z0-9]+", "", raw)
    return CANONICAL_SOURCE_BY_COMPACT_NAME.get(compact, raw)


def _coerce_bool(value: Any, *, default: bool = True) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value or "").strip().lower()
    if text in TRUE_VALUES:
        return True
    if text in FALSE_VALUES:
        return False
    return default


def _resolve_source_switches_path(path: str | Path | None = None) -> Path:
    raw = path or os.getenv(ENV_SOURCE_SWITCHES_FILE) or DEFAULT_SOURCE_SWITCHES_FILE
    resolved = Path(raw)
    if not resolved.is_absolute():
        resolved = REPO_ROOT / resolved
    return resolved


@lru_cache(maxsize=32)
def _load_source_switches_cached(path_text: str, mtime_ns: int, size: int) -> dict[str, dict[str, Any]]:
    del mtime_ns, size
    path = Path(path_text)
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")

    raw_sources = payload.get("sources", payload)
    if not isinstance(raw_sources, dict):
        raise ValueError(f"{path} must contain a 'sources' object")

    switches: dict[str, dict[str, Any]] = {}
    for raw_name, raw_config in raw_sources.items():
        name = normalize_source_name(raw_name)
        if not name:
            continue
        if isinstance(raw_config, dict):
            config = dict(raw_config)
            config["enabled"] = _coerce_bool(config.get("enabled", True), default=True)
        else:
            config = {"enabled": _coerce_bool(raw_config, default=True)}
        switches[name] = config
    return switches


def load_source_switches(path: str | Path | None = None) -> dict[str, dict[str, Any]]:
    resolved = _resolve_source_switches_path(path)
    if not resolved.exists():
        return {}
    stat = resolved.stat()
    return _load_source_switches_cached(str(resolved), int(stat.st_mtime_ns), int(stat.st_size))


def source_switch_status(
    source_name: Any,
    *,
    switches: Mapping[str, Mapping[str, Any]] | None = None,
    source_switches_file: str | Path | None = None,
) -> dict[str, Any]:
    source = normalize_source_name(source_name)
    resolved_switches = switches if switches is not None else load_source_switches(source_switches_file)
    config = dict(resolved_switches.get(source) or {}) if source else {}
    enabled = True
    reasons: list[str] = []

    if config and not _coerce_bool(config.get("enabled", True), default=True):
        enabled = False
        reason = str(config.get("reason") or config.get("note") or "disabled in source switch file").strip()
        reasons.append(reason)

    env_name = LEGACY_ENV_BY_SOURCE.get(source)
    if env_name and os.getenv(env_name) is not None:
        if not _coerce_bool(os.getenv(env_name), default=True):
            enabled = False
            reasons.append(f"disabled by {env_name}=false")

    return {
        "source": source,
        "enabled": bool(enabled),
        "reasons": reasons,
        "config": config,
    }


def source_enabled(
    source_name: Any,
    *,
    switches: Mapping[str, Mapping[str, Any]] | None = None,
    source_switches_file: str | Path | None = None,
) -> bool:
    return bool(
        source_switch_status(
            source_name,
            switches=switches,
            source_switches_file=source_switches_file,
        ).get("enabled")
    )


def disabled_source_response(source_name: Any, *, message: str | None = None) -> dict[str, Any]:
    source = normalize_source_name(source_name)
    status = source_switch_status(source)
    reasons = status.get("reasons") or []
    return {
        "raw": {
            "source": source,
            "error": "source_disabled",
            "message": message or "; ".join(reasons) or f"{source} is disabled in source switches",
            "source_switch_reasons": reasons,
        },
        "originalResponse": None,
        "rows": [],
        "ok": False,
    }
