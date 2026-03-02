# payload_validator.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Tuple, List
try:
    import jsonschema
    from jsonschema import Draft7Validator
except Exception:
    Draft7Validator = None

ROOT = Path(__file__).resolve().parent

DEFAULT_SCHEMA = {
    "type": "object",
    "properties": {
        "variables": {
            "type": "object",
            "properties": {
                "searchQuery": {
                    "type": "object",
                    "properties": {
                        "originDestinations": {
                            "type": "array",
                            "minItems": 1,
                            "items": {
                                "type": "object",
                                "properties": {
                                    "origin": {"type": "string"},
                                    "destination": {"type": "string"},
                                    "departureDate": {"type": "string"}
                                },
                                "required": ["origin", "destination"]
                            }
                        }
                    },
                    "required": ["originDestinations"]
                }
            },
            "required": ["searchQuery"]
        }
    },
    "required": ["variables"]
}

def validate(payload: dict, schema: dict | None = None) -> Tuple[bool, List[str]]:
    schema = schema or DEFAULT_SCHEMA
    if Draft7Validator is None:
        return False, ["jsonschema not installed. Run: pip install jsonschema"]
    v = Draft7Validator(schema)
    errors = []
    for e in v.iter_errors(payload):
        # give user-friendly messages
        path = ".".join(str(x) for x in e.path) if e.path else "<root>"
        errors.append(f"{path}: {e.message}")
    return (len(errors) == 0), errors

def validate_from_file(path: Path | str) -> Tuple[bool, List[str]]:
    p = Path(path)
    if not p.exists():
        return False, [f"File not found: {p}"]
    payload = json.loads(p.read_text(encoding="utf-8"))
    return validate(payload)

if __name__ == "__main__":
    import sys
    target = sys.argv[1] if len(sys.argv) > 1 else ROOT / "payload.json"
    ok, msgs = validate_from_file(target)
    if ok:
        print("✅ payload.json VALID")
    else:
        print("❌ payload.json INVALID")
        for m in msgs:
            print(" -", m)
