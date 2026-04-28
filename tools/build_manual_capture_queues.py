from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCHEDULE_FILE = REPO_ROOT / "config" / "schedule.json"
DEFAULT_ROUTES_FILE = REPO_ROOT / "config" / "routes.json"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "output" / "manual_sessions" / "queues"


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_UTC")


def _today_utc() -> date:
    return datetime.now(timezone.utc).date()


def _json_load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _parse_iso_date(s: Any) -> date | None:
    if s in (None, ""):
        return None
    try:
        return date.fromisoformat(str(s).strip())
    except Exception:
        return None


def _parse_iso_date_list(values: list[Any]) -> list[str]:
    out: list[str] = []
    for v in values:
        d = _parse_iso_date(v)
        if d:
            out.append(d.isoformat())
    return out


def _expand_date_range(start_s: Any, end_s: Any) -> list[str]:
    start = _parse_iso_date(start_s)
    end = _parse_iso_date(end_s)
    if not start or not end or end < start:
        return []
    cur = start
    out: list[str] = []
    while cur <= end:
        out.append(cur.isoformat())
        cur = cur + timedelta(days=1)
    return out


def _parse_offsets_csv(raw: str) -> list[int]:
    out: list[int] = []
    for part in str(raw or "").split(","):
        p = part.strip()
        if not p:
            continue
        try:
            out.append(int(p))
        except Exception:
            continue
    return out


def _truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    return str(v or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _unique_preserve(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for v in values:
        s = str(v or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _drop_past_dates(values: list[str], *, today: date) -> list[str]:
    kept: list[str] = []
    for value in values:
        parsed = _parse_iso_date(value)
        if not parsed or parsed < today:
            continue
        iso = parsed.isoformat()
        if iso not in kept:
            kept.append(iso)
    return kept


def _load_dates_from_file(path: Path, *, today: date) -> list[str]:
    if not path.exists():
        return []
    try:
        obj = _json_load(path)
    except Exception:
        return []
    if isinstance(obj, list):
        return _parse_iso_date_list(list(obj))
    if not isinstance(obj, dict):
        return []

    if obj.get("date"):
        return _parse_iso_date_list([obj.get("date")])
    if obj.get("dates"):
        v = obj["dates"]
        return _parse_iso_date_list(v if isinstance(v, list) else str(v).split(","))
    if obj.get("date_start") and obj.get("date_end"):
        return _expand_date_range(obj.get("date_start"), obj.get("date_end"))
    if obj.get("date_start") or obj.get("date_end"):
        return _parse_iso_date_list([obj.get("date_start") or obj.get("date_end")])

    dr = obj.get("date_range")
    if isinstance(dr, dict):
        parsed = _expand_date_range(dr.get("start") or dr.get("date_start"), dr.get("end") or dr.get("date_end"))
        if parsed:
            return parsed

    if isinstance(obj.get("date_ranges"), list):
        merged: list[str] = []
        for item in obj["date_ranges"]:
            if not isinstance(item, dict):
                continue
            merged.extend(
                _expand_date_range(
                    item.get("start") or item.get("date_start"),
                    item.get("end") or item.get("date_end"),
                )
            )
        merged = _unique_preserve(merged)
        if merged:
            return merged

    if isinstance(obj.get("day_offsets"), list):
        offs: list[int] = []
        for v in obj["day_offsets"]:
            try:
                offs.append(int(v))
            except Exception:
                continue
        return _unique_preserve([(today + timedelta(days=o)).isoformat() for o in offs])

    return []


@dataclass
class DateResolveOptions:
    date: str | None = None
    dates_csv: str | None = None
    date_start: str | None = None
    date_end: str | None = None
    date_offsets_csv: str | None = None
    quick: bool = False
    limit_dates: int | None = None


def _resolve_schedule_dates(schedule_file: Path, *, repo_root: Path, opts: DateResolveOptions) -> dict[str, Any]:
    today = _today_utc()
    try:
        schedule_obj = _json_load(schedule_file) if schedule_file.exists() else {}
    except Exception:
        schedule_obj = {}

    root = {}
    if isinstance(schedule_obj, dict):
        adr = schedule_obj.get("auto_run_date_ranges")
        if isinstance(adr, dict):
            root = adr

    default_section = root.get("default") if isinstance(root.get("default"), dict) else {}
    run_pipeline_section = root.get("run_pipeline") if isinstance(root.get("run_pipeline"), dict) else {}
    legacy_scrape_section = root.get("scrape") if isinstance(root.get("scrape"), dict) else {}
    accumulation_section = root.get("accumulation") if isinstance(root.get("accumulation"), dict) else {}

    merged: dict[str, Any] = {}
    for section in (default_section, run_pipeline_section, legacy_scrape_section, accumulation_section):
        merged.update(section)

    dates: list[str] = []

    # Explicit overrides (same precedence intent as n8n code / pipeline)
    if opts.date:
        dates = _parse_iso_date_list([opts.date])
    elif opts.dates_csv:
        dates = _parse_iso_date_list(str(opts.dates_csv).split(","))
    elif opts.date_start and opts.date_end:
        dates = _expand_date_range(opts.date_start, opts.date_end)
    elif opts.date_start or opts.date_end:
        dates = _parse_iso_date_list([opts.date_start or opts.date_end])
    elif opts.date_offsets_csv:
        offs = _parse_offsets_csv(opts.date_offsets_csv)
        dates = [(today + timedelta(days=o)).isoformat() for o in offs]
    else:
        if _truthy(merged.get("combine")):
            combined: list[str] = []

            def _add_many(values: list[str]) -> None:
                nonlocal combined
                combined = _unique_preserve(combined + (values or []))

            if merged.get("date"):
                _add_many(_parse_iso_date_list([merged.get("date")]))

            if merged.get("dates"):
                v = merged.get("dates")
                _add_many(_parse_iso_date_list(v if isinstance(v, list) else str(v).split(",")))

            ds, de = merged.get("date_start"), merged.get("date_end")
            if ds and de:
                _add_many(_expand_date_range(ds, de))
            elif ds or de:
                _add_many(_parse_iso_date_list([ds or de]))

            if isinstance(merged.get("date_ranges"), list):
                for item in merged["date_ranges"]:
                    if not isinstance(item, dict):
                        continue
                    _add_many(
                        _expand_date_range(
                            item.get("start") or item.get("date_start"),
                            item.get("end") or item.get("date_end"),
                        )
                    )

            offs = merged.get("date_offsets")
            if isinstance(offs, list):
                parsed_offs: list[int] = []
                for v in offs:
                    try:
                        parsed_offs.append(int(v))
                    except Exception:
                        continue
                _add_many([(today + timedelta(days=o)).isoformat() for o in parsed_offs])
            elif isinstance(offs, str) and offs.strip():
                _add_many([(today + timedelta(days=o)).isoformat() for o in _parse_offsets_csv(offs)])

            if merged.get("dates_file"):
                file_path = Path(str(merged["dates_file"]))
                if not file_path.is_absolute():
                    file_path = repo_root / file_path
                _add_many(_load_dates_from_file(file_path, today=today))

            dates = sorted(_unique_preserve(combined))
        else:
            if merged.get("date"):
                dates = _parse_iso_date_list([merged.get("date")])
            elif merged.get("dates"):
                v = merged.get("dates")
                dates = _parse_iso_date_list(v if isinstance(v, list) else str(v).split(","))
            elif merged.get("date_start") and merged.get("date_end"):
                dates = _expand_date_range(merged.get("date_start"), merged.get("date_end"))
            elif merged.get("date_start") or merged.get("date_end"):
                dates = _parse_iso_date_list([merged.get("date_start") or merged.get("date_end")])
            elif merged.get("date_offsets"):
                offs = merged.get("date_offsets")
                if isinstance(offs, list):
                    parsed_offs = []
                    for v in offs:
                        try:
                            parsed_offs.append(int(v))
                        except Exception:
                            continue
                else:
                    parsed_offs = _parse_offsets_csv(str(offs))
                dates = [(today + timedelta(days=o)).isoformat() for o in parsed_offs]
            elif merged.get("dates_file"):
                file_path = Path(str(merged["dates_file"]))
                if not file_path.is_absolute():
                    file_path = repo_root / file_path
                dates = _load_dates_from_file(file_path, today=today)

    dates = _drop_past_dates(_unique_preserve(dates), today=today)

    if not dates:
        fallback_offsets = [0] if opts.quick else [0, 3, 5, 7, 15, 30]
        dates = [(today + timedelta(days=o)).isoformat() for o in fallback_offsets]

    if opts.limit_dates and int(opts.limit_dates) > 0:
        dates = dates[: int(opts.limit_dates)]

    return {
        "dates": dates,
        "schedule_file": str(schedule_file.resolve()),
        "merged_schedule_scrape_defaults": merged,
    }


def _load_routes(routes_file: Path) -> list[dict[str, Any]]:
    raw = _json_load(routes_file)
    if not isinstance(raw, list):
        raise SystemExit(f"routes file must be a JSON list: {routes_file}")
    out: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        airline = str(item.get("airline") or "").strip().upper()
        origin = str(item.get("origin") or "").strip().upper()
        destination = str(item.get("destination") or "").strip().upper()
        if not (airline and origin and destination):
            continue
        out.append(
            {
                "airline": airline,
                "origin": origin,
                "destination": destination,
                "cabins": item.get("cabins"),
            }
        )
    return out


def _build_jobs(routes: list[dict[str, Any]], dates: list[str], *, manual_cabin: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    bs2a_jobs: list[dict[str, Any]] = []
    q2_jobs: list[dict[str, Any]] = []
    protected_jobs: list[dict[str, Any]] = []
    for r in routes:
        airline = r["airline"]
        if airline in {"BS", "2A"}:
            for d in dates:
                bs2a_jobs.append(
                    {
                        "carrier": airline,
                        "origin": r["origin"],
                        "destination": r["destination"],
                        "date": d,
                        "cabin": manual_cabin,
                    }
                )
                protected_jobs.append(
                    {
                        "carrier": airline,
                        "source_family": "ttinteractive",
                        "origin": r["origin"],
                        "destination": r["destination"],
                        "date": d,
                        "cabin": manual_cabin,
                    }
                )
        elif airline == "Q2":
            for d in dates:
                q2_jobs.append(
                    {
                        "origin": r["origin"],
                        "destination": r["destination"],
                        "date": d,
                        "cabin": manual_cabin,
                    }
                )
                protected_jobs.append(
                    {
                        "carrier": airline,
                        "source_family": "maldivian_plnext",
                        "origin": r["origin"],
                        "destination": r["destination"],
                        "date": d,
                        "cabin": manual_cabin,
                    }
                )
        elif airline in {"G9", "OV"}:
            for d in dates:
                protected_jobs.append(
                    {
                        "carrier": airline,
                        "source_family": "airarabia_har" if airline == "G9" else "salamair_har",
                        "origin": r["origin"],
                        "destination": r["destination"],
                        "date": d,
                        "cabin": manual_cabin,
                    }
                )
    return bs2a_jobs, q2_jobs, protected_jobs


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> int:
    p = argparse.ArgumentParser(
        description="Build queue JSON files for manual-assisted capture runners (BS/2A + Q2 + G9/OV) using config/routes.json and config/schedule.json date defaults.",
    )
    p.add_argument("--schedule-file", default=str(DEFAULT_SCHEDULE_FILE))
    p.add_argument("--routes-file", default=str(DEFAULT_ROUTES_FILE))
    p.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    p.add_argument("--result-out", help="Optional manifest JSON path (default under output-dir)")

    # Explicit date overrides (optional)
    p.add_argument("--date")
    p.add_argument("--dates")
    p.add_argument("--date-start")
    p.add_argument("--date-end")
    p.add_argument("--date-offsets")
    p.add_argument("--quick", action="store_true")
    p.add_argument("--limit-dates", type=int)

    p.add_argument("--cabin", default="Economy", help="Manual capture cabin for generated queue rows (default: Economy)")
    p.add_argument("--no-bs2a", action="store_true")
    p.add_argument("--no-q2", action="store_true")
    p.add_argument("--no-protected", action="store_true", help="Do not write the aggregate protected-source queue.")
    p.add_argument("--dry-run", action="store_true", help="Print manifest only; do not write queue files")
    args = p.parse_args()

    schedule_file = Path(args.schedule_file)
    routes_file = Path(args.routes_file)
    output_dir = Path(args.output_dir)
    run_tag = _now_tag()

    resolved = _resolve_schedule_dates(
        schedule_file,
        repo_root=REPO_ROOT,
        opts=DateResolveOptions(
            date=args.date,
            dates_csv=args.dates,
            date_start=args.date_start,
            date_end=args.date_end,
            date_offsets_csv=args.date_offsets,
            quick=bool(args.quick),
            limit_dates=args.limit_dates,
        ),
    )

    routes = _load_routes(routes_file)
    bs2a_jobs_all, q2_jobs_all, protected_jobs_all = _build_jobs(routes, resolved["dates"], manual_cabin=str(args.cabin))
    bs2a_jobs = [] if args.no_bs2a else bs2a_jobs_all
    q2_jobs = [] if args.no_q2 else q2_jobs_all
    protected_jobs = [] if args.no_protected else protected_jobs_all

    bs2a_queue_path = output_dir / f"bs2a_jobs_{run_tag}.json"
    q2_queue_path = output_dir / f"q2_jobs_{run_tag}.json"
    protected_queue_path = output_dir / f"protected_source_jobs_{run_tag}.json"
    result_out = Path(args.result_out) if args.result_out else (output_dir / f"manual_capture_queue_manifest_{run_tag}.json")

    manifest = {
        "ok": True,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "repo_root": str(REPO_ROOT),
        "schedule_file": resolved["schedule_file"],
        "routes_file": str(routes_file.resolve()),
        "output_dir": str(output_dir.resolve()),
        "run_tag": run_tag,
        "cabin": args.cabin,
        "dates_resolved_count": len(resolved["dates"]),
        "dates_resolved": resolved["dates"],
        "merged_schedule_scrape_defaults": resolved["merged_schedule_scrape_defaults"],
        "bs2a_queue_file": str(bs2a_queue_path.resolve()),
        "q2_queue_file": str(q2_queue_path.resolve()),
        "protected_source_queue_file": str(protected_queue_path.resolve()),
        "bs2a_jobs_count": len(bs2a_jobs),
        "q2_jobs_count": len(q2_jobs),
        "protected_source_jobs_count": len(protected_jobs),
        "bs2a_routes_count": len([r for r in routes if r["airline"] in {"BS", "2A"}]),
        "q2_routes_count": len([r for r in routes if r["airline"] == "Q2"]),
        "protected_source_routes_count": len([r for r in routes if r["airline"] in {"BS", "2A", "Q2", "G9", "OV"}]),
        "dry_run": bool(args.dry_run),
    }

    if not args.dry_run:
        if not args.no_bs2a:
            _write_json(bs2a_queue_path, bs2a_jobs)
        if not args.no_q2:
            _write_json(q2_queue_path, q2_jobs)
        if not args.no_protected:
            _write_json(protected_queue_path, protected_jobs)
        _write_json(result_out, manifest)

    print(
        f"[queue-builder] dates_resolved={manifest['dates_resolved_count']} "
        f"bs2a_jobs={manifest['bs2a_jobs_count']} q2_jobs={manifest['q2_jobs_count']} "
        f"protected_source_jobs={manifest['protected_source_jobs_count']}"
    )
    print(f"[queue-builder] schedule={manifest['schedule_file']}")
    print(f"[queue-builder] routes={manifest['routes_file']}")
    if args.dry_run:
        print("[queue-builder] dry-run; no files written")
    else:
        if not args.no_bs2a:
            print(f"[queue-builder] wrote bs2a queue: {bs2a_queue_path}")
        if not args.no_q2:
            print(f"[queue-builder] wrote q2 queue: {q2_queue_path}")
        if not args.no_protected:
            print(f"[queue-builder] wrote protected-source queue: {protected_queue_path}")
        print(f"[queue-builder] wrote manifest: {result_out}")

    print(json.dumps(manifest, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
