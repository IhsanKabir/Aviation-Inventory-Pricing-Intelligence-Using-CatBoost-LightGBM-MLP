"""
Build thesis-ready output pack (P2-D).

The pack includes:
- Copied latest core artifacts (reports, prediction eval/backtest, alert quality, ops health)
- Consolidated thesis tables
- Chapter-ready markdown summary (methodology + key results)
- Reproducibility manifest with checksums
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


TS_RE = re.compile(r"_(\d{8}_\d{6})")


@dataclass
class Artifact:
    category: str
    key: str
    path: Path


def parse_args():
    p = argparse.ArgumentParser(description="Build thesis-ready output pack")
    p.add_argument("--reports-dir", default="output/reports", help="Root reports directory")
    p.add_argument("--output-dir", default="output/reports", help="Where thesis_pack_<ts> is created")
    p.add_argument("--pack-prefix", default="thesis_pack", help="Output folder prefix")
    p.add_argument("--timestamp-tz", choices=["local", "utc"], default="local")
    p.add_argument("--zip", action="store_true", help="Also create zip archive of the pack")
    return p.parse_args()


def now_stamp(tz_mode: str) -> str:
    if tz_mode == "utc":
        ts = datetime.now(timezone.utc)
    else:
        ts = datetime.now().astimezone()
    return ts.strftime("%Y%m%d_%H%M%S")


def parse_file_ts(path: Path) -> Optional[datetime]:
    m = TS_RE.search(path.name)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y%m%d_%H%M%S")
    except ValueError:
        return None


def pick_latest(paths: List[Path]) -> Optional[Path]:
    if not paths:
        return None

    def sort_key(p: Path):
        ts = parse_file_ts(p)
        if ts is not None:
            return (1, ts, p.stat().st_mtime)
        return (0, datetime.fromtimestamp(0), p.stat().st_mtime)

    return sorted(paths, key=sort_key)[-1]


def latest_run_dir(reports_dir: Path) -> Optional[Path]:
    candidates = [p for p in reports_dir.iterdir() if p.is_dir() and p.name.startswith("run_")]
    if not candidates:
        return None
    return sorted(candidates, key=lambda p: p.stat().st_mtime)[-1]


def discover_artifacts(reports_dir: Path) -> List[Artifact]:
    artifacts: List[Artifact] = []

    # Ops health
    ops_latest = reports_dir / "ops_health_latest.md"
    if ops_latest.exists():
        artifacts.append(Artifact("ops", "ops_health_latest", ops_latest))
    else:
        md_candidates = list(reports_dir.glob("ops_health_*.md"))
        txt_candidates = list(reports_dir.glob("ops_health_*.txt"))
        picked = pick_latest(md_candidates) or pick_latest(txt_candidates)
        if picked:
            artifacts.append(Artifact("ops", "ops_health_latest", picked))

    # Alert quality
    for key, pattern in (
        ("alert_quality_overall", "alert_quality_overall_*.csv"),
        ("alert_quality_by_route", "alert_quality_by_route_*.csv"),
        ("alert_quality_daily", "alert_quality_daily_*.csv"),
    ):
        picked = pick_latest(list(reports_dir.glob(pattern)))
        if picked:
            artifacts.append(Artifact("alerts", key, picked))

    # Prediction artifacts (latest per target per artifact type)
    pred_types = [
        ("prediction_eval", "prediction_eval_*_*.csv"),
        ("prediction_eval_by_route", "prediction_eval_by_route_*_*.csv"),
        ("prediction_backtest_eval", "prediction_backtest_eval_*_*.csv"),
        ("prediction_backtest_splits", "prediction_backtest_splits_*_*.csv"),
        ("prediction_backtest_meta", "prediction_backtest_meta_*_*.json"),
        ("prediction_next_day", "prediction_next_day_*_*.csv"),
        ("prediction_trend", "prediction_trend_*_*.csv"),
        ("prediction_history", "prediction_history_*_*.csv"),
    ]

    for key, pattern in pred_types:
        by_target: Dict[str, List[Path]] = {}
        for path in reports_dir.glob(pattern):
            target = extract_target(path.name, key)
            if not target:
                continue
            by_target.setdefault(target, []).append(path)
        for target, candidates in by_target.items():
            if key == "prediction_eval" and target.startswith("by_route_"):
                continue
            picked = pick_latest(candidates)
            if picked:
                artifacts.append(Artifact("predictions", f"{key}:{target}", picked))

    # Latest run core outputs
    run_dir = latest_run_dir(reports_dir)
    if run_dir:
        run_patterns = {
            "route_airline_summary": "route_airline_summary_*.csv",
            "price_changes_daily": "price_changes_daily_*.csv",
            "availability_changes_daily": "availability_changes_daily_*.csv",
            "data_quality_report": "data_quality_report_*.csv",
            "dashboard_xlsx": "airline_intel_dashboard_*.xlsx",
        }
        for key, pattern in run_patterns.items():
            picked = pick_latest(list(run_dir.glob(pattern)))
            if picked:
                artifacts.append(Artifact("core_reports", key, picked))

    # Latest run metadata pointers
    for key, filename in (
        ("latest_run_json", "latest_run.json"),
        ("latest_run_txt", "latest_run.txt"),
    ):
        p = reports_dir / filename
        if p.exists():
            artifacts.append(Artifact("ops", key, p))

    return artifacts


def extract_target(filename: str, pred_type: str) -> Optional[str]:
    prefix = f"{pred_type}_"
    if not filename.startswith(prefix):
        return None
    stem = filename.rsplit(".", 1)[0]
    m = re.match(rf"^{re.escape(prefix)}(.+)_\d{{8}}_\d{{6}}$", stem)
    if not m:
        return None
    return m.group(1)


def copy_artifacts(artifacts: List[Artifact], pack_dir: Path) -> List[Dict[str, str]]:
    copied: List[Dict[str, str]] = []
    raw_dir = pack_dir / "raw"
    for art in artifacts:
        out_dir = raw_dir / art.category
        out_dir.mkdir(parents=True, exist_ok=True)
        dst = out_dir / art.path.name
        shutil.copy2(art.path, dst)
        copied.append(
            {
                "category": art.category,
                "key": art.key,
                "source": str(art.path),
                "copied_to": str(dst),
            }
        )
    return copied


def _to_float(v) -> Optional[float]:
    try:
        if pd.isna(v):
            return None
        return float(v)
    except Exception:
        return None


def build_prediction_table(artifacts: List[Artifact], tables_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    eval_rows = []
    backtest_rows = []

    pred_eval = [a for a in artifacts if a.key.startswith("prediction_eval:")]
    pred_backtest = [a for a in artifacts if a.key.startswith("prediction_backtest_eval:")]

    for art in pred_eval:
        target = art.key.split(":", 1)[1]
        try:
            df = pd.read_csv(art.path)
            if df.empty or "model" not in df.columns:
                continue
            work = df.copy()
            if "mae" in work.columns:
                work["mae"] = pd.to_numeric(work["mae"], errors="coerce")
                work = work.dropna(subset=["mae"])
                if work.empty:
                    continue
                best = work.sort_values("mae", ascending=True).iloc[0]
            else:
                best = work.iloc[0]
            eval_rows.append(
                {
                    "target": target,
                    "best_model_by_mae": best.get("model"),
                    "n": best.get("n"),
                    "mae": best.get("mae"),
                    "rmse": best.get("rmse"),
                    "smape_pct": best.get("smape_pct"),
                    "directional_accuracy_pct": best.get("directional_accuracy_pct"),
                    "f1_macro": best.get("f1_macro"),
                    "source_file": art.path.name,
                }
            )
        except Exception:
            continue

    for art in pred_backtest:
        target = art.key.split(":", 1)[1]
        try:
            df = pd.read_csv(art.path)
            if df.empty:
                continue
            if "dataset" in df.columns:
                df = df[df["dataset"].astype(str).str.lower() == "test"]
            if "selected_on_val" in df.columns:
                selected = df[df["selected_on_val"].astype(str).str.lower() == "true"]
                if not selected.empty:
                    df = selected
            if df.empty:
                continue

            for c in ("mae", "rmse", "smape_pct", "directional_accuracy_pct", "f1_macro"):
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")

            backtest_rows.append(
                {
                    "target": target,
                    "rows_used": int(len(df)),
                    "mean_mae": _to_float(df.get("mae").mean() if "mae" in df.columns else None),
                    "mean_rmse": _to_float(df.get("rmse").mean() if "rmse" in df.columns else None),
                    "mean_smape_pct": _to_float(df.get("smape_pct").mean() if "smape_pct" in df.columns else None),
                    "mean_directional_accuracy_pct": _to_float(
                        df.get("directional_accuracy_pct").mean() if "directional_accuracy_pct" in df.columns else None
                    ),
                    "mean_f1_macro": _to_float(df.get("f1_macro").mean() if "f1_macro" in df.columns else None),
                    "source_file": art.path.name,
                }
            )
        except Exception:
            continue

    eval_df = pd.DataFrame(eval_rows).sort_values("target") if eval_rows else pd.DataFrame()
    backtest_df = pd.DataFrame(backtest_rows).sort_values("target") if backtest_rows else pd.DataFrame()

    if not eval_df.empty:
        eval_df.to_csv(tables_dir / "table_prediction_best_models.csv", index=False)
    if not backtest_df.empty:
        backtest_df.to_csv(tables_dir / "table_backtest_test_summary.csv", index=False)
    return eval_df, backtest_df


def build_alert_table(artifacts: List[Artifact], tables_dir: Path) -> pd.DataFrame:
    alert = next((a for a in artifacts if a.key == "alert_quality_overall"), None)
    if not alert:
        return pd.DataFrame()
    try:
        df = pd.read_csv(alert.path)
        if not df.empty:
            df.to_csv(tables_dir / "table_alert_quality_overall.csv", index=False)
        return df
    except Exception:
        return pd.DataFrame()


def build_data_quality_table(artifacts: List[Artifact], tables_dir: Path) -> pd.DataFrame:
    dq = next((a for a in artifacts if a.key == "data_quality_report"), None)
    if not dq:
        return pd.DataFrame()
    try:
        df = pd.read_csv(dq.path)
        if df.empty:
            return df
        pivot = {}
        for _, row in df.iterrows():
            metric = str(row.get("metric", "")).strip()
            value = row.get("value")
            if metric:
                pivot[metric] = value

        selected = pd.DataFrame(
            [
                {"metric": "total_rows", "value": pivot.get("total_rows")},
                {"metric": "scrape_count", "value": pivot.get("scrape_count")},
                {"metric": "observed_route_count", "value": pivot.get("observed_route_count")},
                {"metric": "duplicate_row_rate_pct", "value": pivot.get("duplicate_row_rate_pct")},
                {"metric": "raw_meta_coverage_pct", "value": pivot.get("raw_meta_coverage_pct")},
                {"metric": "seat_capacity_null_rate_pct", "value": pivot.get("seat_capacity_null_rate_pct")},
            ]
        )
        selected.to_csv(tables_dir / "table_data_quality_snapshot.csv", index=False)
        return selected
    except Exception:
        return pd.DataFrame()


def parse_ops_health(artifacts: List[Artifact]) -> Dict[str, Optional[str]]:
    ops = next((a for a in artifacts if a.key == "ops_health_latest"), None)
    out = {
        "status": None,
        "events_in_window": None,
        "scheduler_run_signals": None,
        "row_emission_signals": None,
        "time_range": None,
        "source_file": None,
    }
    if not ops:
        return out

    out["source_file"] = ops.path.name
    try:
        txt = ops.path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return out

    for line in txt.splitlines():
        line = line.strip()
        if line.startswith("- Status:"):
            out["status"] = line.split(":", 1)[1].strip().replace("*", "")
        elif line.startswith("- Events in window:"):
            out["events_in_window"] = line.split(":", 1)[1].strip()
        elif line.startswith("- Scheduler run signals:"):
            out["scheduler_run_signals"] = line.split(":", 1)[1].strip()
        elif line.startswith("- Row-emission signals:"):
            out["row_emission_signals"] = line.split(":", 1)[1].strip()
        elif line.startswith("- Time range:"):
            out["time_range"] = line.split(":", 1)[1].strip()
    return out


def write_inventory(copied: List[Dict[str, str]], tables_dir: Path):
    if not copied:
        return
    keys = ["category", "key", "source", "copied_to"]
    path = tables_dir / "table_artifact_inventory.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(copied)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_manifest(pack_dir: Path, copied: List[Dict[str, str]], args, pack_ts: str):
    manifest = {
        "pack_name": pack_dir.name,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "timestamp_label": pack_ts,
        "generator": "tools/build_thesis_pack.py",
        "args": {
            "reports_dir": args.reports_dir,
            "output_dir": args.output_dir,
            "pack_prefix": args.pack_prefix,
            "timestamp_tz": args.timestamp_tz,
            "zip": bool(args.zip),
        },
        "files": [],
    }

    for rel in sorted(pack_dir.rglob("*")):
        if rel.is_dir():
            continue
        manifest["files"].append(
            {
                "path": str(rel.relative_to(pack_dir)),
                "size_bytes": rel.stat().st_size,
                "sha256": sha256_file(rel),
            }
        )

    manifest["copied_sources"] = copied
    out = pack_dir / "manifest.json"
    out.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def write_summary(
    pack_dir: Path,
    eval_df: pd.DataFrame,
    backtest_df: pd.DataFrame,
    alert_df: pd.DataFrame,
    dq_df: pd.DataFrame,
    ops_summary: Dict[str, Optional[str]],
):
    def _fmt(v):
        if v is None:
            return "NA"
        try:
            if pd.isna(v):
                return "NA"
        except Exception:
            pass
        return str(v)

    lines: List[str] = []
    lines.append("# Thesis-Ready Output Pack")
    lines.append("")
    lines.append("## Scope")
    lines.append("- This package bundles the latest reproducible outputs for forecasting, backtesting, alert evaluation, data quality, and operations health.")
    lines.append("- It is generated from repository artifacts under `output/reports` without modifying scheduler flow.")
    lines.append("")

    lines.append("## Methodology Summary")
    lines.append("- Forecast baselines: persistence, rolling mean, seasonal naive, EWMA.")
    lines.append("- Validation protocol: rolling-window backtest with fixed split metadata.")
    lines.append("- Alert evaluation: precision/recall/F1 plus false-alarm and missed-event cost tracking.")
    lines.append("- Data quality gates: duplicate rates, null rates, and raw metadata coverage.")
    lines.append("")

    lines.append("## Key Results Snapshot")
    if not eval_df.empty:
        lines.append("- Forecast best-model summary saved at `tables/table_prediction_best_models.csv`.")
    else:
        lines.append("- Forecast best-model summary: unavailable (no matching prediction eval files).")

    if not backtest_df.empty:
        lines.append("- Backtest test-set summary saved at `tables/table_backtest_test_summary.csv`.")
    else:
        lines.append("- Backtest summary: unavailable (no matching backtest files).")

    if not alert_df.empty:
        lines.append("- Alert quality summary saved at `tables/table_alert_quality_overall.csv`.")
        for _, r in alert_df.iterrows():
            lines.append(
                f"- Alert `{_fmt(r.get('alert_type'))}`: precision={_fmt(r.get('precision'))}, "
                f"recall={_fmt(r.get('recall'))}, f1={_fmt(r.get('f1'))}, total_cost={_fmt(r.get('total_cost'))}."
            )
    else:
        lines.append("- Alert quality summary: unavailable.")

    if not dq_df.empty:
        lines.append("- Data quality snapshot saved at `tables/table_data_quality_snapshot.csv`.")
    else:
        lines.append("- Data quality snapshot: unavailable.")

    lines.append("")
    lines.append("## Ops Health (P1-E Tracking)")
    lines.append(f"- Status: {ops_summary.get('status')}")
    lines.append(f"- Events in window: {ops_summary.get('events_in_window')}")
    lines.append(f"- Scheduler run signals: {ops_summary.get('scheduler_run_signals')}")
    lines.append(f"- Row-emission signals: {ops_summary.get('row_emission_signals')}")
    lines.append(f"- Time range: {ops_summary.get('time_range')}")
    lines.append("")
    lines.append("## Reproducibility")
    lines.append("- Full file inventory and checksums are in `manifest.json`.")
    lines.append("- Source-to-pack file mapping is in `tables/table_artifact_inventory.csv`.")
    lines.append("")

    (pack_dir / "thesis_summary.md").write_text("\n".join(lines), encoding="utf-8")


def main():
    args = parse_args()
    reports_dir = Path(args.reports_dir)
    output_dir = Path(args.output_dir)
    if not reports_dir.exists():
        raise SystemExit(f"reports-dir not found: {reports_dir}")

    pack_ts = now_stamp(args.timestamp_tz)
    pack_dir = output_dir / f"{args.pack_prefix}_{pack_ts}"
    tables_dir = pack_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)

    artifacts = discover_artifacts(reports_dir)
    copied = copy_artifacts(artifacts, pack_dir)

    eval_df, backtest_df = build_prediction_table(artifacts, tables_dir)
    alert_df = build_alert_table(artifacts, tables_dir)
    dq_df = build_data_quality_table(artifacts, tables_dir)
    ops_summary = parse_ops_health(artifacts)
    write_inventory(copied, tables_dir)
    write_summary(pack_dir, eval_df, backtest_df, alert_df, dq_df, ops_summary)
    write_manifest(pack_dir, copied, args, pack_ts)

    zip_path = None
    if args.zip:
        zip_path = shutil.make_archive(str(pack_dir), "zip", root_dir=pack_dir)

    print(f"pack_dir={pack_dir}")
    print(f"artifacts_copied={len(copied)}")
    if not eval_df.empty:
        print(f"prediction_targets={len(eval_df)}")
    if zip_path:
        print(f"pack_zip={zip_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
