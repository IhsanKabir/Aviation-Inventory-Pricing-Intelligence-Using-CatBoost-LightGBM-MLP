"""
Model drift monitor using event-volume forecast residuals.

Uses a simple route/cabin daily event series from column_change_events:
- naive forecast = previous day's value
- computes MAE over lookback
- compares recent-window MAE vs prior-window MAE
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from core.runtime_config import get_database_url


def parse_args():
    p = argparse.ArgumentParser(description="Monitor drift in event forecast residuals")
    p.add_argument("--db-url", default=get_database_url())
    p.add_argument("--reports-dir", default="output/reports")
    p.add_argument("--lookback-days", type=int, default=28)
    p.add_argument("--recent-window-days", type=int, default=7)
    p.add_argument("--drift-ratio-threshold", type=float, default=1.35, help="recent_mae / prior_mae threshold")
    p.add_argument("--min-points", type=int, default=8)
    p.add_argument("--strict", action="store_true")
    p.add_argument("--timestamp-tz", choices=["local", "utc"], default="local")
    return p.parse_args()


def _now(tz_mode: str):
    return datetime.now(timezone.utc) if tz_mode == "utc" else datetime.now().astimezone()


def _stamp(now: datetime):
    return now.strftime("%Y%m%d_%H%M%S")


def _load_series(db_url: str, lookback_days: int):
    eng = create_engine(db_url, pool_pre_ping=True, future=True)
    q = text(
        """
        SELECT
            airline,
            origin,
            destination,
            cabin,
            DATE(detected_at) AS day,
            COUNT(*)::int AS events
        FROM airline_intel.column_change_events
        WHERE detected_at >= NOW() - (:lookback_days || ' days')::interval
        GROUP BY airline, origin, destination, cabin, DATE(detected_at)
        ORDER BY day
        """
    )
    with eng.connect() as conn:
        rows = conn.execute(q, {"lookback_days": int(lookback_days)}).mappings().all()
    return pd.DataFrame(rows)


def _compute(df: pd.DataFrame, recent_window_days: int, min_points: int, drift_ratio_threshold: float):
    if df.empty:
        return {
            "overall_status": "WARN",
            "group_count": 0,
            "drift_groups": [],
            "rows": [],
        }

    out_rows = []
    group_cols = ["airline", "origin", "destination", "cabin"]
    for key, g in df.groupby(group_cols):
        g = g.sort_values("day").copy()
        g["forecast"] = g["events"].shift(1)
        g = g.dropna(subset=["forecast"])
        if len(g) < min_points:
            continue
        g["abs_error"] = (g["events"] - g["forecast"]).abs()
        recent = g.tail(recent_window_days)
        prior = g.iloc[max(0, len(g) - 2 * recent_window_days) : max(0, len(g) - recent_window_days)]
        recent_mae = float(recent["abs_error"].mean()) if not recent.empty else 0.0
        prior_mae = float(prior["abs_error"].mean()) if not prior.empty else 0.0
        ratio = (recent_mae / prior_mae) if prior_mae > 0 else (999.0 if recent_mae > 0 else 1.0)
        drift_flag = ratio >= drift_ratio_threshold
        out_rows.append(
            {
                "airline": key[0],
                "origin": key[1],
                "destination": key[2],
                "cabin": key[3],
                "points": int(len(g)),
                "recent_mae": round(recent_mae, 4),
                "prior_mae": round(prior_mae, 4),
                "drift_ratio": round(ratio, 4),
                "drift_flag": bool(drift_flag),
            }
        )

    drift_groups = [r for r in out_rows if r["drift_flag"]]
    status = "PASS"
    if out_rows and len(drift_groups) >= max(1, int(0.2 * len(out_rows))):
        status = "WARN"
    if out_rows and len(drift_groups) >= max(2, int(0.4 * len(out_rows))):
        status = "FAIL"
    return {
        "overall_status": status,
        "group_count": len(out_rows),
        "drift_groups": drift_groups,
        "rows": out_rows,
    }


def main():
    args = parse_args()
    now = _now(args.timestamp_tz)
    ts = _stamp(now)
    out_dir = Path(args.reports_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = _load_series(args.db_url, args.lookback_days)
    result = _compute(df, args.recent_window_days, args.min_points, args.drift_ratio_threshold)
    payload = {
        "generated_at": now.isoformat(),
        "lookback_days": args.lookback_days,
        "recent_window_days": args.recent_window_days,
        "drift_ratio_threshold": args.drift_ratio_threshold,
        **result,
    }

    latest_json = out_dir / "model_drift_latest.json"
    run_json = out_dir / f"model_drift_{ts}.json"
    latest_md = out_dir / "model_drift_latest.md"
    run_md = out_dir / f"model_drift_{ts}.md"
    latest_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    run_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        "# Model Drift Monitor",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Status: **{payload['overall_status']}**",
        f"- Groups evaluated: `{payload['group_count']}`",
        f"- Drift groups: `{len(payload['drift_groups'])}`",
        "",
        "## Top Drift Groups",
        "",
    ]
    top = sorted(payload["drift_groups"], key=lambda r: r["drift_ratio"], reverse=True)[:20]
    if top:
        for r in top:
            lines.append(
                f"- `{r['airline']}:{r['origin']}-{r['destination']}:{r['cabin']}` "
                f"ratio={r['drift_ratio']} recent_mae={r['recent_mae']} prior_mae={r['prior_mae']}"
            )
    else:
        lines.append("- none")
    md_text = "\n".join(lines) + "\n"
    latest_md.write_text(md_text, encoding="utf-8")
    run_md.write_text(md_text, encoding="utf-8")

    print(f"drift_status={payload['overall_status']} groups={payload['group_count']} drift_groups={len(payload['drift_groups'])}")
    print(f"latest_json={latest_json}")
    print(f"latest_md={latest_md}")
    if args.strict and payload["overall_status"] == "FAIL":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
