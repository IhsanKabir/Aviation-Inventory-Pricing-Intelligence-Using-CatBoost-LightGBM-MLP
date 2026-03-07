from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from google.cloud import bigquery

REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = REPO_ROOT / "sql" / "bigquery"


def _split_sql_script(content: str) -> list[str]:
    parts = [part.strip() for part in content.split(";")]
    return [part for part in parts if part]


def _read_sql(path: Path) -> list[str]:
    return _split_sql_script(path.read_text(encoding="utf-8"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create BigQuery dataset and curated tables from repo SQL files.")
    parser.add_argument("--project-id", default=os.getenv("BIGQUERY_PROJECT_ID", "aeropulseintelligence"))
    parser.add_argument("--dataset", default=os.getenv("BIGQUERY_DATASET", "aviation_intel"))
    parser.add_argument("--run-table-ddl", action="store_true", help="Also create curated tables after dataset creation.")
    parser.add_argument("--run-view-ddl", action="store_true", help="Also create Looker-facing views after table creation.")
    parser.add_argument("--replace-tables", action="store_true", help="Use CREATE OR REPLACE TABLE for curated tables.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    client = bigquery.Client(project=args.project_id)

    dataset_sql_path = SQL_DIR / "create_aviation_intel_dataset.sql"
    table_sql_path = SQL_DIR / "create_aviation_intel_tables.sql"
    view_sql_path = SQL_DIR / "create_aviation_intel_looker_views.sql"

    statements = _read_sql(dataset_sql_path)
    if args.run_table_ddl:
        statements.extend(_read_sql(table_sql_path))
    if args.run_view_ddl:
        statements.extend(_read_sql(view_sql_path))

    for statement in statements:
        rendered = (
            statement.replace("`aeropulseintelligence.aviation_intel`", f"`{args.project_id}.{args.dataset}`")
            .replace("`aeropulseintelligence.aviation_intel.", f"`{args.project_id}.{args.dataset}.")
        )
        if args.replace_tables:
            rendered = rendered.replace("CREATE TABLE IF NOT EXISTS", "CREATE OR REPLACE TABLE")
        job = client.query(rendered)
        job.result()
        print(f"OK: {rendered.splitlines()[0][:120]}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
