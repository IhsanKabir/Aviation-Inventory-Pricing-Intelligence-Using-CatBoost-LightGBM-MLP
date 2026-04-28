import os
import unittest
from datetime import date
from types import SimpleNamespace

import run_pipeline
from tools import bigquery_apply_retention, bigquery_storage_audit, export_bigquery_stage


class BigQueryCostControlTests(unittest.TestCase):
    def test_export_window_uses_exclusive_end_date(self):
        start_day, end_day, start_ts, end_ts = export_bigquery_stage._parse_export_window(
            "2026-03-01",
            "2026-03-03",
        )

        self.assertEqual(date(2026, 3, 1), start_day)
        self.assertEqual(date(2026, 3, 3), end_day)
        self.assertEqual("2026-03-01T00:00:00+00:00", start_ts)
        self.assertEqual("2026-03-03T00:00:00+00:00", end_ts)

    def test_partition_refresh_replaces_dimensions_only(self):
        self.assertEqual(
            export_bigquery_stage.LOAD_MODE_REPLACE,
            export_bigquery_stage._effective_load_mode("dim_airline", "partition-refresh"),
        )
        self.assertEqual(
            export_bigquery_stage.LOAD_MODE_PARTITION_REFRESH,
            export_bigquery_stage._effective_load_mode("fact_offer_snapshot", "partition-refresh"),
        )

    def test_partition_refresh_delete_sql_targets_export_window(self):
        offer_sql = export_bigquery_stage._build_partition_delete_sql(
            "aeropulseintelligence",
            "aviation_intel",
            "fact_offer_snapshot",
        )
        change_sql = export_bigquery_stage._build_partition_delete_sql(
            "aeropulseintelligence",
            "aviation_intel",
            "fact_change_event",
        )

        self.assertIn("DATE(captured_at_utc) >= @partition_start_date", offer_sql)
        self.assertIn("DATE(captured_at_utc) < @partition_end_date", offer_sql)
        self.assertIn("report_day >= @partition_start_date", change_sql)
        self.assertIn("report_day < @partition_end_date", change_sql)

    def test_run_pipeline_requires_explicit_bigquery_sync_enabled(self):
        disabled = SimpleNamespace(
            bigquery_sync_enabled=False,
            bigquery_project_id="aeropulseintelligence",
            bigquery_dataset="aviation_intel",
        )
        enabled = SimpleNamespace(
            bigquery_sync_enabled=True,
            bigquery_project_id="aeropulseintelligence",
            bigquery_dataset="aviation_intel",
        )

        self.assertFalse(run_pipeline._bigquery_sync_is_configured(disabled))
        self.assertTrue(run_pipeline._bigquery_sync_is_configured(enabled))

    def test_run_pipeline_bigquery_sync_env_flag_parsing(self):
        old_value = os.environ.get("BIGQUERY_SYNC_ENABLED")
        try:
            os.environ["BIGQUERY_SYNC_ENABLED"] = "1"
            self.assertTrue(run_pipeline._env_flag_enabled("BIGQUERY_SYNC_ENABLED"))
            os.environ["BIGQUERY_SYNC_ENABLED"] = "false"
            self.assertFalse(run_pipeline._env_flag_enabled("BIGQUERY_SYNC_ENABLED"))
        finally:
            if old_value is None:
                os.environ.pop("BIGQUERY_SYNC_ENABLED", None)
            else:
                os.environ["BIGQUERY_SYNC_ENABLED"] = old_value

    def test_run_pipeline_passes_load_mode_to_exporter(self):
        args = SimpleNamespace(
            python_exe="python",
            bigquery_sync_output_dir="output/warehouse/bigquery",
            bigquery_sync_lookback_days=2,
            bigquery_project_id="aeropulseintelligence",
            bigquery_dataset="aviation_intel",
            bigquery_load_mode="partition-refresh",
        )

        cmd = run_pipeline.build_bigquery_sync_cmd(args)

        self.assertIn("--load-mode", cmd)
        self.assertEqual("partition-refresh", cmd[cmd.index("--load-mode") + 1])

    def test_retention_statements_encode_hot_cache_policy(self):
        statements = bigquery_apply_retention.build_retention_statements(
            "aeropulseintelligence",
            "aviation_intel",
            hot_days=35,
            forecast_days=90,
            time_travel_hours=48,
        )
        sql = "\n".join(statements)

        self.assertIn("default_partition_expiration_days = 35", sql)
        self.assertIn("max_time_travel_hours = 48", sql)
        self.assertIn("fact_offer_snapshot", sql)
        self.assertNotIn("fact_gds_fare_snapshot", sql)
        self.assertIn("partition_expiration_days = 35", sql)
        self.assertIn("fact_forecast_next_day", sql)
        self.assertNotIn("fact_forecast_route_winner", sql)
        self.assertNotIn("fact_backtest_route_winner", sql)
        self.assertIn("partition_expiration_days = 90", sql)

    def test_storage_audit_warns_for_old_high_volume_partitions(self):
        warnings = bigquery_storage_audit.build_retention_warnings(
            [
                {
                    "table_name": "fact_offer_snapshot",
                    "oldest_partition_id": "20260301",
                },
                {
                    "table_name": "fact_forecast_next_day",
                    "oldest_partition_id": "20260301",
                },
            ],
            hot_retention_days=35,
            today=date(2026, 4, 27),
        )

        self.assertEqual(1, len(warnings))
        self.assertIn("fact_offer_snapshot", warnings[0])

    def test_storage_audit_uses_region_qualified_table_storage(self):
        sql = bigquery_storage_audit.build_storage_query(
            "aeropulseintelligence",
            "aviation_intel",
            "asia-south1",
        )

        self.assertIn("`aeropulseintelligence.region-asia-south1.INFORMATION_SCHEMA.TABLE_STORAGE`", sql)
        self.assertIn("table_schema = @dataset", sql)

    def test_storage_audit_builds_enable_statement(self):
        sql = bigquery_storage_audit.build_enable_table_storage_statement(
            "aeropulseintelligence",
            "asia-south1",
        )

        self.assertIn("ALTER PROJECT `aeropulseintelligence`", sql)
        self.assertIn("`region-asia-south1.enable_info_schema_storage` = TRUE", sql)


if __name__ == "__main__":
    unittest.main()
