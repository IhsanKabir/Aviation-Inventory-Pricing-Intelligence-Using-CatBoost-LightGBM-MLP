"""add extraction attempts telemetry

Revision ID: 45ef31c8a9f2
Revises: ce8cc3cd3452
Create Date: 2026-04-27
"""

from alembic import op
import sqlalchemy as sa


revision = "45ef31c8a9f2"
down_revision = "ce8cc3cd3452"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "extraction_attempts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("scrape_id", sa.String(), nullable=False),
        sa.Column("cycle_id", sa.String(), nullable=True),
        sa.Column("query_key", sa.String(), nullable=True),
        sa.Column("airline", sa.String(), nullable=False),
        sa.Column("module_name", sa.String(), nullable=True),
        sa.Column("source_family", sa.String(), nullable=True),
        sa.Column("final_source", sa.String(), nullable=True),
        sa.Column("fallback_used", sa.Boolean(), nullable=True, server_default=sa.false()),
        sa.Column("origin", sa.String(), nullable=True),
        sa.Column("destination", sa.String(), nullable=True),
        sa.Column("departure_date", sa.String(), nullable=True),
        sa.Column("return_date", sa.String(), nullable=True),
        sa.Column("trip_type", sa.String(), nullable=True),
        sa.Column("cabin", sa.String(), nullable=True),
        sa.Column("adt_count", sa.Integer(), nullable=True),
        sa.Column("chd_count", sa.Integer(), nullable=True),
        sa.Column("inf_count", sa.Integer(), nullable=True),
        sa.Column("ok", sa.Boolean(), nullable=True, server_default=sa.false()),
        sa.Column("row_count", sa.Integer(), nullable=True, server_default="0"),
        sa.Column("inserted_core_count", sa.Integer(), nullable=True, server_default="0"),
        sa.Column("inserted_raw_meta_count", sa.Integer(), nullable=True, server_default="0"),
        sa.Column("raw_meta_matched", sa.Integer(), nullable=True, server_default="0"),
        sa.Column("raw_meta_unmatched", sa.Integer(), nullable=True, server_default="0"),
        sa.Column("raw_meta_match_modes", sa.JSON(), nullable=True),
        sa.Column("elapsed_sec", sa.Float(), nullable=True),
        sa.Column("error_class", sa.String(), nullable=True),
        sa.Column("no_rows_reason", sa.String(), nullable=True),
        sa.Column("manual_action_required", sa.Boolean(), nullable=True, server_default=sa.false()),
        sa.Column("retry_recommended", sa.Boolean(), nullable=True, server_default=sa.false()),
        sa.Column("capture_state", sa.JSON(), nullable=True),
        sa.Column("session_state", sa.JSON(), nullable=True),
        sa.Column("source_attempts", sa.JSON(), nullable=True),
        sa.Column("meta", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_extraction_attempts_scrape_id", "extraction_attempts", ["scrape_id"])
    op.create_index("ix_extraction_attempts_cycle_id", "extraction_attempts", ["cycle_id"])
    op.create_index("ix_extraction_attempts_query_key", "extraction_attempts", ["query_key"])
    op.create_index("ix_extraction_attempts_airline", "extraction_attempts", ["airline"])
    op.create_index("ix_extraction_attempts_module_name", "extraction_attempts", ["module_name"])
    op.create_index("ix_extraction_attempts_source_family", "extraction_attempts", ["source_family"])
    op.create_index("ix_extraction_attempts_origin", "extraction_attempts", ["origin"])
    op.create_index("ix_extraction_attempts_destination", "extraction_attempts", ["destination"])
    op.create_index("ix_extraction_attempts_departure_date", "extraction_attempts", ["departure_date"])
    op.create_index("ix_extraction_attempts_error_class", "extraction_attempts", ["error_class"])
    op.create_index("ix_extraction_attempts_manual_action_required", "extraction_attempts", ["manual_action_required"])
    op.create_index("ix_extraction_attempts_scrape_airline", "extraction_attempts", ["scrape_id", "airline"])
    op.create_index(
        "ix_extraction_attempts_route_window",
        "extraction_attempts",
        ["airline", "origin", "destination", "departure_date", "cabin"],
    )
    op.create_index(
        "ix_extraction_attempts_gate",
        "extraction_attempts",
        ["scrape_id", "error_class", "manual_action_required"],
    )


def downgrade():
    op.drop_table("extraction_attempts")
