"""add noaa gsod backfill progress table

Revision ID: 0010_noaa_gsod_backfill_progress
Revises: 0009_backfill_progress
Create Date: 2026-03-03
"""

from alembic import op
import sqlalchemy as sa


revision = "0010_noaa_gsod_backfill_progress"
down_revision = "0009_backfill_progress"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "noaa_backfill_progress",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("month", sa.Date(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_msg", sa.Text(), nullable=True),
        sa.Column("rows_written", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("stations_total", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("stations_success", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("stations_failed", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("strong_wind_proxy_used", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("duration_sec", sa.Float(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index(op.f("ix_noaa_backfill_progress_run_id"), "noaa_backfill_progress", ["run_id"], unique=False)
    op.create_index(op.f("ix_noaa_backfill_progress_month"), "noaa_backfill_progress", ["month"], unique=False)
    op.create_index(op.f("ix_noaa_backfill_progress_status"), "noaa_backfill_progress", ["status"], unique=False)
    op.create_index(op.f("ix_noaa_backfill_progress_updated_at"), "noaa_backfill_progress", ["updated_at"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_noaa_backfill_progress_updated_at"), table_name="noaa_backfill_progress")
    op.drop_index(op.f("ix_noaa_backfill_progress_status"), table_name="noaa_backfill_progress")
    op.drop_index(op.f("ix_noaa_backfill_progress_month"), table_name="noaa_backfill_progress")
    op.drop_index(op.f("ix_noaa_backfill_progress_run_id"), table_name="noaa_backfill_progress")
    op.drop_table("noaa_backfill_progress")
