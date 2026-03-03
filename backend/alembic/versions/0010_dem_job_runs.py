"""add dem job runs table

Revision ID: 0010_dem_job_runs
Revises: 0009_backfill_progress
Create Date: 2026-03-03
"""

from alembic import op
import sqlalchemy as sa


revision = "0010_dem_job_runs"
down_revision = "0009_backfill_progress"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dem_job_runs",
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("include_grid", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("progress_json", sa.Text(), nullable=True),
        sa.Column("province_gcs_uri", sa.Text(), nullable=True),
        sa.Column("grid_gcs_uri", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("run_id"),
    )
    op.create_index(op.f("ix_dem_job_runs_status"), "dem_job_runs", ["status"], unique=False)
    op.create_index(op.f("ix_dem_job_runs_updated_at"), "dem_job_runs", ["updated_at"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_dem_job_runs_updated_at"), table_name="dem_job_runs")
    op.drop_index(op.f("ix_dem_job_runs_status"), table_name="dem_job_runs")
    op.drop_table("dem_job_runs")
