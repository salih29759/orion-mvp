"""add backfill progress tracking table

Revision ID: 0009_backfill_progress
Revises: 0008_aws_era5_hybrid
Create Date: 2026-03-02
"""

from alembic import op
import sqlalchemy as sa


revision = "0009_backfill_progress"
down_revision = "0008_aws_era5_hybrid"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "backfill_progress",
        sa.Column("month", sa.Date(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="pending"),
        sa.Column("row_count", sa.Integer(), nullable=True),
        sa.Column("duration_sec", sa.Float(), nullable=True),
        sa.Column("error_msg", sa.Text(), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("run_id", sa.String(length=50), nullable=True),
        sa.PrimaryKeyConstraint("month"),
    )
    op.create_index(op.f("ix_backfill_progress_status"), "backfill_progress", ["status"], unique=False)
    op.create_index(op.f("ix_backfill_progress_run_id"), "backfill_progress", ["run_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_backfill_progress_run_id"), table_name="backfill_progress")
    op.drop_index(op.f("ix_backfill_progress_status"), table_name="backfill_progress")
    op.drop_table("backfill_progress")
