"""add cams backfill progress tracking table

Revision ID: 0010_cams_backfill_progress
Revises: 0009_backfill_progress
Create Date: 2026-03-02
"""

from alembic import op
import sqlalchemy as sa


revision = "0010_cams_backfill_progress"
down_revision = "0009_backfill_progress"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cams_backfill_progress",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("month", sa.Date(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="pending"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_msg", sa.Text(), nullable=True),
        sa.Column("rows_written", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("duration_sec", sa.Float(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id", "month", name="uq_cams_backfill_run_month"),
    )
    op.create_index(op.f("ix_cams_backfill_progress_run_id"), "cams_backfill_progress", ["run_id"], unique=False)
    op.create_index(op.f("ix_cams_backfill_progress_status"), "cams_backfill_progress", ["status"], unique=False)
    op.create_index(op.f("ix_cams_backfill_progress_month"), "cams_backfill_progress", ["month"], unique=False)
    op.create_index(op.f("ix_cams_backfill_progress_updated_at"), "cams_backfill_progress", ["updated_at"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_cams_backfill_progress_updated_at"), table_name="cams_backfill_progress")
    op.drop_index(op.f("ix_cams_backfill_progress_month"), table_name="cams_backfill_progress")
    op.drop_index(op.f("ix_cams_backfill_progress_status"), table_name="cams_backfill_progress")
    op.drop_index(op.f("ix_cams_backfill_progress_run_id"), table_name="cams_backfill_progress")
    op.drop_table("cams_backfill_progress")
