"""add sentinel backfill progress table

Revision ID: 0010_sentinel_backfill_progress
Revises: 0009_backfill_progress
Create Date: 2026-03-03
"""

from alembic import op
import sqlalchemy as sa


revision = "0010_sentinel_backfill_progress"
down_revision = "0009_backfill_progress"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "sentinel_backfill_progress",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("month", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="queued"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rows_written", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_msg", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("run_id", "year", "month", name="uq_sentinel_backfill_run_month"),
    )
    op.create_index(op.f("ix_sentinel_backfill_progress_run_id"), "sentinel_backfill_progress", ["run_id"], unique=False)
    op.create_index(op.f("ix_sentinel_backfill_progress_status"), "sentinel_backfill_progress", ["status"], unique=False)
    op.create_index(op.f("ix_sentinel_backfill_progress_year"), "sentinel_backfill_progress", ["year"], unique=False)
    op.create_index(op.f("ix_sentinel_backfill_progress_month"), "sentinel_backfill_progress", ["month"], unique=False)
    op.create_index(op.f("ix_sentinel_backfill_progress_updated_at"), "sentinel_backfill_progress", ["updated_at"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_sentinel_backfill_progress_updated_at"), table_name="sentinel_backfill_progress")
    op.drop_index(op.f("ix_sentinel_backfill_progress_month"), table_name="sentinel_backfill_progress")
    op.drop_index(op.f("ix_sentinel_backfill_progress_year"), table_name="sentinel_backfill_progress")
    op.drop_index(op.f("ix_sentinel_backfill_progress_status"), table_name="sentinel_backfill_progress")
    op.drop_index(op.f("ix_sentinel_backfill_progress_run_id"), table_name="sentinel_backfill_progress")
    op.drop_table("sentinel_backfill_progress")
