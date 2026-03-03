"""add glofas backfill jobs

Revision ID: 0010_glofas_jobs
Revises: 0009_backfill_progress
Create Date: 2026-03-03
"""

from alembic import op
import sqlalchemy as sa


revision = "0010_glofas_jobs"
down_revision = "0009_backfill_progress"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "glofas_backfill_jobs",
        sa.Column("job_id", sa.String(length=64), nullable=False),
        sa.Column("request_signature", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("effective_end_date", sa.Date(), nullable=False),
        sa.Column("concurrency", sa.Integer(), nullable=False, server_default="2"),
        sa.Column("months_total", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("months_success", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("months_failed", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("baseline_ready", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("baseline_gcs_uri", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("job_id"),
    )
    op.create_index(op.f("ix_glofas_backfill_jobs_request_signature"), "glofas_backfill_jobs", ["request_signature"], unique=True)
    op.create_index(op.f("ix_glofas_backfill_jobs_status"), "glofas_backfill_jobs", ["status"], unique=False)

    op.create_table(
        "glofas_backfill_items",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("job_id", sa.String(length=64), nullable=False),
        sa.Column("month_label", sa.String(length=7), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="queued"),
        sa.Column("rows_written", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("output_gcs_uri", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.ForeignKeyConstraint(["job_id"], ["glofas_backfill_jobs.job_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("job_id", "month_label", name="uq_glofas_backfill_job_month"),
    )
    op.create_index(op.f("ix_glofas_backfill_items_job_id"), "glofas_backfill_items", ["job_id"], unique=False)
    op.create_index(op.f("ix_glofas_backfill_items_month_label"), "glofas_backfill_items", ["month_label"], unique=False)
    op.create_index(op.f("ix_glofas_backfill_items_status"), "glofas_backfill_items", ["status"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_glofas_backfill_items_status"), table_name="glofas_backfill_items")
    op.drop_index(op.f("ix_glofas_backfill_items_month_label"), table_name="glofas_backfill_items")
    op.drop_index(op.f("ix_glofas_backfill_items_job_id"), table_name="glofas_backfill_items")
    op.drop_table("glofas_backfill_items")

    op.drop_index(op.f("ix_glofas_backfill_jobs_status"), table_name="glofas_backfill_jobs")
    op.drop_index(op.f("ix_glofas_backfill_jobs_request_signature"), table_name="glofas_backfill_jobs")
    op.drop_table("glofas_backfill_jobs")
