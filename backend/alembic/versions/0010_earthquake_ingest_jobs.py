"""add earthquake ingest jobs table

Revision ID: 0010_earthquake_ingest_jobs
Revises: 0009_backfill_progress
Create Date: 2026-03-03
"""

from alembic import op
import sqlalchemy as sa


revision = "0010_earthquake_ingest_jobs"
down_revision = "0009_backfill_progress"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "earthquake_ingest_jobs",
        sa.Column("job_id", sa.String(length=64), nullable=False),
        sa.Column("request_signature", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("rows_written", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("files_written", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("progress_json", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("job_id"),
    )
    op.create_index(op.f("ix_earthquake_ingest_jobs_status"), "earthquake_ingest_jobs", ["status"], unique=False)
    op.create_index(op.f("ix_earthquake_ingest_jobs_request_signature"), "earthquake_ingest_jobs", ["request_signature"], unique=False)
    op.create_index(op.f("ix_earthquake_ingest_jobs_created_at"), "earthquake_ingest_jobs", ["created_at"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_earthquake_ingest_jobs_created_at"), table_name="earthquake_ingest_jobs")
    op.drop_index(op.f("ix_earthquake_ingest_jobs_request_signature"), table_name="earthquake_ingest_jobs")
    op.drop_index(op.f("ix_earthquake_ingest_jobs_status"), table_name="earthquake_ingest_jobs")
    op.drop_table("earthquake_ingest_jobs")
