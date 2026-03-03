"""add nasa earthdata ingest jobs table

Revision ID: 0010_nasa_earthdata_jobs
Revises: 0009_backfill_progress
Create Date: 2026-03-03
"""

from alembic import op
import sqlalchemy as sa


revision = "0010_nasa_earthdata_jobs"
down_revision = "0009_backfill_progress"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "nasa_ingest_jobs",
        sa.Column("job_id", sa.String(length=64), nullable=False),
        sa.Column("request_signature", sa.String(length=64), nullable=False),
        sa.Column("dataset", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("months_total", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("months_completed", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("months_failed", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("rows_written", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("files_downloaded", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("files_written", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("progress_json", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("job_id"),
        sa.UniqueConstraint("request_signature"),
    )
    op.create_index(op.f("ix_nasa_ingest_jobs_dataset"), "nasa_ingest_jobs", ["dataset"], unique=False)
    op.create_index(op.f("ix_nasa_ingest_jobs_status"), "nasa_ingest_jobs", ["status"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_nasa_ingest_jobs_status"), table_name="nasa_ingest_jobs")
    op.drop_index(op.f("ix_nasa_ingest_jobs_dataset"), table_name="nasa_ingest_jobs")
    op.drop_table("nasa_ingest_jobs")
