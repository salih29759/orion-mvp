"""add openmeteo jobs and artifacts tables

Revision ID: 0101_openmeteo_jobs
Revises: 0009_backfill_progress
Create Date: 2026-03-02
"""

from alembic import op
import sqlalchemy as sa


revision = "0101_openmeteo_jobs"
down_revision = "0009_backfill_progress"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "openmeteo_jobs",
        sa.Column("job_id", sa.String(length=64), nullable=False),
        sa.Column("request_signature", sa.String(length=64), nullable=False),
        sa.Column("job_type", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="queued"),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("rows_written", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("files_written", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("progress_json", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_seconds", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("job_id"),
    )
    op.create_index(op.f("ix_openmeteo_jobs_request_signature"), "openmeteo_jobs", ["request_signature"], unique=False)
    op.create_index(op.f("ix_openmeteo_jobs_job_type"), "openmeteo_jobs", ["job_type"], unique=False)
    op.create_index(op.f("ix_openmeteo_jobs_status"), "openmeteo_jobs", ["status"], unique=False)
    op.create_index(op.f("ix_openmeteo_jobs_run_id"), "openmeteo_jobs", ["run_id"], unique=False)

    op.create_table(
        "openmeteo_artifacts",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("job_id", sa.String(length=64), nullable=False),
        sa.Column("artifact_type", sa.String(length=32), nullable=False),
        sa.Column("gcs_uri", sa.Text(), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("row_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("checksum_sha256", sa.String(length=64), nullable=False),
        sa.Column("byte_size", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(["job_id"], ["openmeteo_jobs.job_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_openmeteo_artifacts_job_id"), "openmeteo_artifacts", ["job_id"], unique=False)
    op.create_index(op.f("ix_openmeteo_artifacts_artifact_type"), "openmeteo_artifacts", ["artifact_type"], unique=False)
    op.create_index(op.f("ix_openmeteo_artifacts_checksum_sha256"), "openmeteo_artifacts", ["checksum_sha256"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_openmeteo_artifacts_checksum_sha256"), table_name="openmeteo_artifacts")
    op.drop_index(op.f("ix_openmeteo_artifacts_artifact_type"), table_name="openmeteo_artifacts")
    op.drop_index(op.f("ix_openmeteo_artifacts_job_id"), table_name="openmeteo_artifacts")
    op.drop_table("openmeteo_artifacts")

    op.drop_index(op.f("ix_openmeteo_jobs_run_id"), table_name="openmeteo_jobs")
    op.drop_index(op.f("ix_openmeteo_jobs_status"), table_name="openmeteo_jobs")
    op.drop_index(op.f("ix_openmeteo_jobs_job_type"), table_name="openmeteo_jobs")
    op.drop_index(op.f("ix_openmeteo_jobs_request_signature"), table_name="openmeteo_jobs")
    op.drop_table("openmeteo_jobs")
