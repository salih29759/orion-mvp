"""add era5 ingestion jobs and artifacts

Revision ID: 0003_era5_jobs
Revises: 0002_assets_bigint
Create Date: 2026-03-01
"""

from alembic import op
import sqlalchemy as sa


revision = "0003_era5_jobs"
down_revision = "0002_assets_bigint"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "era5_ingest_jobs",
        sa.Column("job_id", sa.String(length=64), nullable=False),
        sa.Column("request_signature", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("dataset", sa.String(length=128), nullable=False),
        sa.Column("variables_csv", sa.Text(), nullable=False),
        sa.Column("bbox_csv", sa.String(length=128), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("rows_written", sa.Integer(), nullable=False),
        sa.Column("bytes_downloaded", sa.BigInteger(), nullable=False),
        sa.Column("raw_files", sa.Integer(), nullable=False),
        sa.Column("feature_files", sa.Integer(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("job_id"),
    )
    op.create_index(op.f("ix_era5_ingest_jobs_request_signature"), "era5_ingest_jobs", ["request_signature"], unique=False)
    op.create_index(op.f("ix_era5_ingest_jobs_status"), "era5_ingest_jobs", ["status"], unique=False)

    op.create_table(
        "era5_artifacts",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("request_signature", sa.String(length=64), nullable=False),
        sa.Column("job_id", sa.String(length=64), nullable=False),
        sa.Column("artifact_type", sa.String(length=32), nullable=False),
        sa.Column("dataset", sa.String(length=128), nullable=False),
        sa.Column("variables_csv", sa.Text(), nullable=False),
        sa.Column("bbox_csv", sa.String(length=128), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("gcs_uri", sa.Text(), nullable=False),
        sa.Column("checksum_sha256", sa.String(length=64), nullable=False),
        sa.Column("byte_size", sa.BigInteger(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.ForeignKeyConstraint(["job_id"], ["era5_ingest_jobs.job_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("request_signature", "artifact_type", "gcs_uri", name="uq_era5_artifact_sig_type_uri"),
    )
    op.create_index(op.f("ix_era5_artifacts_artifact_type"), "era5_artifacts", ["artifact_type"], unique=False)
    op.create_index(op.f("ix_era5_artifacts_checksum_sha256"), "era5_artifacts", ["checksum_sha256"], unique=False)
    op.create_index(op.f("ix_era5_artifacts_job_id"), "era5_artifacts", ["job_id"], unique=False)
    op.create_index(op.f("ix_era5_artifacts_request_signature"), "era5_artifacts", ["request_signature"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_era5_artifacts_request_signature"), table_name="era5_artifacts")
    op.drop_index(op.f("ix_era5_artifacts_job_id"), table_name="era5_artifacts")
    op.drop_index(op.f("ix_era5_artifacts_checksum_sha256"), table_name="era5_artifacts")
    op.drop_index(op.f("ix_era5_artifacts_artifact_type"), table_name="era5_artifacts")
    op.drop_table("era5_artifacts")

    op.drop_index(op.f("ix_era5_ingest_jobs_status"), table_name="era5_ingest_jobs")
    op.drop_index(op.f("ix_era5_ingest_jobs_request_signature"), table_name="era5_ingest_jobs")
    op.drop_table("era5_ingest_jobs")
