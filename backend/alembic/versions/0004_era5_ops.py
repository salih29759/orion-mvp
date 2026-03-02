"""era5 ops tables and dq columns

Revision ID: 0004_era5_ops
Revises: 0003_era5_jobs
Create Date: 2026-03-02
"""

from alembic import op
import sqlalchemy as sa


revision = "0004_era5_ops"
down_revision = "0003_era5_jobs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("era5_ingest_jobs", sa.Column("dq_status", sa.String(length=32), nullable=True))
    op.add_column("era5_ingest_jobs", sa.Column("dq_report_json", sa.Text(), nullable=True))
    op.add_column("era5_ingest_jobs", sa.Column("duration_seconds", sa.Float(), nullable=True))
    op.create_index(op.f("ix_era5_ingest_jobs_dq_status"), "era5_ingest_jobs", ["dq_status"], unique=False)

    op.create_table(
        "era5_backfill_jobs",
        sa.Column("backfill_id", sa.String(length=64), nullable=False),
        sa.Column("request_signature", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=False),
        sa.Column("dataset", sa.String(length=128), nullable=False),
        sa.Column("variables_csv", sa.Text(), nullable=False),
        sa.Column("bbox_csv", sa.String(length=128), nullable=False),
        sa.Column("start_month", sa.String(length=7), nullable=False),
        sa.Column("end_month", sa.String(length=7), nullable=False),
        sa.Column("months_total", sa.Integer(), nullable=False),
        sa.Column("months_success", sa.Integer(), nullable=False),
        sa.Column("months_failed", sa.Integer(), nullable=False),
        sa.Column("failed_months_json", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("backfill_id"),
    )
    op.create_index(op.f("ix_era5_backfill_jobs_request_signature"), "era5_backfill_jobs", ["request_signature"], unique=True)
    op.create_index(op.f("ix_era5_backfill_jobs_status"), "era5_backfill_jobs", ["status"], unique=False)

    op.create_table(
        "era5_backfill_items",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("backfill_id", sa.String(length=64), nullable=False),
        sa.Column("month_label", sa.String(length=7), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("job_id", sa.String(length=64), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["backfill_id"], ["era5_backfill_jobs.backfill_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["job_id"], ["era5_ingest_jobs.job_id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("backfill_id", "month_label", name="uq_backfill_month"),
    )
    op.create_index(op.f("ix_era5_backfill_items_backfill_id"), "era5_backfill_items", ["backfill_id"], unique=False)
    op.create_index(op.f("ix_era5_backfill_items_job_id"), "era5_backfill_items", ["job_id"], unique=False)
    op.create_index(op.f("ix_era5_backfill_items_month_label"), "era5_backfill_items", ["month_label"], unique=False)
    op.create_index(op.f("ix_era5_backfill_items_status"), "era5_backfill_items", ["status"], unique=False)

    op.create_table(
        "export_jobs",
        sa.Column("export_id", sa.String(length=64), nullable=False),
        sa.Column("portfolio_id", sa.String(length=128), nullable=False),
        sa.Column("scenario", sa.String(length=32), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("output_format", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("row_count", sa.Integer(), nullable=False),
        sa.Column("gcs_uri", sa.Text(), nullable=True),
        sa.Column("signed_url", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.PrimaryKeyConstraint("export_id"),
    )
    op.create_index(op.f("ix_export_jobs_portfolio_id"), "export_jobs", ["portfolio_id"], unique=False)
    op.create_index(op.f("ix_export_jobs_status"), "export_jobs", ["status"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_export_jobs_status"), table_name="export_jobs")
    op.drop_index(op.f("ix_export_jobs_portfolio_id"), table_name="export_jobs")
    op.drop_table("export_jobs")

    op.drop_index(op.f("ix_era5_backfill_items_status"), table_name="era5_backfill_items")
    op.drop_index(op.f("ix_era5_backfill_items_month_label"), table_name="era5_backfill_items")
    op.drop_index(op.f("ix_era5_backfill_items_job_id"), table_name="era5_backfill_items")
    op.drop_index(op.f("ix_era5_backfill_items_backfill_id"), table_name="era5_backfill_items")
    op.drop_table("era5_backfill_items")

    op.drop_index(op.f("ix_era5_backfill_jobs_status"), table_name="era5_backfill_jobs")
    op.drop_index(op.f("ix_era5_backfill_jobs_request_signature"), table_name="era5_backfill_jobs")
    op.drop_table("era5_backfill_jobs")

    op.drop_index(op.f("ix_era5_ingest_jobs_dq_status"), table_name="era5_ingest_jobs")
    op.drop_column("era5_ingest_jobs", "duration_seconds")
    op.drop_column("era5_ingest_jobs", "dq_report_json")
    op.drop_column("era5_ingest_jobs", "dq_status")
