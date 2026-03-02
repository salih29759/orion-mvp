"""add aws era5 catalog and hybrid ingest metadata

Revision ID: 0008_aws_era5_hybrid
Revises: 0007_firms_notifications
Create Date: 2026-03-02
"""

from alembic import op
import sqlalchemy as sa


revision = "0008_aws_era5_hybrid"
down_revision = "0007_firms_notifications"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("era5_ingest_jobs", sa.Column("provider", sa.String(length=32), nullable=False, server_default="cds"))
    op.add_column("era5_ingest_jobs", sa.Column("mode", sa.String(length=16), nullable=False, server_default="bbox"))
    op.add_column("era5_ingest_jobs", sa.Column("points_set", sa.String(length=64), nullable=True))
    op.add_column("era5_ingest_jobs", sa.Column("month_label", sa.String(length=7), nullable=True))
    op.add_column("era5_ingest_jobs", sa.Column("source_range_json", sa.Text(), nullable=True))
    op.create_index(op.f("ix_era5_ingest_jobs_month_label"), "era5_ingest_jobs", ["month_label"], unique=False)

    op.add_column("era5_artifacts", sa.Column("source_uri", sa.Text(), nullable=True))
    op.add_column("era5_artifacts", sa.Column("source_etag", sa.String(length=255), nullable=True))
    op.add_column("era5_artifacts", sa.Column("cache_hit", sa.Boolean(), nullable=False, server_default=sa.text("false")))

    op.add_column("era5_backfill_jobs", sa.Column("provider_strategy", sa.String(length=32), nullable=False, server_default="aws_first_hybrid"))
    op.add_column("era5_backfill_jobs", sa.Column("force", sa.Boolean(), nullable=False, server_default=sa.text("false")))

    op.add_column("era5_backfill_items", sa.Column("provider_selected", sa.String(length=16), nullable=False, server_default="cds"))
    op.add_column("era5_backfill_items", sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"))

    op.create_table(
        "aws_era5_objects",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("bucket", sa.String(length=128), nullable=False),
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("size", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("etag", sa.String(length=255), nullable=True),
        sa.Column("last_modified", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dataset_group", sa.String(length=128), nullable=True),
        sa.Column("variable", sa.String(length=64), nullable=True),
        sa.Column("year", sa.Integer(), nullable=True),
        sa.Column("month", sa.Integer(), nullable=True),
        sa.Column("day", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("bucket", "key", name="uq_aws_era5_bucket_key"),
    )
    op.create_index(op.f("ix_aws_era5_objects_bucket"), "aws_era5_objects", ["bucket"], unique=False)
    op.create_index(op.f("ix_aws_era5_objects_dataset_group"), "aws_era5_objects", ["dataset_group"], unique=False)
    op.create_index(op.f("ix_aws_era5_objects_etag"), "aws_era5_objects", ["etag"], unique=False)
    op.create_index(op.f("ix_aws_era5_objects_last_modified"), "aws_era5_objects", ["last_modified"], unique=False)
    op.create_index(op.f("ix_aws_era5_objects_month"), "aws_era5_objects", ["month"], unique=False)
    op.create_index(op.f("ix_aws_era5_objects_variable"), "aws_era5_objects", ["variable"], unique=False)
    op.create_index(op.f("ix_aws_era5_objects_year"), "aws_era5_objects", ["year"], unique=False)

    op.create_table(
        "aws_era5_catalog_runs",
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("objects_scanned", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("run_id"),
    )
    op.create_index(op.f("ix_aws_era5_catalog_runs_status"), "aws_era5_catalog_runs", ["status"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_aws_era5_catalog_runs_status"), table_name="aws_era5_catalog_runs")
    op.drop_table("aws_era5_catalog_runs")

    op.drop_index(op.f("ix_aws_era5_objects_year"), table_name="aws_era5_objects")
    op.drop_index(op.f("ix_aws_era5_objects_variable"), table_name="aws_era5_objects")
    op.drop_index(op.f("ix_aws_era5_objects_month"), table_name="aws_era5_objects")
    op.drop_index(op.f("ix_aws_era5_objects_last_modified"), table_name="aws_era5_objects")
    op.drop_index(op.f("ix_aws_era5_objects_etag"), table_name="aws_era5_objects")
    op.drop_index(op.f("ix_aws_era5_objects_dataset_group"), table_name="aws_era5_objects")
    op.drop_index(op.f("ix_aws_era5_objects_bucket"), table_name="aws_era5_objects")
    op.drop_table("aws_era5_objects")

    op.drop_column("era5_backfill_items", "attempt_count")
    op.drop_column("era5_backfill_items", "provider_selected")

    op.drop_column("era5_backfill_jobs", "force")
    op.drop_column("era5_backfill_jobs", "provider_strategy")

    op.drop_column("era5_artifacts", "cache_hit")
    op.drop_column("era5_artifacts", "source_etag")
    op.drop_column("era5_artifacts", "source_uri")

    op.drop_index(op.f("ix_era5_ingest_jobs_month_label"), table_name="era5_ingest_jobs")
    op.drop_column("era5_ingest_jobs", "source_range_json")
    op.drop_column("era5_ingest_jobs", "month_label")
    op.drop_column("era5_ingest_jobs", "points_set")
    op.drop_column("era5_ingest_jobs", "mode")
    op.drop_column("era5_ingest_jobs", "provider")
