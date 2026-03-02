"""add firms ingest, fires, notifications

Revision ID: 0007_firms_notifications
Revises: 0006_climatology_doy
Create Date: 2026-03-01 23:50:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "0007_firms_notifications"
down_revision = "0006_climatology_doy"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "firms_ingest_jobs",
        sa.Column("job_id", sa.String(length=64), nullable=False),
        sa.Column("request_signature", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("bbox_csv", sa.String(length=128), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("rows_fetched", sa.Integer(), nullable=False),
        sa.Column("rows_inserted", sa.Integer(), nullable=False),
        sa.Column("raw_gcs_uri", sa.Text(), nullable=True),
        sa.Column("duration_seconds", sa.Float(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("job_id"),
    )
    op.create_index(op.f("ix_firms_ingest_jobs_request_signature"), "firms_ingest_jobs", ["request_signature"], unique=True)
    op.create_index(op.f("ix_firms_ingest_jobs_status"), "firms_ingest_jobs", ["status"], unique=False)
    op.create_index(op.f("ix_firms_ingest_jobs_source"), "firms_ingest_jobs", ["source"], unique=False)

    op.create_table(
        "fires",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("time_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("lat", sa.Float(), nullable=False),
        sa.Column("lon", sa.Float(), nullable=False),
        sa.Column("lat_round", sa.Float(), nullable=False),
        sa.Column("lon_round", sa.Float(), nullable=False),
        sa.Column("geom_wkt", sa.Text(), nullable=True),
        sa.Column("frp", sa.Float(), nullable=True),
        sa.Column("confidence", sa.String(length=32), nullable=True),
        sa.Column("satellite", sa.String(length=32), nullable=True),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("raw_job_id", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.ForeignKeyConstraint(["raw_job_id"], ["firms_ingest_jobs.job_id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source", "time_utc", "lat_round", "lon_round", name="uq_fire_event_key"),
    )
    op.create_index(op.f("ix_fires_time_utc"), "fires", ["time_utc"], unique=False)
    op.create_index(op.f("ix_fires_lat"), "fires", ["lat"], unique=False)
    op.create_index(op.f("ix_fires_lon"), "fires", ["lon"], unique=False)
    op.create_index(op.f("ix_fires_lat_round"), "fires", ["lat_round"], unique=False)
    op.create_index(op.f("ix_fires_lon_round"), "fires", ["lon_round"], unique=False)
    op.create_index(op.f("ix_fires_source"), "fires", ["source"], unique=False)
    op.create_index(op.f("ix_fires_raw_job_id"), "fires", ["raw_job_id"], unique=False)

    op.create_table(
        "notifications",
        sa.Column("id", sa.String(length=64), nullable=False),
        sa.Column("customer_id", sa.String(length=128), nullable=True),
        sa.Column("portfolio_id", sa.String(length=128), nullable=True),
        sa.Column("asset_id", sa.String(length=128), nullable=False),
        sa.Column("type", sa.String(length=64), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("dedup_key", sa.String(length=255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("acknowledged_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("dedup_key", name="uq_notification_dedup"),
    )
    op.create_index(op.f("ix_notifications_customer_id"), "notifications", ["customer_id"], unique=False)
    op.create_index(op.f("ix_notifications_portfolio_id"), "notifications", ["portfolio_id"], unique=False)
    op.create_index(op.f("ix_notifications_asset_id"), "notifications", ["asset_id"], unique=False)
    op.create_index(op.f("ix_notifications_type"), "notifications", ["type"], unique=False)
    op.create_index(op.f("ix_notifications_severity"), "notifications", ["severity"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_notifications_severity"), table_name="notifications")
    op.drop_index(op.f("ix_notifications_type"), table_name="notifications")
    op.drop_index(op.f("ix_notifications_asset_id"), table_name="notifications")
    op.drop_index(op.f("ix_notifications_portfolio_id"), table_name="notifications")
    op.drop_index(op.f("ix_notifications_customer_id"), table_name="notifications")
    op.drop_table("notifications")

    op.drop_index(op.f("ix_fires_raw_job_id"), table_name="fires")
    op.drop_index(op.f("ix_fires_source"), table_name="fires")
    op.drop_index(op.f("ix_fires_lon_round"), table_name="fires")
    op.drop_index(op.f("ix_fires_lat_round"), table_name="fires")
    op.drop_index(op.f("ix_fires_lon"), table_name="fires")
    op.drop_index(op.f("ix_fires_lat"), table_name="fires")
    op.drop_index(op.f("ix_fires_time_utc"), table_name="fires")
    op.drop_table("fires")

    op.drop_index(op.f("ix_firms_ingest_jobs_source"), table_name="firms_ingest_jobs")
    op.drop_index(op.f("ix_firms_ingest_jobs_status"), table_name="firms_ingest_jobs")
    op.drop_index(op.f("ix_firms_ingest_jobs_request_signature"), table_name="firms_ingest_jobs")
    op.drop_table("firms_ingest_jobs")
