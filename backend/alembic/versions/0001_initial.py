"""initial schema

Revision ID: 0001_initial
Revises:
Create Date: 2026-03-01
"""

from alembic import op
import sqlalchemy as sa


revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "provinces",
        sa.Column("id", sa.String(length=4), nullable=False),
        sa.Column("plate", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("region", sa.String(length=120), nullable=False),
        sa.Column("lat", sa.Float(), nullable=False),
        sa.Column("lng", sa.Float(), nullable=False),
        sa.Column("population", sa.Integer(), nullable=False),
        sa.Column("insured_assets", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_provinces_plate"), "provinces", ["plate"], unique=True)
    op.create_index(op.f("ix_provinces_region"), "provinces", ["region"], unique=False)

    op.create_table(
        "daily_scores",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("province_id", sa.String(length=4), nullable=False),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("flood_score", sa.Integer(), nullable=False),
        sa.Column("drought_score", sa.Integer(), nullable=False),
        sa.Column("overall_score", sa.Integer(), nullable=False),
        sa.Column("risk_level", sa.String(length=16), nullable=False),
        sa.Column("trend", sa.String(length=16), nullable=False),
        sa.Column("trend_pct", sa.Float(), nullable=False),
        sa.Column("rain_7d_mm", sa.Float(), nullable=False),
        sa.Column("rain_60d_mm", sa.Float(), nullable=False),
        sa.Column("data_source", sa.String(length=64), nullable=False),
        sa.Column("model_version", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.ForeignKeyConstraint(["province_id"], ["provinces.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("province_id", "as_of_date", name="uq_daily_scores_province_date"),
    )
    op.create_index(op.f("ix_daily_scores_as_of_date"), "daily_scores", ["as_of_date"], unique=False)
    op.create_index(op.f("ix_daily_scores_province_id"), "daily_scores", ["province_id"], unique=False)
    op.create_index(op.f("ix_daily_scores_risk_level"), "daily_scores", ["risk_level"], unique=False)

    op.create_table(
        "alerts",
        sa.Column("id", sa.String(length=64), nullable=False),
        sa.Column("province_id", sa.String(length=4), nullable=False),
        sa.Column("level", sa.String(length=16), nullable=False),
        sa.Column("risk_type", sa.String(length=16), nullable=False),
        sa.Column("affected_policies", sa.Integer(), nullable=False),
        sa.Column("estimated_loss_usd", sa.Float(), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("active", sa.Boolean(), server_default="true", nullable=False),
        sa.ForeignKeyConstraint(["province_id"], ["provinces.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_alerts_active"), "alerts", ["active"], unique=False)
    op.create_index(op.f("ix_alerts_issued_at"), "alerts", ["issued_at"], unique=False)
    op.create_index(op.f("ix_alerts_level"), "alerts", ["level"], unique=False)
    op.create_index(op.f("ix_alerts_province_id"), "alerts", ["province_id"], unique=False)

    op.create_table(
        "pipeline_runs",
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("rows_written", sa.Integer(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("run_id"),
    )
    op.create_index(op.f("ix_pipeline_runs_status"), "pipeline_runs", ["status"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_pipeline_runs_status"), table_name="pipeline_runs")
    op.drop_table("pipeline_runs")

    op.drop_index(op.f("ix_alerts_province_id"), table_name="alerts")
    op.drop_index(op.f("ix_alerts_level"), table_name="alerts")
    op.drop_index(op.f("ix_alerts_issued_at"), table_name="alerts")
    op.drop_index(op.f("ix_alerts_active"), table_name="alerts")
    op.drop_table("alerts")

    op.drop_index(op.f("ix_daily_scores_risk_level"), table_name="daily_scores")
    op.drop_index(op.f("ix_daily_scores_province_id"), table_name="daily_scores")
    op.drop_index(op.f("ix_daily_scores_as_of_date"), table_name="daily_scores")
    op.drop_table("daily_scores")

    op.drop_index(op.f("ix_provinces_region"), table_name="provinces")
    op.drop_index(op.f("ix_provinces_plate"), table_name="provinces")
    op.drop_table("provinces")
