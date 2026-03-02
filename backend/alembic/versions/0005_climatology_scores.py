"""climatology + asset scores + portfolio assets

Revision ID: 0005_climatology_scores
Revises: 0004_era5_ops
Create Date: 2026-03-02
"""

from alembic import op
import sqlalchemy as sa


revision = "0005_climatology_scores"
down_revision = "0004_era5_ops"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "climatology_runs",
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("climatology_version", sa.String(length=128), nullable=False),
        sa.Column("dataset", sa.String(length=128), nullable=False),
        sa.Column("baseline_start", sa.Date(), nullable=False),
        sa.Column("baseline_end", sa.Date(), nullable=False),
        sa.Column("level", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("row_count", sa.Integer(), nullable=False),
        sa.Column("thresholds_gcs_uri", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.PrimaryKeyConstraint("run_id"),
    )
    op.create_index(op.f("ix_climatology_runs_climatology_version"), "climatology_runs", ["climatology_version"], unique=True)
    op.create_index(op.f("ix_climatology_runs_status"), "climatology_runs", ["status"], unique=False)

    op.create_table(
        "climatology_thresholds",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("climatology_version", sa.String(length=128), nullable=False),
        sa.Column("cell_lat", sa.Float(), nullable=False),
        sa.Column("cell_lng", sa.Float(), nullable=False),
        sa.Column("month", sa.Integer(), nullable=False),
        sa.Column("temp_max_p95", sa.Float(), nullable=True),
        sa.Column("wind_max_p95", sa.Float(), nullable=True),
        sa.Column("precip_1d_p95", sa.Float(), nullable=True),
        sa.Column("precip_1d_p99", sa.Float(), nullable=True),
        sa.Column("precip_7d_p95", sa.Float(), nullable=True),
        sa.Column("precip_7d_p99", sa.Float(), nullable=True),
        sa.Column("precip_30d_p10", sa.Float(), nullable=True),
        sa.Column("soil_moisture_p10", sa.Float(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("climatology_version", "cell_lat", "cell_lng", "month", name="uq_clim_version_cell_month"),
    )
    op.create_index(op.f("ix_climatology_thresholds_climatology_version"), "climatology_thresholds", ["climatology_version"], unique=False)
    op.create_index(op.f("ix_climatology_thresholds_cell_lat"), "climatology_thresholds", ["cell_lat"], unique=False)
    op.create_index(op.f("ix_climatology_thresholds_cell_lng"), "climatology_thresholds", ["cell_lng"], unique=False)
    op.create_index(op.f("ix_climatology_thresholds_month"), "climatology_thresholds", ["month"], unique=False)

    op.create_table(
        "asset_risk_scores",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("asset_id", sa.String(length=128), nullable=False),
        sa.Column("score_date", sa.Date(), nullable=False),
        sa.Column("peril", sa.String(length=32), nullable=False),
        sa.Column("scenario", sa.String(length=32), nullable=False),
        sa.Column("horizon", sa.String(length=32), nullable=False),
        sa.Column("likelihood", sa.String(length=32), nullable=False),
        sa.Column("score_0_100", sa.Integer(), nullable=False),
        sa.Column("band", sa.String(length=16), nullable=False),
        sa.Column("exposure_json", sa.Text(), nullable=False),
        sa.Column("drivers_json", sa.Text(), nullable=False),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("climatology_version", sa.String(length=128), nullable=False),
        sa.Column("data_version", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("asset_id", "score_date", "peril", "scenario", "horizon", "likelihood", name="uq_asset_risk_score_dim"),
    )
    op.create_index(op.f("ix_asset_risk_scores_asset_id"), "asset_risk_scores", ["asset_id"], unique=False)
    op.create_index(op.f("ix_asset_risk_scores_score_date"), "asset_risk_scores", ["score_date"], unique=False)
    op.create_index(op.f("ix_asset_risk_scores_peril"), "asset_risk_scores", ["peril"], unique=False)
    op.create_index(op.f("ix_asset_risk_scores_scenario"), "asset_risk_scores", ["scenario"], unique=False)
    op.create_index(op.f("ix_asset_risk_scores_band"), "asset_risk_scores", ["band"], unique=False)
    op.create_index(op.f("ix_asset_risk_scores_run_id"), "asset_risk_scores", ["run_id"], unique=False)
    op.create_index(op.f("ix_asset_risk_scores_climatology_version"), "asset_risk_scores", ["climatology_version"], unique=False)

    op.create_table(
        "portfolio_assets",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("portfolio_id", sa.String(length=128), nullable=False),
        sa.Column("asset_id", sa.String(length=128), nullable=False),
        sa.Column("lat", sa.Float(), nullable=False),
        sa.Column("lon", sa.Float(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("portfolio_id", "asset_id", name="uq_portfolio_asset"),
    )
    op.create_index(op.f("ix_portfolio_assets_portfolio_id"), "portfolio_assets", ["portfolio_id"], unique=False)
    op.create_index(op.f("ix_portfolio_assets_asset_id"), "portfolio_assets", ["asset_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_portfolio_assets_asset_id"), table_name="portfolio_assets")
    op.drop_index(op.f("ix_portfolio_assets_portfolio_id"), table_name="portfolio_assets")
    op.drop_table("portfolio_assets")

    op.drop_index(op.f("ix_asset_risk_scores_climatology_version"), table_name="asset_risk_scores")
    op.drop_index(op.f("ix_asset_risk_scores_run_id"), table_name="asset_risk_scores")
    op.drop_index(op.f("ix_asset_risk_scores_band"), table_name="asset_risk_scores")
    op.drop_index(op.f("ix_asset_risk_scores_scenario"), table_name="asset_risk_scores")
    op.drop_index(op.f("ix_asset_risk_scores_peril"), table_name="asset_risk_scores")
    op.drop_index(op.f("ix_asset_risk_scores_score_date"), table_name="asset_risk_scores")
    op.drop_index(op.f("ix_asset_risk_scores_asset_id"), table_name="asset_risk_scores")
    op.drop_table("asset_risk_scores")

    op.drop_index(op.f("ix_climatology_thresholds_month"), table_name="climatology_thresholds")
    op.drop_index(op.f("ix_climatology_thresholds_cell_lng"), table_name="climatology_thresholds")
    op.drop_index(op.f("ix_climatology_thresholds_cell_lat"), table_name="climatology_thresholds")
    op.drop_index(op.f("ix_climatology_thresholds_climatology_version"), table_name="climatology_thresholds")
    op.drop_table("climatology_thresholds")

    op.drop_index(op.f("ix_climatology_runs_status"), table_name="climatology_runs")
    op.drop_index(op.f("ix_climatology_runs_climatology_version"), table_name="climatology_runs")
    op.drop_table("climatology_runs")

