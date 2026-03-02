"""add doy climatology thresholds

Revision ID: 0006_climatology_doy
Revises: 0005_climatology_scores
Create Date: 2026-03-02
"""

from alembic import op
import sqlalchemy as sa


revision = "0006_climatology_doy"
down_revision = "0005_climatology_scores"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "climatology_thresholds_doy",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("climatology_version", sa.String(length=128), nullable=False),
        sa.Column("cell_lat", sa.Float(), nullable=False),
        sa.Column("cell_lng", sa.Float(), nullable=False),
        sa.Column("doy", sa.Integer(), nullable=False),
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
        sa.UniqueConstraint("climatology_version", "cell_lat", "cell_lng", "doy", name="uq_clim_version_cell_doy"),
    )
    op.create_index(op.f("ix_climatology_thresholds_doy_climatology_version"), "climatology_thresholds_doy", ["climatology_version"], unique=False)
    op.create_index(op.f("ix_climatology_thresholds_doy_cell_lat"), "climatology_thresholds_doy", ["cell_lat"], unique=False)
    op.create_index(op.f("ix_climatology_thresholds_doy_cell_lng"), "climatology_thresholds_doy", ["cell_lng"], unique=False)
    op.create_index(op.f("ix_climatology_thresholds_doy_doy"), "climatology_thresholds_doy", ["doy"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_climatology_thresholds_doy_doy"), table_name="climatology_thresholds_doy")
    op.drop_index(op.f("ix_climatology_thresholds_doy_cell_lng"), table_name="climatology_thresholds_doy")
    op.drop_index(op.f("ix_climatology_thresholds_doy_cell_lat"), table_name="climatology_thresholds_doy")
    op.drop_index(op.f("ix_climatology_thresholds_doy_climatology_version"), table_name="climatology_thresholds_doy")
    op.drop_table("climatology_thresholds_doy")

