"""make provinces.insured_assets bigint

Revision ID: 0002_assets_bigint
Revises: 0001_initial
Create Date: 2026-03-01
"""

from alembic import op
import sqlalchemy as sa


revision = "0002_assets_bigint"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "provinces",
        "insured_assets",
        existing_type=sa.Integer(),
        type_=sa.BigInteger(),
        existing_nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "provinces",
        "insured_assets",
        existing_type=sa.BigInteger(),
        type_=sa.Integer(),
        existing_nullable=False,
    )
