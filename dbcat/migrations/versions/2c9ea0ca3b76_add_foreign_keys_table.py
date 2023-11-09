"""add foreign keys table

Revision ID: 2c9ea0ca3b76
Revises: 0fed90ee2030
Create Date: 2023-11-08 15:52:24.121812

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2c9ea0ca3b76'
down_revision = '0fed90ee2030'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "foreign_keys",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("source_column_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["source_column_id"], ["columns.id"], ),
        sa.Column("target_column_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["target_column_id"], ["columns.id"], ),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade():
    op.drop_table("foreign_keys")
