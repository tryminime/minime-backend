"""Add is_superadmin column and create admin user

Revision ID: 003
Revises: 002
Create Date: 2026-03-18

"""
from alembic import op
import sqlalchemy as sa

revision = '003'
down_revision = '001'
branch_labels = None
depends_on = None


def upgrade():
    # Add is_superadmin column
    op.add_column('users', sa.Column('is_superadmin', sa.Boolean(), server_default='false', nullable=False))

    # Create admin user (password will be set via the app's registration/update flow)
    # We'll handle admin user creation in a startup script instead
    pass


def downgrade():
    op.drop_column('users', 'is_superadmin')
