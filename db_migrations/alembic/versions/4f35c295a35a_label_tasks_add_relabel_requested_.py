"""label_tasks_add_relabel_requested_timestamp

Revision ID: 4f35c295a35a
Revises: 529d2b0d07e8
Create Date: 2025-03-03 23:08:07.916278

"""
from typing import Union
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4f35c295a35a'
down_revision: Union[str, None] = '529d2b0d07e8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('label_tasks', sa.Column('relabel_requested_timestamp', sa.Integer(), nullable=False, server_default=sa.text('0')))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('label_tasks', 'relabel_requested_timestamp')
    # ### end Alembic commands ###
