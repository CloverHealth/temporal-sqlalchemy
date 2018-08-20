import pytest
import sqlalchemy as sa
from sqlalchemy.inspection import inspect as sa_inspect

import temporal_sqlalchemy as temporal
from temporal_sqlalchemy.clock import (
    build_history_table,
    build_history_class,
    build_clock_class,
    build_clock_table)
from temporal_sqlalchemy.core import TemporalModel

from . import models


def test_build_history_table():
    rel_id_prop = sa.inspect(models.RelationalTemporalModel.rel_id).property
    rel_prop = sa.inspect(models.RelationalTemporalModel.rel).property

    history_table = build_history_table(
        models.RelationalTemporalModel, rel_id_prop, models.TEMPORAL_SCHEMA)

    assert history_table == build_history_table(
        models.RelationalTemporalModel,
        rel_prop,
        models.TEMPORAL_SCHEMA)
    assert history_table.name == 'relational_temporal_history_rel_id'
    assert history_table.schema == models.TEMPORAL_SCHEMA
    assert history_table.c.keys() == [
        'id', 'effective', 'vclock', 'entity_id', 'rel_id',
    ]


def test_build_history_class():
    rel_id_prop = sa.inspect(models.SimpleTable.rel_id).property
    rel_prop = sa.inspect(models.SimpleTable.rel).property

    rel_id_prop_class = build_history_class(models.SimpleTable, rel_id_prop)
    rel_prop_class = build_history_class(models.SimpleTable, rel_prop)

    assert rel_id_prop_class.__table__ == rel_prop_class.__table__
    assert rel_id_prop_class.__name__ == 'SimpleTableHistory_rel_id'
    assert hasattr(rel_id_prop_class, 'entity')


def test_build_clock_table():
    clock_table = TemporalModel.build_clock_table(
        models.RelationalTemporalModel.__table__,
        sa.MetaData(),
        models.TEMPORAL_SCHEMA,
    )

    assert clock_table.name == 'relational_temporal_clock'
    assert clock_table.schema == models.TEMPORAL_SCHEMA
    assert clock_table.c.keys() == ['id', 'tick', 'timestamp', 'entity_id']


def test_build_clock_class():
    clock = build_clock_class(
        'Testing', sa.MetaData(), {'__tablename__': 'test'})

    assert clock.__name__ == 'TestingClock'
    assert issubclass(clock, temporal.EntityClock)
    actual_primary_keys = [k.name for k in sa_inspect(clock).primary_key]
    assert actual_primary_keys == ['id']


@pytest.mark.parametrize('table,expected_name,expected_cols,activity_class', (
    (
        sa.Table(
            'bare_table_single_pk_no_activity',
            sa.MetaData(),
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('description', sa.Text),
            schema='bare_table_test_schema',
        ),
        'bare_table_single_pk_no_activity_clock',
        {'id', 'tick', 'timestamp', 'entity_id'},
        None,
    ),
    (
        sa.Table(
            'bare_table_compositve_pk_no_activity',
            sa.MetaData(),
            sa.Column('num_id', sa.Integer, primary_key=True),
            sa.Column('text_id', sa.Text, primary_key=True),
            sa.Column('description', sa.Text),
            schema='bare_table_test_schema',
        ),
        'bare_table_compositve_pk_no_activity_clock',
        {'id', 'tick', 'timestamp', 'entity_num_id', 'entity_text_id'},
        None,
    ),
    (
        sa.Table(
            'bare_table_single_pk_with_activity',
            sa.MetaData(),
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('description', sa.Text),
            schema='bare_table_test_schema',
        ),
        'bare_table_single_pk_with_activity_clock',
        {'id', 'tick', 'timestamp', 'entity_id', 'activity_id'},
        models.Activity,
    ),
    (
        sa.Table(
            'bare_table_compositve_pk_with_activity',
            sa.MetaData(),
            sa.Column('num_id', sa.Integer, primary_key=True),
            sa.Column('text_id', sa.Text, primary_key=True),
            sa.Column('description', sa.Text),
            schema='bare_table_test_schema',
        ),
        'bare_table_compositve_pk_with_activity_clock',
        {'id', 'tick', 'timestamp', 'entity_num_id', 'entity_text_id', 'activity_id'},
        models.Activity,
    ),
))
def test_build_clock_tables(table, expected_name, expected_cols, activity_class):
    clock_table = build_clock_table(
        table,
        table.metadata,
        table.schema,
        activity_class,
    )
    assert clock_table.name == expected_name
    assert clock_table.metadata is table.metadata
    assert {c.key for c in clock_table.c} == expected_cols
    for foreign_key in clock_table.foreign_keys:
        references_entity = foreign_key.references(table)
        if activity_class:
            assert foreign_key.references(activity_class.__table__) or references_entity
        else:
            assert references_entity
