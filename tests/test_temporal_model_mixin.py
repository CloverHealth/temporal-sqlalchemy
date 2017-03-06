import pytest
import sqlalchemy as sa
import temporal_sqlalchemy

from . import shared, models


def test_declaration_check():
    with pytest.raises(AssertionError):
        class Error(models.Base, temporal_sqlalchemy.TemporalModel):
            __tablename__ = 'new_style_temporal_model'
            __table_args__ = {'schema': models.SCHEMA}

            id = models.auto_uuid()
            description = sa.Column(sa.TEXT)


def test_create_temporal_options():
    assert hasattr(models.NewStyleModel, 'temporal_options')

    m = models.NewStyleModel()

    assert hasattr(m, 'temporal_options')
    assert m.temporal_options is models.NewStyleModel.temporal_options
    assert isinstance(m.temporal_options, temporal_sqlalchemy.ClockedOption)


@pytest.mark.parametrize('table,expected_name,expected_cols,activity_class', (
    (
        sa.Table(
            'bare_table_single_pk_no_activity',
            sa.MetaData(),
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('description', sa.Text),
            schema='bare_table_test_schema'
        ),
        'bare_table_single_pk_no_activity_clock',
        {'tick', 'timestamp', 'entity_id'},
        None
    ),
    (
        sa.Table(
            'bare_table_compositve_pk_no_activity',
            sa.MetaData(),
            sa.Column('num_id', sa.Integer, primary_key=True),
            sa.Column('text_id', sa.Text, primary_key=True),
            sa.Column('description', sa.Text),
            schema='bare_table_test_schema'
        ),
        'bare_table_compositve_pk_no_activity_clock',
        {'tick', 'timestamp', 'entity_num_id', 'entity_text_id'},
        None
    ),
    (
        sa.Table(
            'bare_table_single_pk_with_activity',
            sa.MetaData(),
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('description', sa.Text),
            schema='bare_table_test_schema'
        ),
        'bare_table_single_pk_with_activity_clock',
        {'tick', 'timestamp', 'entity_id', 'activity_id'},
        models.Activity
    ),
    (
        sa.Table(
            'bare_table_compositve_pk_with_activity',
            sa.MetaData(),
            sa.Column('num_id', sa.Integer, primary_key=True),
            sa.Column('text_id', sa.Text, primary_key=True),
            sa.Column('description', sa.Text),
            schema='bare_table_test_schema'
        ),
        'bare_table_compositve_pk_with_activity_clock',
        {'tick', 'timestamp', 'entity_num_id', 'entity_text_id', 'activity_id'},
        models.Activity
    )
))
def test_build_clock_table(table, expected_name, expected_cols, activity_class):
    clock_table = temporal_sqlalchemy.TemporalModel.build_clock_table(table,
                                                                      table.metadata,
                                                                      table.schema,
                                                                      activity_class)
    assert clock_table.name == expected_name
    assert clock_table.metadata is table.metadata
    assert {c.key for c in clock_table.c} == expected_cols
    for foreign_key in clock_table.foreign_keys:
        references_entity = foreign_key.references(table)
        if activity_class:
            assert foreign_key.references(activity_class.__table__) or references_entity
        else:
            assert references_entity


def test_creates_clock_model():
    options = models.NewStyleModel.temporal_options

    clock_model = options.clock_model
    assert (clock_model.__table__.name == '%s_clock' % models.NewStyleModel.__table__.name)

    inspected = sa.inspect(clock_model)
    assert 'entity' in inspected.relationships
    entity_rel = inspected.relationships['entity']
    assert entity_rel.target is models.NewStyleModel.__table__


class TestTemporalModelMixin(shared.DatabaseTest):
    @pytest.fixture(autouse=True)
    def setup(self, session):
        models.basic_metadata.create_all(session.bind)

    def test_creates_clock_table(self, session):
        options = models.NewStyleModel.temporal_options

        clock_table = options.clock_model.__table__
        assert self.has_table(session.bind, clock_table.name, schema=clock_table.schema)

    def test_create_history_tables(self, session):
        table_name = models.NewStyleModel.__table__.name
        # sanity check the current state table first
        assert self.has_table(session.bind, table_name, schema=models.SCHEMA)
        # then check the history tables
        assert self.has_table(session.bind, '%s_history_description' % table_name)
        assert self.has_table(session.bind, '%s_history_int_prop' % table_name)
        assert self.has_table(session.bind, '%s_history_bool_prop' % table_name)
        assert self.has_table(session.bind, '%s_history_datetime_prop' % table_name)
