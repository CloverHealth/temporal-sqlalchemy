import datetime

import pytest
import sqlalchemy as sa
import temporal_sqlalchemy as temporal

from . import shared, models


def test_declaration_check():
    with pytest.raises(AssertionError):
        class Error(models.Base, temporal.TemporalModel):
            __tablename__ = 'new_style_temporal_model'
            __table_args__ = {'schema': models.SCHEMA}

            id = models.auto_uuid()
            description = sa.Column(sa.TEXT)


def test_create_temporal_options():
    assert hasattr(models.NewStyleModel, 'temporal_options')

    m = models.NewStyleModel()

    assert hasattr(m, 'temporal_options')
    assert m.temporal_options is models.NewStyleModel.temporal_options
    assert isinstance(m.temporal_options, temporal.ClockedOption)


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
    clock_table = temporal.TemporalModel.build_clock_table(
        table,
        table.metadata,
        table.schema,
        activity_class
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


def test_creates_clock_model():
    options = models.NewStyleModel.temporal_options

    clock_model = options.clock_model
    assert (clock_model.__table__.name == '%s_clock' % models.NewStyleModel.__table__.name)

    inspected = sa.inspect(clock_model)
    assert 'entity' in inspected.relationships
    entity_rel = inspected.relationships['entity']
    assert entity_rel.target is models.NewStyleModel.__table__


class TestTemporalModelMixin(shared.DatabaseTest):
    @pytest.fixture()
    def newstylemodel(self):
        return models.NewStyleModel(
            description="desc",
            int_prop=1,
            bool_prop=True,
            activity=models.Activity(description="Activity Description"),
            datetime_prop=datetime.datetime.now(datetime.timezone.utc)
        )

    def test_creates_clock_table(self):
        options = models.NewStyleModel.temporal_options

        clock_table = options.clock_model.__table__
        assert self.has_table(
            self.connection,
            clock_table.name,
            schema=clock_table.schema
        )

    def test_create_history_tables(self):
        table_name = models.NewStyleModel.__table__.name
        # sanity check the current state table first
        assert self.has_table(self.connection, table_name, schema=models.SCHEMA)
        # then check the history tables
        assert self.has_table(self.connection, '%s_history_description' % table_name)
        assert self.has_table(self.connection, '%s_history_int_prop' % table_name)
        assert self.has_table(self.connection, '%s_history_bool_prop' % table_name)
        assert self.has_table(self.connection, '%s_history_datetime_prop' % table_name)

    def test_init_adds_clock_tick(self, session, newstylemodel):
        clock_query = session.query(
            models.NewStyleModel.temporal_options.clock_model).count()
        assert clock_query == 0
        assert newstylemodel.clock.count() == 1

        session.add(newstylemodel)
        session.commit()

        t = session.query(models.NewStyleModel).first()
        clock_query = session.query(
            models.NewStyleModel.temporal_options.clock_model)
        assert clock_query.count() == 1
        assert t.vclock == 1
        assert t.clock.count() == 1

        clock = clock_query.first()

        desc_history_model = temporal.get_history_model(
            models.NewStyleModel.description)
        int_prop_history_model = temporal.get_history_model(
            models.NewStyleModel.int_prop)
        bool_prop_history_model = temporal.get_history_model(
            models.NewStyleModel.bool_prop)
        datetime_prop_history_model = temporal.get_history_model(
            models.NewStyleModel.datetime_prop)

        for attr, backref, history_model in [
            ('description', 'description_history', desc_history_model),
            ('int_prop', 'int_prop_history', int_prop_history_model),
            ('bool_prop', 'bool_prop_history', bool_prop_history_model),
            ('datetime_prop', 'datetime_prop_history', datetime_prop_history_model),
        ]:
            backref_history_query = getattr(t, backref)
            clock_query = session.query(history_model).count()
            assert clock_query == 1, "missing entry for %r" % history_model
            assert clock_query == backref_history_query.count()

            backref_history = backref_history_query[0]
            history = session.query(history_model).first()
            assert clock.tick in history.vclock
            assert clock.tick in backref_history.vclock
            assert getattr(history, attr) == getattr(t, attr) == getattr(backref_history, attr)

    def test_date_created(self, session, newstylemodel):
        session.add(newstylemodel)
        session.commit()

        clock_query = session.query(
            models.NewStyleModel.temporal_options.clock_model
        ).filter_by(entity=newstylemodel)
        assert clock_query.count() == 1
        assert newstylemodel.vclock == 1
        assert newstylemodel.clock.count() == 1
        assert newstylemodel.date_created == clock_query.first().timestamp
