# pylint: disable=missing-docstring, no-self-use
import datetime

import pytest
import sqlalchemy as sa
import sqlalchemy.exc as sa_exc
import temporal_sqlalchemy as temporal

from . import shared, models


def test_declaration_check():
    with pytest.raises(AssertionError):
        class Error(models.Base, temporal.TemporalModel):  # pylint: disable=unused-variable
            __tablename__ = 'new_style_temporal_model'
            __table_args__ = {'schema': models.SCHEMA}

            id = models.auto_uuid()
            description = sa.Column(sa.TEXT)


def test_create_temporal_options():
    assert hasattr(models.NewStyleModel, 'temporal_options')

    m = models.NewStyleModel(activity=models.Activity(description="Activity Description"))

    assert hasattr(m, 'temporal_options')
    assert m.temporal_options is models.NewStyleModel.temporal_options
    assert isinstance(m.temporal_options, temporal.TemporalOption)


def test_creates_clock_model():
    options = models.NewStyleModel.temporal_options

    clock_model = options.clock_model
    assert clock_model.__table__.name == '%s_clock' % models.NewStyleModel.__table__.name

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
            datetime_prop=datetime.datetime.now(datetime.timezone.utc),
        )

    def test_creates_clock_table(self):
        options = models.NewStyleModel.temporal_options

        clock_table = options.clock_model.__table__
        assert self.has_table(
            self.connection,
            clock_table.name,
            schema=clock_table.schema,
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

        for attr, backref, history_model in [
            ('description', 'description_history'),
            ('int_prop', 'int_prop_history'),
            ('bool_prop', 'bool_prop_history'),
            ('datetime_prop', 'datetime_prop_history'),
        ]:
            history_model = temporal.get_history_model(getattr(models.NewStyleModel, 'attr'))
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
        clock_model = models.NewStyleModel.temporal_options.clock_model
        session.add(newstylemodel)
        session.commit()

        tick = session.query(clock_model).filter_by(tick=1, entity_id=newstylemodel.id).one()
        assert newstylemodel.vclock == 1
        assert newstylemodel.clock.count() == 1
        assert newstylemodel.date_created == tick.timestamp

    def test_date_modified(self, session, newstylemodel):
        clock_model = models.NewStyleModel.temporal_options.clock_model
        session.add(newstylemodel)
        session.commit()

        first_tick = session.query(clock_model).filter_by(tick=1, entity_id=newstylemodel.id).one()
        assert newstylemodel.vclock == 1
        assert newstylemodel.clock.count() == 1
        assert newstylemodel.date_created == first_tick.timestamp

        activity = models.Activity(description="Activity Description #2")
        with newstylemodel.clock_tick(activity=activity):
            newstylemodel.description = "this is new"

        session.commit()

        second_tick = session.query(clock_model).filter_by(tick=1, entity_id=newstylemodel.id).one()
        assert newstylemodel.vclock == 2
        assert newstylemodel.clock.count() == 2
        assert newstylemodel.date_created == first_tick.timestamp
        assert newstylemodel.date_modified == second_tick.timestamp

    def test_clock_tick_editing(self, session, newstylemodel):
        clock_model = models.NewStyleModel.temporal_options.clock_model

        session.add(newstylemodel)
        session.commit()

        activity = models.Activity(description="Activity Description #2")
        with newstylemodel.clock_tick(activity=activity):
            newstylemodel.description = "this is new"
            newstylemodel.int_prop = 2
            newstylemodel.bool_prop = False
            newstylemodel.datetime_prop = datetime.datetime(2017, 2, 10)

        session.commit()

        t = session.query(models.NewStyleModel).first()
        clock_query = session.query(clock_model)
        assert clock_query.count() == 2

        create_clock = clock_query.first()
        update_clock = clock_query.order_by(
            clock_model.timestamp.desc()).first()
        assert create_clock.timestamp == t.date_created
        assert update_clock.timestamp == t.date_modified

        assert t.vclock == 2
        assert t.clock.count() == 2

        clock = (
            clock_query
            .order_by(clock_model.tick.desc())
            .first())
        for history_model in (models.NewStyleModel
                              .temporal_options.history_models.values()):
            clock_query = session.query(history_model).count()
            assert clock_query == 2

            history = (
                session.query(history_model)
                .order_by(history_model.vclock.desc()).first())
            assert clock.tick in history.vclock

    def test_disallaw_same_tick_for_same_entity(self, session, newstylemodel):
        clock_model = models.NewStyleModel.temporal_options.clock_model

        session.add(newstylemodel)
        session.commit()

        first_tick = session.query(clock_model).first()
        duplicate_tick = clock_model(
            tick=first_tick.tick,
            entity_id=first_tick.entity_id,
            activity=models.Activity(description="Inserting a duplicate"),
        )
        session.add(duplicate_tick)
        with pytest.raises(sa_exc.IntegrityError):
            session.commit()
