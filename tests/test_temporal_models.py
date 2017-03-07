import datetime

import psycopg2.extras as psql_extras
import sqlalchemy as sa

import temporal_sqlalchemy as temporal

from . import shared, models


class TestTemporalModels(shared.DatabaseTest):

    def test_temporal_options_class(self):
        options = models.SimpleTableTemporal.temporal_options

        assert isinstance(options, temporal.ClockedOption)

        clock_table = options.clock_table
        assert (clock_table.__table__.name
                == '%s_clock' % models.SimpleTableTemporal.__table__.name)

        inspected = sa.inspect(clock_table)
        assert 'entity' in inspected.relationships
        entity_rel = inspected.relationships['entity']
        assert entity_rel.target is models.SimpleTableTemporal.__table__

    def test_temporal_options_instance(self):
        temp = models.SimpleTableTemporal(prop_a=1, prop_b=2)

        assert (temp.temporal_options
                is models.SimpleTableTemporal.temporal_options)

    def test_creates_temporal_tables(self):
        table_name = models.SimpleTableTemporal.__table__.name

        assert self.has_table(self.connection, table_name, schema=models.SCHEMA)
        assert self.has_table(self.connection, '%s_clock' % table_name)
        assert self.has_table(self.connection, '%s_history_prop_a' % table_name)
        assert self.has_table(self.connection, '%s_history_prop_b' % table_name)

    def test_init_adds_clock_tick(self, session):
        clock_query = session.query(
            models.SimpleTableTemporal.temporal_options.clock_table).count()
        assert clock_query == 0
        t = models.SimpleTableTemporal(prop_a=1, prop_b='foo')
        assert t.clock.count() == 1

        session.add(t)
        session.commit()

        t = session.query(models.SimpleTableTemporal).first()
        clock_query = session.query(
            models.SimpleTableTemporal.temporal_options.clock_table)
        assert clock_query.count() == 1
        assert t.vclock == 1
        assert t.clock.count() == 1

        clock = clock_query.first()

        prop_a_history_model = temporal.get_history_model(
            models.SimpleTableTemporal.prop_a)
        prop_b_history_model = temporal.get_history_model(
            models.SimpleTableTemporal.prop_b)

        for attr, history_table in [
                ('prop_a', prop_a_history_model),
                ('prop_b', prop_b_history_model),
                ]:
            clock_query = session.query(history_table).count()
            assert clock_query == 1, "missing entry for %r" % history_table

            history = session.query(history_table).first()
            assert clock.tick in history.vclock
            assert getattr(history, attr) == getattr(t, attr)

    def test_clock_tick_editing(self, session):
        clock_table = models.SimpleTableTemporal.temporal_options.clock_table
        t = models.SimpleTableTemporal(
            prop_a=1,
            prop_b='foo',
            prop_c=datetime.datetime(2016, 5, 11, 1, 2, 3,
                                     tzinfo=datetime.timezone.utc),
            prop_d={'foo': 'old value'},
            prop_e=psql_extras.DateRange(datetime.date(2016, 1, 1),
                                         datetime.date(2016, 1, 10)),
            prop_f=['old', 'stuff']
        )

        session.add(t)
        session.commit()

        with t.clock_tick():
            t.prop_a = 2
            t.prop_b = 'bar'
            t.prop_c = datetime.datetime.now(tz=datetime.timezone.utc)
            t.prop_d['foo'] = 'new value'
            t.prop_e = psql_extras.DateRange(datetime.date(2016, 2, 1),
                                             datetime.date(2016, 2, 10))
            t.prop_f = ['new', 'stuff']

        session.commit()

        t = session.query(models.SimpleTableTemporal).first()
        clock_query = session.query(clock_table)
        assert clock_query.count() == 2

        create_clock = clock_query.first()
        update_clock = clock_query.order_by(
            clock_table.timestamp.desc()).first()
        assert create_clock.timestamp == t.date_created
        assert update_clock.timestamp == t.date_modified

        assert t.vclock == 2
        assert t.clock.count() == 2

        clock = (
            clock_query
            .order_by(models.SimpleTableTemporal
                      .temporal_options.clock_table.tick.desc())
            .first())
        for history_table in (models.SimpleTableTemporal
                              .temporal_options.history_tables.values()):
            clock_query = session.query(history_table).count()
            assert clock_query == 2

            history = (
                session.query(history_table)
                .order_by(history_table.vclock.desc()).first())
            assert clock.tick in history.vclock

    def test_clock_table_has_no_activity_columns(self):
        assert models.SimpleTableTemporal.temporal_options.activity_cls is None
        assert 'activity_id' not in (
            models.SimpleTableTemporal.temporal_options.clock_table.__dict__)
        assert 'activity' not in (
            models.SimpleTableTemporal.temporal_options.clock_table.__dict__)

    def test_default_parameters(self, session):
        t = models.TemporalTableWithDefault(prop_a=1, prop_b='foo')
        session.add(t)
        session.commit()

        t = session.query(models.TemporalTableWithDefault).first()
        clock_query = session.query(
            models.TemporalTableWithDefault.temporal_options.clock_table)
        assert clock_query.count() == 1
        assert t.vclock == 1
        assert t.clock.count() == 1
        # some sanity check
        assert t.prop_default == 10
        assert t.prop_callable, "a value here"
        assert isinstance(t.prop_func, datetime.datetime)

        clock = clock_query.first()
        history_tables = {
            'prop_func': temporal.get_history_model(
                models.TemporalTableWithDefault.prop_func),
            'prop_callable': temporal.get_history_model(
                models.TemporalTableWithDefault.prop_callable),
            'prop_default': temporal.get_history_model(
                models.TemporalTableWithDefault.prop_default),
            'prop_a': temporal.get_history_model(
                models.TemporalTableWithDefault.prop_a),
            'prop_b': temporal.get_history_model(
                models.TemporalTableWithDefault.prop_b),
        }
        for attr, history in history_tables.items():
            clock_query = session.query(history)
            assert clock_query.count() == 1, \
                "%r missing a history entry for initial value" % history

            recorded_history = clock_query.first()
            assert clock.tick in recorded_history.vclock
            assert getattr(t, attr) == getattr(recorded_history, attr)

    def test_multiple_edits(self, session):
        history_tables = {
            'prop_a': temporal.get_history_model(
                models.SimpleTableTemporal.prop_a),
            'prop_b': temporal.get_history_model(
                models.SimpleTableTemporal.prop_b),
        }

        t = models.SimpleTableTemporal(prop_a=1, prop_b='foo')
        session.add(t)
        session.commit()

        for attr, history in history_tables.items():
            clock_query = session.query(history)
            assert clock_query.count() == 1, \
                "%r missing a history entry for initial value" % history

            recorded_history = clock_query.first()
            assert 1 in recorded_history.vclock
            assert getattr(t, attr) == getattr(recorded_history, attr)

        with t.clock_tick():
            t.prop_a = 2
            t.prop_b = 'bar'
        session.commit()

        for attr, history in history_tables.items():
            clock_query = session.query(history)
            assert clock_query.count() == 2, \
                "%r missing a history entry for initial value" % history

            recorded_history = clock_query[-1]
            assert 2 in recorded_history.vclock
            assert getattr(t, attr) == getattr(recorded_history, attr)

        with t.clock_tick():
            t.prop_a = 3
            t.prop_b = 'foobar'
        session.commit()

        for attr, history in history_tables.items():
            clock_query = session.query(history)
            assert clock_query.count() == 3, \
                "%r missing a history entry for initial value" % history

            recorded_history = clock_query[-1]
            assert 3 in recorded_history.vclock
            assert getattr(t, attr) == getattr(recorded_history, attr)

    def test_doesnt_duplicate_unnecessary_history(self, session):
        history_tables = {
            'prop_a': temporal.get_history_model(
                models.SimpleTableTemporal.prop_a),
            'prop_b': temporal.get_history_model(
                models.SimpleTableTemporal.prop_b),
            'prop_c': temporal.get_history_model(
                models.SimpleTableTemporal.prop_c),
        }

        t = models.SimpleTableTemporal(
            prop_a=1,
            prop_b='foo',
            prop_c=datetime.datetime(2016, 5, 11,
                                     tzinfo=datetime.timezone.utc))

        session.add(t)
        session.commit()

        with t.clock_tick():
            t.prop_a = 1
            t.prop_c = datetime.datetime(2016, 5, 11,
                                         tzinfo=datetime.timezone.utc)

        session.commit()

        assert t.vclock == 1
        for attr, history in history_tables.items():
            clock_query = session.query(history)
            assert clock_query.count() == 1, \
                "%r missing a history entry for initial value" % history

            recorded_history = clock_query.first()
            assert 1 in recorded_history.vclock
            assert getattr(t, attr) == getattr(recorded_history, attr)
