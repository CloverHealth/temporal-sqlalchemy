from psycopg2.extras import NumericRange
import sqlalchemy as sa
import sqlalchemy.orm as orm
import pytest

import temporal_sqlalchemy as temporal
from temporal_sqlalchemy.metadata import CHANGESET_STACK_KEY
from . import shared, models


@pytest.yield_fixture()
def non_temporal_session(connection):
    sessionmaker = orm.sessionmaker()

    transaction = connection.begin()
    sess = sessionmaker(bind=connection)

    yield sess

    transaction.rollback()
    sess.close()

    sess.close_all()


@pytest.yield_fixture()
def second_session(connection: sa.engine.Connection, sessionmaker: orm.sessionmaker):
    transaction = connection.begin()
    sess = sessionmaker(bind=connection)

    yield sess

    transaction.rollback()
    sess.close()


class TestPersistChangesOnCommit(shared.DatabaseTest):
    def test_persist_on_commit(self, session):
        activity = models.Activity(description='Create temp')
        session.add(activity)

        t = models.PersistOnCommitTable(prop_a=1234, activity=activity)
        session.add(t)
        session.flush()

        activity_query = session.query(models.Activity)
        assert activity_query.count() == 1
        activity_result = activity_query.first()
        assert activity_result.description == 'Create temp'

        clock_query = session.query(
            models.PersistOnCommitTable.temporal_options.clock_table)
        assert clock_query.count() == 1
        clock_result = clock_query.first()
        assert clock_result.activity_id == activity_result.id

        history_query = session.query(
            models.PersistOnCommitTable.temporal_options.history_models[
                models.PersistOnCommitTable.prop_a.property])
        assert history_query.count() == 0

        session.commit()

        assert history_query.count() == 1
        history_result = history_query.first()
        assert history_result.prop_a == 1234

    def test_persist_no_changes(self, non_temporal_session):
        """temporalize after transaction has started to cover some additional edge cases"""
        temporal.temporal_session(non_temporal_session)

    def test_persist_no_temporal_changes(self, non_temporal_session):
        """temporalize after transaction has started to cover some additional edge cases"""
        session = temporal.temporal_session(non_temporal_session)

        t = models.NonTemporalTable()
        session.add(t)

        session.commit()

    def test_no_session_cross_pollution(self, session, second_session):
        """make sure the junk from one session doesn't cross pollute another session"""
        activity_1 = models.Activity(description='Create temp')
        session.add(activity_1)

        t_1 = models.PersistOnCommitTable(prop_a=1234, activity=activity_1)
        session.add(t_1)
        session.flush()

        t_1.prop_a = 4567

        assert session is not second_session
        # current changeset is blank (not being contaminated by other sessions)
        assert len(second_session.info[CHANGESET_STACK_KEY][-1]) == 0

    def test_persist_only_last_change_before_flush(self, session):
        activity = models.Activity(description='Create temp')
        session.add(activity)

        t = models.PersistOnCommitTable(prop_a=1234, activity=activity)
        session.add(t)

        t.prop_a = 4567

        session.flush()

        history_query = session.query(
            models.PersistOnCommitTable.temporal_options.history_models[
                models.PersistOnCommitTable.prop_a.property])
        assert history_query.count() == 0

        session.commit()

        assert history_query.count() == 1
        history_result = history_query.first()
        assert history_result.prop_a == 4567

    def test_persist_only_last_change_after_flush(self, session):
        activity = models.Activity(description='Create temp')
        session.add(activity)

        t = models.PersistOnCommitTable(prop_a=1234, activity=activity)
        session.add(t)
        session.flush()

        t.prop_a = 4567

        history_query = session.query(
            models.PersistOnCommitTable.temporal_options.history_models[
                models.PersistOnCommitTable.prop_a.property])
        assert history_query.count() == 0

        session.commit()

        assert history_query.count() == 1
        history_result = history_query.first()
        assert history_result.prop_a == 4567

    def test_mixed_models_persist_on_commit_and_regular_persist(self, session):
        activity = models.Activity(description='Create temp')
        session.add(activity)

        t1 = models.PersistOnCommitTable(prop_a=1234, activity=activity)
        session.add(t1)
        t2 = models.PersistOnFlushTable(prop_a=1234, activity=activity)
        session.add(t2)
        session.flush()

        activity_query = session.query(models.Activity)
        assert activity_query.count() == 1
        activity_result = activity_query.first()
        assert activity_result.description == 'Create temp'

        # check persist on commit works
        clock_query_1 = session.query(
            models.PersistOnCommitTable.temporal_options.clock_table)
        assert clock_query_1.count() == 1
        clock_result_1 = clock_query_1.first()
        assert clock_result_1.activity_id == activity_result.id

        history_query_1 = session.query(
            models.PersistOnCommitTable.temporal_options.history_models[
                models.PersistOnCommitTable.prop_a.property])
        assert history_query_1.count() == 0

        # check persist on flush works
        clock_query_2 = session.query(
            models.PersistOnFlushTable.temporal_options.clock_table)
        assert clock_query_2.count() == 1
        clock_result_2 = clock_query_2.first()
        assert clock_result_2.activity_id == activity_result.id

        history_query_2 = session.query(
            models.PersistOnFlushTable.temporal_options.history_models[
                models.PersistOnFlushTable.prop_a.property])
        assert history_query_2.count() == 1
        history_result_2 = history_query_2.first()
        assert history_result_2.prop_a == 1234

        session.commit()

        # check persist on commit works again
        assert history_query_1.count() == 1
        history_result_1 = history_query_1.first()
        assert history_result_1.prop_a == 1234

    def test_persist_on_commit_enabled_with_regular_persist(self, session):
        activity = models.Activity(description='Create temp')
        session.add(activity)

        t = models.PersistOnFlushTable(prop_a=1234, activity=activity)
        session.add(t)
        session.flush()

        activity_query = session.query(models.Activity)
        assert activity_query.count() == 1
        activity_result = activity_query.first()
        assert activity_result.description == 'Create temp'

        # check persist on flush works
        clock_query = session.query(
            models.PersistOnFlushTable.temporal_options.clock_table)
        assert clock_query.count() == 1
        clock_result = clock_query.first()
        assert clock_result.activity_id == activity_result.id

        history_query = session.query(
            models.PersistOnFlushTable.temporal_options.history_models[
                models.PersistOnFlushTable.prop_a.property])
        assert history_query.count() == 1
        history_result = history_query.first()
        assert history_result.prop_a == 1234

        session.commit()

    def test_persist_multiple_rows(self, session):
        activity = models.Activity(description='Create temp')
        session.add(activity)

        t1 = models.PersistOnCommitTable(prop_a=1234, activity=activity)
        session.add(t1)
        t2 = models.PersistOnCommitTable(prop_a=5678, activity=activity)
        session.add(t2)
        session.flush()

        activity_query = session.query(models.Activity)
        assert activity_query.count() == 1
        activity_result = activity_query.first()
        assert activity_result.description == 'Create temp'

        clock_query = session.query(
            models.PersistOnCommitTable.temporal_options.clock_table)
        assert clock_query.count() == 2
        clock_result = clock_query.first()
        assert clock_result.activity_id == activity_result.id

        history_query = session.query(
            models.PersistOnCommitTable.temporal_options.history_models[
                models.PersistOnCommitTable.prop_a.property])
        assert history_query.count() == 0

        session.commit()

        assert history_query.count() == 2
        history_result_1 = history_query.filter_by(entity_id=t1.id).one()
        assert history_result_1.prop_a == 1234
        history_result_2 = history_query.filter_by(entity_id=t2.id).one()
        assert history_result_2.prop_a == 5678

    def test_persist_when_inside_nested_transaction(self, session):
        outer_activity = models.Activity(description='Create temp')
        session.add(outer_activity)

        outer_t = models.PersistOnCommitTable(prop_a=5678, activity=outer_activity)
        session.add(outer_t)
        session.flush()

        history_query = session.query(
            models.PersistOnCommitTable.temporal_options.history_models[
                models.PersistOnCommitTable.prop_a.property])

        assert history_query.count() == 0

        assert session.transaction.nested is False
        session.begin_nested()
        assert session.transaction.nested is True

        activity = models.Activity(description='Create temp')
        session.add(activity)

        t = models.PersistOnCommitTable(prop_a=1234, activity=activity)
        session.add(t)
        session.flush()

        activity_query = session.query(models.Activity)
        assert activity_query.count() == 2

        clock_query = session.query(
            models.PersistOnCommitTable.temporal_options.clock_table)
        assert clock_query.count() == 2

        assert history_query.count() == 0

        assert session.transaction.nested is True
        session.commit()
        assert session.transaction.nested is False

        assert history_query.count() == 1
        history_result = history_query.filter_by(prop_a=1234).one()
        assert history_result.prop_a == 1234

        session.commit()

        assert history_query.count() == 2
        history_result = history_query.filter_by(prop_a=5678).one()
        assert history_result.prop_a == 5678

    def test_persist_when_inside_nested_transaction_with_rollback(self, session):
        outer_activity = models.Activity(description='Create temp')
        session.add(outer_activity)

        outer_t = models.PersistOnCommitTable(prop_a=5678, activity=outer_activity)
        session.add(outer_t)
        session.flush()

        history_query = session.query(
            models.PersistOnCommitTable.temporal_options.history_models[
                models.PersistOnCommitTable.prop_a.property])

        assert history_query.count() == 0

        assert session.transaction.nested is False
        session.begin_nested()
        assert session.transaction.nested is True

        activity = models.Activity(description='Create temp')
        session.add(activity)

        t = models.PersistOnCommitTable(prop_a=1234, activity=activity)
        session.add(t)
        session.flush()

        activity_query = session.query(models.Activity)
        assert activity_query.count() == 2

        clock_query = session.query(
            models.PersistOnCommitTable.temporal_options.clock_table)
        assert clock_query.count() == 2

        assert history_query.count() == 0

        assert session.transaction.nested is True
        session.rollback()
        assert session.transaction.nested is False

        assert history_query.count() == 0

        session.commit()

        assert history_query.count() == 1
        history_result = history_query.first()
        assert history_result.prop_a == 5678

    def test_persist_on_commit_with_edit_inside_clock_tick(self, session):
        create_activity = models.Activity(description='Create temp')
        session.add(create_activity)

        t = models.PersistOnCommitTable(prop_a=1234, activity=create_activity)
        session.add(t)

        session.commit()

        history_table = models.PersistOnCommitTable.temporal_options.history_models[
            models.PersistOnCommitTable.prop_a.property]

        history_query = session.query(history_table).order_by(history_table.prop_a)
        assert history_query.count() == 1

        edit_activity = models.Activity(description='Edit temp')
        session.add(edit_activity)

        with t.clock_tick(edit_activity):
            t.prop_a = 9876

        session.flush()
        assert history_query.count() == 1

        session.commit()
        activity_query = session.query(models.Activity).order_by(models.Activity.description)
        assert activity_query.count() == 2
        activity_results = activity_query.all()
        assert activity_results[0].description == 'Create temp'
        assert activity_results[1].description == 'Edit temp'

        clock_query = session.query(
            models.PersistOnCommitTable.temporal_options.clock_table).order_by(
                models.PersistOnCommitTable.temporal_options.clock_table.tick)
        assert clock_query.count() == 2
        clock_results = clock_query.all()
        assert clock_results[0].activity_id == activity_results[0].id
        assert clock_results[1].activity_id == activity_results[1].id

        assert history_query.count() == 2
        history_results = history_query.all()
        assert history_results[0].prop_a == 1234
        assert history_results[0].vclock == NumericRange(1, 2, '[)')
        assert history_results[1].prop_a == 9876
        assert history_results[1].vclock == NumericRange(2, None, '[)')

    def test_persist_on_commit_with_edit_outside_clock_tick(self, session):
        create_activity = models.Activity(description='Create temp')
        session.add(create_activity)

        t = models.PersistOnCommitTable(prop_a=1234, activity=create_activity)
        session.add(t)

        session.commit()

        history_table = models.PersistOnCommitTable.temporal_options.history_models[
            models.PersistOnCommitTable.prop_a.property]

        history_query = session.query(history_table).order_by(history_table.prop_a)
        assert history_query.count() == 1

        edit_activity = models.Activity(description='Edit temp')
        session.add(edit_activity)

        with t.clock_tick(edit_activity):
            t.prop_a = 9876

        session.flush()
        assert history_query.count() == 1

        # we're setting this outside a clock tick, which should get picked up by the history builder
        t.prop_a = 5678

        session.commit()
        activity_query = session.query(models.Activity).order_by(models.Activity.description)
        assert activity_query.count() == 2
        activity_results = activity_query.all()
        assert activity_results[0].description == 'Create temp'
        assert activity_results[1].description == 'Edit temp'

        clock_query = session.query(
            models.PersistOnCommitTable.temporal_options.clock_table).order_by(
                models.PersistOnCommitTable.temporal_options.clock_table.tick)
        assert clock_query.count() == 2
        clock_results = clock_query.all()
        assert clock_results[0].activity_id == activity_results[0].id
        assert clock_results[1].activity_id == activity_results[1].id

        assert history_query.count() == 2
        history_results = history_query.all()
        assert history_results[0].prop_a == 1234
        assert history_results[0].vclock == NumericRange(1, 2, '[)')
        assert history_results[1].prop_a == 5678
        assert history_results[1].vclock == NumericRange(2, None, '[)')

    def test_persist_on_commit_with_edit_no_clock_tick_no_strict_mode(self, session):
        create_activity = models.Activity(description='Create temp')
        session.add(create_activity)

        t = models.PersistOnCommitTable(prop_a=1234, activity=create_activity)
        session.add(t)

        session.commit()

        history_table = models.PersistOnCommitTable.temporal_options.history_models[
            models.PersistOnCommitTable.prop_a.property]

        history_query = session.query(history_table).order_by(history_table.prop_a)
        assert history_query.count() == 1

        session.flush()
        assert history_query.count() == 1

        # we're setting this outside a clock tick, which won't get picked up by the history builder
        # since we never used clock_tick
        t.prop_a = 5678

        session.commit()
        activity_query = session.query(models.Activity).order_by(models.Activity.description)
        assert activity_query.count() == 1
        activity_result = activity_query.first()
        assert activity_result.description == 'Create temp'

        clock_query = session.query(
            models.PersistOnCommitTable.temporal_options.clock_table).order_by(
                models.PersistOnCommitTable.temporal_options.clock_table.tick)
        assert clock_query.count() == 1
        clock_result = clock_query.first()
        assert clock_result.activity_id == activity_result.id

        assert history_query.count() == 2
        history_results = history_query.all()
        assert history_results[0].prop_a == 1234
        # this is bad
        assert history_results[0].vclock == NumericRange(empty=True)

    def test_persist_on_commit_with_edit_no_clock_tick_with_strict_mode(self, session):
        temporal.temporal_session(session, strict_mode=True)

        create_activity = models.Activity(description='Create temp')
        session.add(create_activity)

        t = models.PersistOnCommitTable(prop_a=1234, activity=create_activity)
        session.add(t)

        session.commit()

        history_table = models.PersistOnCommitTable.temporal_options.history_models[
            models.PersistOnCommitTable.prop_a.property]

        history_query = session.query(history_table).order_by(history_table.prop_a)
        assert history_query.count() == 1

        session.flush()
        assert history_query.count() == 1

        # we're setting this outside a clock tick, which won't get picked up by the history builder
        # since we never used clock_tick
        t.prop_a = 5678

        with pytest.raises(AssertionError) as excinfo:
            session.commit()
