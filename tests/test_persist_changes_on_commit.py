import pytest

import temporal_sqlalchemy as temporal

from . import shared, models


class TestPersistChangesOnCommit(shared.DatabaseTest):
    @pytest.fixture(autouse=True)
    def setup_batched_session(self, session):
        temporal.temporal_session(session, persist_on_commit=True)

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
        assert session.transaction.nested is False
        session.begin_nested()
        assert session.transaction.nested is True

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

        assert session.transaction.nested is True
        session.commit()
        assert session.transaction.nested is False

        assert history_query.count() == 1
        history_result = history_query.first()
        assert history_result.prop_a == 1234

        session.commit()