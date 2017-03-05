import pytest
import sqlalchemy.exc as exc
import sqlalchemy.orm.attributes as orm_attr

import temporal_sqlalchemy as temporal

from . import shared, models


class TestTemporalWithActivity(shared.DatabaseTest):
    @pytest.fixture(autouse=True)
    def setup(self, session):
        models.basic_metadata.create_all(session.bind)

    def test_clock_table_has_activity_id(self):
        assert isinstance(
            models.FirstTemporalWithActivity
            .temporal_options.clock_table.activity_id,
            orm_attr.InstrumentedAttribute)
        assert isinstance(
            models.FirstTemporalWithActivity
            .temporal_options.clock_table.activity,
            orm_attr.InstrumentedAttribute)

    def test_no_activity_on_entity_create(self):
        with pytest.raises(ValueError):
            models.FirstTemporalWithActivity(column=1234)

    def test_activity_on_entity_create(self, session):
        activity = models.Activity(description='Create temp')
        session.add(activity)

        t = models.FirstTemporalWithActivity(column=1234, activity=activity)
        session.add(t)
        session.commit()

        activity_query = session.query(models.Activity)
        assert activity_query.count() == 1
        activity_result = activity_query.first()
        assert activity_result.description == 'Create temp'
        activity_clock_history = temporal.get_activity_clock_backref(
            activity_result, models.FirstTemporalWithActivity)
        assert getattr(activity_result, activity_clock_history.key)

        t = session.query(models.FirstTemporalWithActivity).first()
        assert t.vclock == 1
        assert t.clock.count() == 1

        clock_query = session.query(
            models.FirstTemporalWithActivity.temporal_options.clock_table)
        assert clock_query.count() == 1

        clock_result = clock_query.first()
        assert clock_result.activity_id == activity_result.id

    def test_no_activity_on_entity_edit(self, session):
        create_activity = models.Activity(description='Create temp')
        session.add(create_activity)

        t = models.FirstTemporalWithActivity(column=1234,
                                             activity=create_activity)
        session.add(t)
        session.commit()

        with pytest.raises(ValueError):
            with t.clock_tick():
                t.column = 4567

    def test_activity_on_entity_edit(self, session):
        create_activity = models.Activity(description='Create temp')
        session.add(create_activity)

        t = models.FirstTemporalWithActivity(column=1234,
                                             activity=create_activity)
        session.add(t)
        session.commit()

        edit_activity = models.Activity(description='Edit temp')
        session.add(edit_activity)

        with t.clock_tick(edit_activity):
            t.column = 4567

        session.commit()
        # sanity check Activity
        activity_query = session.query(models.Activity)
        assert activity_query.count() == 2
        # deeper check into each expected Activity
        create_activity_result = activity_query.order_by(
            models.Activity.date_created).first()
        assert create_activity_result.description == 'Create temp'

        activity_clock1_backref = temporal.get_activity_clock_backref(
            models.Activity, models.FirstTemporalWithActivity)
        assert getattr(
            create_activity_result, activity_clock1_backref.key, False)

        edit_activity_result = activity_query.order_by(
            models.Activity.date_created.desc()).first()
        assert edit_activity_result.description == 'Edit temp'
        assert getattr(
            edit_activity_result, activity_clock1_backref.key, False)
        # verify clocks on clocked entity now
        t = session.query(models.FirstTemporalWithActivity).first()
        assert t.vclock == 2
        assert t.clock.count() == 2

        clock_query = (
            session.query(models.FirstTemporalWithActivity
                          .temporal_options.clock_table)
            .order_by(models.FirstTemporalWithActivity
                      .temporal_options.clock_table.tick).all())
        assert len(clock_query) == 2

        assert clock_query[0].activity_id == create_activity_result.id
        assert clock_query[1].activity_id == edit_activity_result.id

    def test_activity_on_entity_edit_duplicate_activity(self, session):
        create_activity = models.Activity(description='Create temp')
        session.add(create_activity)

        t = models.FirstTemporalWithActivity(column=1234,
                                             activity=create_activity)
        session.add(t)
        session.commit()

        with pytest.raises(exc.IntegrityError):
            with t.clock_tick(create_activity):
                t.column = 4567
            session.commit()
