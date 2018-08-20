import temporal_sqlalchemy as temporal

from . import shared, models


class TestTemporalMultiEntityWithActivity(shared.DatabaseTest):
    def test_activity_on_multi_entity_create(self, session):
        activity = models.Activity(description='Create temps')
        session.add(activity)

        t1 = models.FirstTemporalWithActivity(column=1234, activity=activity)
        t2 = models.SecondTemporalWithActivity(column=4567, activity=activity)
        session.add(t1)
        session.add(t2)
        session.commit()

        activity_query = session.query(models.Activity)
        assert activity_query.count() == 1
        activity_result = activity_query.first()
        assert activity_result.description == 'Create temps'

        activity_clock1_backref = temporal.get_activity_clock_backref(
            models.Activity, models.FirstTemporalWithActivity)
        activity_clock2_backref = temporal.get_activity_clock_backref(
            models.Activity, models.SecondTemporalWithActivity)
        assert getattr(activity_result, activity_clock2_backref.key)
        assert getattr(activity_result, activity_clock1_backref.key)

        t1 = session.query(models.FirstTemporalWithActivity).first()
        assert t1.vclock == 1
        assert t1.clock.count() == 1

        t2 = session.query(models.FirstTemporalWithActivity).first()
        assert t2.vclock == 1
        assert t2.clock.count() == 1

        clock1_query = session.query(
            models.FirstTemporalWithActivity.temporal_options.clock_table)
        assert clock1_query.count() == 1
        clock2_query = session.query(
            models.SecondTemporalWithActivity.temporal_options.clock_table)
        assert clock2_query.count() == 1

        clock1_result = clock1_query.first()
        assert clock1_result.activity_id == activity_result.id

        clock2_result = clock2_query.first()
        assert clock2_result.activity_id == activity_result.id

    def test_activity_on_multi_entity_edit(self, session):
        create_activity = models.Activity(description='Create temp')
        session.add(create_activity)

        t1 = models.FirstTemporalWithActivity(
            column=1234, activity=create_activity)
        t2 = models.SecondTemporalWithActivity(
            column=4567, activity=create_activity)
        session.add(t1)
        session.add(t2)
        session.commit()

        edit_activity = models.Activity(description='Edit temp')
        session.add(edit_activity)

        with t1.clock_tick(edit_activity):
            t1.column = 123456

        with t2.clock_tick(edit_activity):
            t2.column = 456789

        session.commit()

        activity_query = session.query(models.Activity)
        assert activity_query.count() == 2

        create_activity_result = activity_query\
            .order_by(models.Activity.date_created)\
            .first()
        assert create_activity_result.description == 'Create temp'

        activity_clock2_backref = temporal.get_activity_clock_backref(
            models.Activity, models.SecondTemporalWithActivity)
        activity_clock1_backref = temporal.get_activity_clock_backref(
            models.Activity, models.FirstTemporalWithActivity)

        assert getattr(create_activity_result, activity_clock2_backref.key)
        assert getattr(create_activity_result, activity_clock1_backref.key)

        edit_activity_result = activity_query\
            .order_by(models.Activity.date_created.desc())\
            .first()
        assert edit_activity_result.description == 'Edit temp'

        assert getattr(edit_activity, activity_clock2_backref.key)
        assert getattr(edit_activity_result, activity_clock1_backref.key)

        t1 = session.query(models.FirstTemporalWithActivity).first()
        assert t1.vclock == 2
        assert t1.clock.count() == 2

        t2 = session.query(models.FirstTemporalWithActivity).first()
        assert t2.vclock == 2
        assert t2.clock.count() == 2

        clock_model = models.FirstTemporalWithActivity.temporal_options\
            .clock_table
        clock1_query = session.query(clock_model)\
            .order_by(clock_model.tick).all()
        assert len(clock1_query) == 2

        assert clock1_query[0].activity_id == create_activity_result.id
        assert clock1_query[1].activity_id == edit_activity_result.id

        clock_model = models.SecondTemporalWithActivity.temporal_options\
            .clock_table
        clock2_query = session.query(clock_model)\
            .order_by(clock_model.tick).all()
        assert len(clock2_query) == 2

        assert clock2_query[0].activity_id == create_activity_result.id
        assert clock2_query[1].activity_id == edit_activity_result.id
