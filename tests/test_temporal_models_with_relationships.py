import pytest
import sqlalchemy as sa

from . import shared, models


class TestTemporalRelationshipModels(shared.DatabaseTest):
    @pytest.fixture(autouse=True)
    def setup(self, session):
        models.basic_metadata.create_all(session.bind)

    def test_assign_by_rel_on_init(self, session):
        related = models.RelatedTable(prop_a=1)
        parent = models.RelationalTemporalModel(
            prop_a=1,
            prop_b='foo',
            rel=related
        )
        session.add(parent)
        session.commit()

        clock_query = session.query(
            models.RelationalTemporalModel.temporal_options.clock_table)
        clock_tick = clock_query.first()
        assert clock_query.count() == 1
        assert clock_tick.tick == parent.vclock
        for history_class in (models.RelationalTemporalModel
                              .temporal_options.history_tables.values()):
            property_history = session.query(history_class)
            assert property_history.count() == 1, \
                "missing history #1 for %r" % history_class

            history = property_history.first()
            assert clock_tick.tick in history.vclock

    def test_assign_by_rel_on_edit_init_with_rel(self, session):
        related = models.RelatedTable(prop_a=1)
        parent = models.RelationalTemporalModel(
            prop_a=1,
            prop_b='foo',
            rel=related
        )
        session.add(parent)
        session.commit()

        with parent.clock_tick():
            parent.rel = models.RelatedTable(prop_a=2)

        session.commit()
        assert parent.vclock == 2

        rel_prop = sa.inspect(models.RelationalTemporalModel.rel).property
        history_class = (models.RelationalTemporalModel
                         .temporal_options.history_tables[rel_prop])

        property_history = (
            session.query(history_class)
            .order_by(sa.func.lower(history_class.effective).asc()))
        assert property_history.count() == 2, \
            "missing history #2 for %r" % history_class

        first_history = property_history[0]
        assert first_history.vclock.upper_inf is False
        assert first_history.effective.upper_inf is False
        second_history = property_history[1]
        assert second_history.vclock.upper_inf
        assert second_history.effective.upper_inf

        assert first_history.vclock < second_history.vclock
        assert first_history.effective < second_history.effective

        assert not(first_history.vclock.lower in second_history.vclock)
        assert not(second_history.vclock.lower in first_history.vclock)

    def test_assign_by_rel_on_edit_init_without_rel(self, session):
        parent = models.RelationalTemporalModel(
            prop_a=1,
            prop_b='foo'
        )
        session.add(parent)
        session.commit()

        with parent.clock_tick():
            parent.rel = models.RelatedTable(prop_a=1)

        session.commit()
        assert parent.vclock == 2

        rel_prop = sa.inspect(models.RelationalTemporalModel.rel).property
        history_class = (models.RelationalTemporalModel
                         .temporal_options.history_tables[rel_prop])

        property_history = (
            session.query(history_class)
            .order_by(sa.func.lower(history_class.effective).asc()))
        assert property_history.count() == 1, \
            "missing history #1 for %r" % history_class

        first_history = property_history[0]
        assert first_history.vclock.upper_inf
        assert first_history.effective.upper_inf

    def test_assign_by_rel_on_edit_init_with_rel_none(self, session):
        """Test temporality when initializing parent with rel_id = None.

        This situation required special-case code in temporal.py. This is not
        the same as not specifying a rel_id at all when creating the parent;
        that case is handled by the above test.
        """
        parent = models.RelationalTemporalModel(
            prop_a=1,
            prop_b='foo',
            rel_id=None
        )
        session.add(parent)
        session.commit()

        with parent.clock_tick():
            parent.rel = models.RelatedTable(prop_a=1)

        session.commit()
        assert parent.vclock == 2

        rel_prop = sa.inspect(models.RelationalTemporalModel.rel).property
        history_class = (models.RelationalTemporalModel
                         .temporal_options.history_tables[rel_prop])

        property_history = (
            session.query(history_class)
            .order_by(sa.func.lower(history_class.effective).asc()))
        assert property_history.count() == 2, \
            "missing history #2 for %r" % history_class

        first_history = property_history[0]
        assert not first_history.vclock.upper_inf
        assert not first_history.effective.upper_inf
        second_history = property_history[1]
        assert second_history.vclock.upper_inf
        assert second_history.effective.upper_inf

        assert first_history.vclock < second_history.vclock
        assert first_history.effective < second_history.effective

        assert not(first_history.vclock.lower in second_history.vclock)
        assert not(second_history.vclock.lower in first_history.vclock)
