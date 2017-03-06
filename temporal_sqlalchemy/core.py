import sqlalchemy as sa
import sqlalchemy.ext.declarative as declarative
import sqlalchemy.orm as orm

from temporal_sqlalchemy import bases, clock, util


class TemporalModel(object):
    temporal_options = None  # type: bases.ClockedOption

    @staticmethod
    def build_clock_table(entity_table, metadata, schema, activity_class=None) -> sa.Table:
        clock_table_name = clock.truncate_identifier("%s_clock" % entity_table.name)
        clock_table = sa.Table(
            clock_table_name,
            metadata,
            sa.Column('tick', sa.Integer, primary_key=True, autoincrement=False),
            sa.Column('timestamp', sa.DateTime(True), server_default=sa.func.current_timestamp()),
            schema=schema)

        entity_keys = set()
        for fk in util.foreign_key_to(entity_table, primary_key=True):
            # this is done to support arbitrary primary key shape on entity
            clock_table.append_column(fk)
            entity_keys.add(fk.key)

        if activity_class:
            activity_keys = set()
            # support arbitrary shaped activity primary keys
            for fk in util.foreign_key_to(activity_class.__table__, prefix='activity', nullable=False):
                clock_table.append_column(fk)
                activity_keys.add(fk.key)
            # ensure we have DB constraint on clock <> activity uniqueness
            clock_table.append_constraint(
                sa.UniqueConstraint(*(entity_keys | activity_keys))
            )

        return clock_table

    @staticmethod
    def temporal_map(mapper: orm.Mapper):
        cls = mapper.class_
        assert hasattr(cls, 'Temporal')
        entity_table = mapper.local_table
        temporal_declaration = cls.Temporal
        # get things defined on Temporal:
        tracked_props = frozenset(mapper.get_property(prop) for prop in cls.Temporal.track)
        activity_class = getattr(temporal_declaration, 'activity_class', None)
        schema = getattr(temporal_declaration, 'schema', entity_table.schema)

        clock_table = TemporalModel.build_clock_table(entity_table, cls.metadata, schema, activity_class)
        clock_properties = {
            'entity': orm.relationship(lambda: cls, backref=orm.backref('clock', lazy='dynamic')),
            '__table__': clock_table
        }  # used to construct a new clock model for this entity

        if activity_class:
            # create a relationship to the activity from the clock model
            backref_name = '%s_clock' % entity_table.name
            clock_properties['activity'] = orm.relationship(lambda: activity_class, backref=backref_name)

        clock_model = clock.build_clock_class(cls.__name__, cls.metadata, clock_properties)

        cls.temporal_options = bases.ClockedOption(
            temporal_props=tracked_props,
            history_models=history_models,
            clock_model=clock_model,
            activity_cls=activity_class
        )

    @declarative.declared_attr
    def __mapper_cls__(cls):
        assert hasattr(cls, 'Temporal')

        def map_(cls, *args, **kwargs):
            mp = orm.mapper(cls, *args, **kwargs)
            cls.temporal_map(mp)
            return mp

        return map_
