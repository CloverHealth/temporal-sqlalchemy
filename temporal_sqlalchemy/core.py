import sqlalchemy as sa
import sqlalchemy.ext.declarative as declarative
import sqlalchemy.orm as orm
import sqlalchemy.event as event

from temporal_sqlalchemy import bases, clock, util


class TemporalModel(bases.Clocked):
    @staticmethod
    def build_clock_table(entity_table: sa.Table,
                          metadata: sa.MetaData,
                          schema: str,
                          activity_class=None) -> sa.Table:
        clock_table_name = clock.truncate_identifier(
            "%s_clock" % entity_table.name)
        clock_table = sa.Table(
            clock_table_name,
            metadata,
            sa.Column('tick',
                      sa.Integer,
                      primary_key=True,
                      autoincrement=False),
            sa.Column('timestamp',
                      sa.DateTime(True),
                      server_default=sa.func.current_timestamp()),
            schema=schema)

        entity_keys = set()
        for fk in util.foreign_key_to(entity_table, primary_key=True):
            # this is done to support arbitrary primary key shape on entity
            clock_table.append_column(fk)
            entity_keys.add(fk.key)

        if activity_class:
            activity_keys = set()
            # support arbitrary shaped activity primary keys
            for fk in util.foreign_key_to(activity_class.__table__,
                                          prefix='activity',
                                          nullable=False):
                clock_table.append_column(fk)
                activity_keys.add(fk.key)
            # ensure we have DB constraint on clock <> activity uniqueness
            clock_table.append_constraint(
                sa.UniqueConstraint(*(entity_keys | activity_keys))
            )

        return clock_table

    @staticmethod
    def temporal_map(mapper: orm.Mapper, cls: bases.Clocked):
        temporal_declaration = cls.Temporal
        assert 'vclock' not in temporal_declaration.track
        entity_table = mapper.local_table
        # get things defined on Temporal:
        tracked_props = frozenset(
            mapper.get_property(prop) for prop in temporal_declaration.track
        )
        # make sure all temporal properties have active_history (always loaded)
        for prop in tracked_props:
            getattr(cls, prop.key).impl.active_history = True

        activity_class = getattr(temporal_declaration, 'activity_class', None)
        schema = getattr(temporal_declaration, 'schema', entity_table.schema)

        clock_table = TemporalModel.build_clock_table(
            entity_table,
            entity_table.metadata,
            schema,
            activity_class
        )
        clock_properties = {
            'entity': orm.relationship(
                lambda: cls, backref=orm.backref('clock', lazy='dynamic')
            ),
            'entity_first_tick': orm.relationship(
                lambda: cls,
                backref=orm.backref(
                    'first_tick',
                    primaryjoin=sa.and_(
                        clock_table.join(entity_table).onclause,
                        clock_table.c.tick == 1
                    ),
                    innerjoin=True,
                    uselist=False,  # single record
                    viewonly=True  # view only
                )
            ),
            'entity_latest_tick': orm.relationship(
                lambda: cls,
                backref=orm.backref(
                    'latest_tick',
                    primaryjoin=sa.and_(
                        clock_table.join(entity_table).onclause,
                        entity_table.c.vclock == clock_table.c.tick
                    ),
                    innerjoin=True,
                    uselist=False,  # single record
                    viewonly=True  # view only
                )
            ),
            '__table__': clock_table
        }  # used to construct a new clock model for this entity

        if activity_class:
            # create a relationship to the activity from the clock model
            backref_name = '%s_clock' % entity_table.name
            clock_properties['activity'] = \
                orm.relationship(lambda: activity_class, backref=backref_name)

        clock_model = clock.build_clock_class(cls.__name__,
                                              entity_table.metadata,
                                              clock_properties)

        history_models = {
            p: clock.build_history_class(cls, p, schema)
            for p in tracked_props
        }

        cls.temporal_options = bases.ClockedOption(
            temporal_props=tracked_props,
            history_models=history_models,
            clock_model=clock_model,
            activity_cls=activity_class
        )

        event.listen(cls, 'init', TemporalModel.init_clock)

    @staticmethod
    def init_clock(clocked: 'TemporalModel', args, kwargs):
        kwargs.setdefault('vclock', 1)
        initial_tick = clocked.temporal_options.clock_model(
            tick=kwargs['vclock'],
            entity=clocked,
        )

        if 'activity' in kwargs:
            initial_tick.activity = kwargs.pop('activity')

    @declarative.declared_attr
    def __mapper_cls__(cls):
        assert hasattr(cls, 'Temporal')

        def mapper(cls, *args, **kwargs):
            mp = orm.mapper(cls, *args, **kwargs)
            cls.temporal_map(mp, cls)
            return mp

        return mapper
