import itertools
import typing
import uuid
import warnings

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sap
import sqlalchemy.event as event
import sqlalchemy.ext.declarative as declarative
import sqlalchemy.orm as orm

from temporal_sqlalchemy import nine, util
from temporal_sqlalchemy.bases import (
    T_PROPS,
    Clocked,
    TemporalOption,
    TemporalActivityMixin,
    EntityClock,
    TemporalProperty)


def temporal_map(*track, mapper: orm.Mapper, activity_class=None,
                 schema=None, allow_persist_on_commit=False):
    assert 'vclock' not in track

    cls = mapper.class_
    entity_table = mapper.local_table
    # get things defined on Temporal:
    tracked_props = frozenset(
        mapper.get_property(prop) for prop in track
    )
    # make sure all temporal properties have active_history (always loaded)
    for prop in tracked_props:
        getattr(cls, prop.key).impl.active_history = True

    schema = schema or entity_table.schema

    clock_table = build_clock_table(
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

    clock_model = build_clock_class(cls.__name__,
                                    entity_table.metadata,
                                    clock_properties)

    history_models = {
        p: build_history_class(cls, p, schema)
        for p in tracked_props
    }

    cls.temporal_options = TemporalOption(
        temporal_props=tracked_props,
        history_models=history_models,
        clock_model=clock_model,
        activity_cls=activity_class,
        allow_persist_on_commit=allow_persist_on_commit,
    )

    event.listen(cls, 'init', init_clock)


def init_clock(obj: Clocked, args, kwargs):
    kwargs.setdefault('vclock', 1)
    initial_tick = obj.temporal_options.clock_model(
        tick=kwargs['vclock'],
        entity=obj,
    )

    if obj.temporal_options.activity_cls and 'activity' not in kwargs:
        raise ValueError(
            "%r missing keyword argument: activity" % obj.__class__)

    if 'activity' in kwargs:
        initial_tick.activity = kwargs.pop('activity')

    materialize_defaults(obj, kwargs)


def materialize_defaults(obj: Clocked, kwargs):
    """Add the first clock tick when initializing.
            Note: Special case for non-server side defaults"""
    # Note this block is because sqlalchemy doesn't materialize default
    # values on instances until after a flush but we need defaults & nulls
    # before flush so we can have a consistent history
    warnings.warn("this method is unnecessary with recent sqlalchemy changes",
                  PendingDeprecationWarning)
    to_materialize = {
        prop for prop in obj.temporal_options.history_models.keys()
        if prop.key not in kwargs
        and getattr(prop.class_attribute, 'default', None) is not None
    }
    for prop in to_materialize:
        if callable(prop.class_attribute.default.arg):
            value = prop.class_attribute.default.arg(obj)
        else:
            value = prop.class_attribute.default.arg
        setattr(obj, prop.key, value)


def defaults_safety(*track, mapper):
    warnings.warn("these caveats are temporary", PendingDeprecationWarning)
    local_props = {mapper.get_property(prop) for prop in track}
    for prop in local_props:
        if isinstance(prop, orm.RelationshipProperty):
            continue
        assert all(col.onupdate is None for col in prop.columns), \
            '%r has onupdate' % prop
        assert all(col.server_default is None for col in prop.columns), \
            '%r has server_default' % prop
        assert all(col.server_onupdate is None for col in prop.columns), \
            '%r has server_onupdate' % prop


# TODO kwargs to override default clock table and history tables prefix
def add_clock(*props: typing.Iterable[str],  # noqa: C901
              activity_cls: nine.Type[TemporalActivityMixin] = None,
              temporal_schema: typing.Optional[str] = None,
              allow_persist_on_commit: bool = False):
    """Decorator to add clock and history to an orm model."""

    def make_temporal(cls: nine.Type[Clocked]):
        assert issubclass(cls, Clocked), "add temporal.Clocked to %r" % cls
        mapper = cls.__mapper__
        defaults_safety(*props, mapper=mapper)
        temporal_map(*props,
                     mapper=mapper,
                     activity_class=activity_cls,
                     schema=temporal_schema,
                     allow_persist_on_commit=allow_persist_on_commit)
        return cls

    return make_temporal


def build_clock_class(
        name: str,
        metadata: sa.MetaData,
        props: typing.Dict) -> nine.Type[EntityClock]:
    base_classes = (
        EntityClock,
        declarative.declarative_base(metadata=metadata),
    )
    return type('%sClock' % name, base_classes, props)


def build_clock_table(entity_table: sa.Table,
                      metadata: sa.MetaData,
                      schema: str,
                      activity_class=None) -> sa.Table:
    clock_table_name = util.truncate_identifier(
        "%s_clock" % entity_table.name)
    clock_table = sa.Table(
        clock_table_name,
        metadata,
        sa.Column('id',
                  sap.UUID(as_uuid=True),
                  default=uuid.uuid4,
                  primary_key=True),
        sa.Column('tick',
                  sa.Integer,
                  nullable=False,
                  autoincrement=False),
        sa.Column('timestamp',
                  sa.DateTime(True),
                  server_default=sa.func.current_timestamp()),
        schema=schema)

    entity_keys = list()
    for fk in util.foreign_key_to(entity_table, nullable=False):
        # this is done to support arbitrary primary key shape on entity
        # We don't add additional indices on the foreign keys here because
        # the uniqueness constraints will add an implicit index.
        clock_table.append_column(fk)
        entity_keys.append(fk.key)

    tick_entity_unique_name = util.truncate_identifier(
        '%s_tick_entity_id_key' % clock_table_name
    )
    clock_table.append_constraint(
        sa.UniqueConstraint(*(entity_keys + ['tick']),
                            name=tick_entity_unique_name)
    )

    if activity_class:
        activity_keys = list()
        # support arbitrary shaped activity primary keys
        for fk in util.foreign_key_to(activity_class.__table__,
                                      prefix='activity',
                                      nullable=False):
            clock_table.append_column(fk)
            activity_keys.append(fk.key)
        # ensure we have DB constraint on clock <> activity uniqueness
        clock_table.append_constraint(
            sa.UniqueConstraint(*(entity_keys + activity_keys))
        )

    return clock_table


def build_history_class(
        cls: declarative.DeclarativeMeta,
        prop: T_PROPS,
        schema: str = None) -> nine.Type[TemporalProperty]:
    """build a sqlalchemy model for given prop"""
    class_name = "%s%s_%s" % (cls.__name__, 'History', prop.key)
    table = build_history_table(cls, prop, schema)
    base_classes = (
        TemporalProperty,
        declarative.declarative_base(metadata=table.metadata),
    )
    class_attrs = {
        '__table__': table,
        'entity': orm.relationship(
            lambda: cls,
            backref=orm.backref('%s_history' % prop.key, lazy='dynamic')
        ),
    }

    if isinstance(prop, orm.RelationshipProperty):
        class_attrs[prop.key] = orm.relationship(
            prop.argument,
            lazy='noload')

    model = type(class_name, base_classes, class_attrs)
    return model


def _generate_history_table_name(local_table: sa.Table,
                                 cols: typing.Iterable[sa.Column]) -> str:
    base_name = '%s_history' % local_table.name
    sort_col_names = sorted(col.key for col in cols)

    return "%s_%s" % (base_name, "_".join(sort_col_names))


@nine.singledispatch
def _exclusion_in(type_, name) -> typing.Tuple:
    return name, '='


@_exclusion_in.register(sap.UUID)
def _exclusion_in_uuid(type_, name):
    """
    Cast UUIDs to text for our exclusion index because postgres doesn't
    currently allow GiST indices on UUIDs.
    """
    return sa.cast(sa.text(name), sap.TEXT), '='


def build_history_table(
        cls: declarative.DeclarativeMeta,
        prop: T_PROPS,
        schema: str = None) -> sa.Table:
    """build a sql alchemy table for given prop"""
    if isinstance(prop, orm.RelationshipProperty):
        columns = [util.copy_column(column) for column in prop.local_columns]
    else:
        columns = [util.copy_column(column) for column in prop.columns]

    local_table = cls.__table__
    table_name = util.truncate_identifier(
        _generate_history_table_name(local_table, columns)
    )
    # Build the foreign key(s), specifically adding an index since we may use
    # a casted foreign key in our constraints. See _exclusion_in_uuid
    entity_foreign_keys = list(util.foreign_key_to(local_table, index=True))
    entity_constraints = [
        _exclusion_in(fk.type, fk.key)
        for fk in entity_foreign_keys
    ]

    constraints = [
        sa.Index(
            util.truncate_identifier('%s_effective_idx' % table_name),
            'effective',
            postgresql_using='gist'
        ),
        sap.ExcludeConstraint(
            *itertools.chain(entity_constraints, [('vclock', '&&')]),
            name=util.truncate_identifier('%s_excl_vclock' % table_name)
        ),
        sap.ExcludeConstraint(
            *itertools.chain(entity_constraints, [('effective', '&&')]),
            name=util.truncate_identifier('%s_excl_effective' % table_name)
        ),
    ]

    return sa.Table(
        table_name,
        local_table.metadata,
        sa.Column('id',
                  sap.UUID(as_uuid=True),
                  default=uuid.uuid4,
                  primary_key=True),
        sa.Column('effective',
                  sap.TSTZRANGE,
                  default=util.effective_now,
                  nullable=False),
        sa.Column('vclock', sap.INT4RANGE, nullable=False),
        *itertools.chain(entity_foreign_keys, columns, constraints),
        schema=schema or local_table.schema,
        keep_existing=True
    )  # memoization ftw
