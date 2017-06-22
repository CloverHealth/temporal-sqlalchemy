import datetime as dt
import itertools
import uuid
import typing

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sap
import sqlalchemy.event as event
import sqlalchemy.ext.declarative as declarative
import sqlalchemy.orm as orm
import sqlalchemy.orm.attributes as attributes
import sqlalchemy.util as sa_util
import psycopg2.extras as psql_extras

from temporal_sqlalchemy import nine, util
from temporal_sqlalchemy.bases import (
    T_PROPS,
    Clocked,
    ClockedOption,
    TemporalActivityMixin,
    EntityClock,
    TemporalProperty)


def effective_now() -> psql_extras.DateTimeTZRange:
    utc_now = dt.datetime.now(tz=dt.timezone.utc)
    return psql_extras.DateTimeTZRange(utc_now, None)


def get_activity_clock_backref(
        activity: TemporalActivityMixin,
        entity: Clocked) -> orm.RelationshipProperty:
    """Get the backref'd clock history for a given entity."""
    assert (
        activity is entity.temporal_options.activity_cls or
        isinstance(activity, entity.temporal_options.activity_cls)
    ), "cannot inspect %r for mapped activity %r" % (entity, activity)

    inspected = sa.inspect(entity.temporal_options.activity_cls)
    backref = entity.temporal_options.clock_table.activity.property.backref

    return inspected.relationships[backref]


def get_history_model(
        target: attributes.InstrumentedAttribute) -> TemporalProperty:
    """Get the history model for given entity class."""
    assert hasattr(target.class_, 'temporal_options')

    return target.class_.temporal_options.history_tables[target.property]


# TODO kwargs to override default clock table and history tables prefix
def add_clock(*props: typing.Iterable[str],  # noqa: C901
              activity_cls: nine.Type[TemporalActivityMixin] = None,
              temporal_schema: typing.Optional[str] = None):
    """Decorator to add clock and history to an orm model."""

    def init_clock(clocked: Clocked, args, kwargs):
        """Add the first clock tick when initializing.

        Note: Special case for non-server side defaults"""
        # Note this block is because sqlalchemy doesn't materialize default
        # values on instances until after a flush but we need defaults & nulls
        # before flush so we can have a consistent history
        # TODO this whole thing seems horribly complex
        to_materialize = {
            prop for prop
            in clocked.temporal_options.history_tables.keys()
            if prop.key not in kwargs
            and getattr(prop.class_attribute, 'default', None) is not None}
        for prop in to_materialize:
            if callable(prop.class_attribute.default.arg):
                value = prop.class_attribute.default.arg(clocked)
            else:
                value = prop.class_attribute.default.arg
            setattr(clocked, prop.key, value)

        clocked.vclock = 1
        initial_clock_tick = clocked.temporal_options.clock_table(
            tick=clocked.vclock)
        if activity_cls is not None:
            try:
                initial_clock_tick.activity = kwargs.pop('activity')
            except KeyError as e:
                raise ValueError(
                    "activity is missing on create (%s)" % e) from None

        clocked.clock = [initial_clock_tick]

    def make_temporal(cls: nine.Type[Clocked]):
        assert issubclass(cls, Clocked), "add temporal.Clocked to %r" % cls
        mapper = cls.__mapper__

        local_props = {mapper.get_property(prop) for prop in props}
        for prop in local_props:
            assert all(col.onupdate is None for col in prop.columns), \
                '%r has onupdate' % prop
            assert all(col.server_default is None for col in prop.columns), \
                '%r has server_default' % prop
            assert all(col.server_onupdate is None for col in prop.columns), \
                '%r has server_onupdate' % prop

        relationship_props = set()
        for prop in mapper.relationships:
            # TODO: there has got to be a better way
            if 'temporal_on' in prop.info:
                assert hasattr(cls, prop.info['temporal_on']), \
                    '%r is missing a property %s' % (
                        cls, prop.info['temporal_on'])
                assert isinstance(
                    mapper.get_property(prop.info['temporal_on']),
                    orm.ColumnProperty), \
                    '%r has %s but it is not a Column' % (
                        cls, prop.info['temporal_on'])
                relationship_props.add(prop)

        # make sure all temporal properties have active_history (always loaded)
        for prop in local_props | relationship_props:
            getattr(cls, prop.key).impl.active_history = True

        entity_table = mapper.local_table
        entity_table_name = entity_table.name
        schema = temporal_schema or entity_table.schema
        clock_table_name = truncate_identifier("%s_clock" % entity_table_name)

        history_tables = {
            p: build_history_class(cls, p, schema)
            for p in local_props | relationship_props
        }

        clock_properties = dict(
            __tablename__=clock_table_name,

            entity_id=sa.Column(sa.ForeignKey(cls.id), nullable=False),
            entity=orm.relationship(
                cls, backref=orm.backref("clock", lazy='dynamic')),
        )

        tick_entity_constraint_name = truncate_identifier(
            '%s_tick_entity_id_key' % clock_table_name
        )
        table_args = [
            sa.UniqueConstraint('tick', 'entity_id',
                                name=tick_entity_constraint_name)
        ]

        if activity_cls is not None:
            backref_name = '%s_clock' % entity_table_name
            clock_properties['activity_id'] = sa.Column(
                sa.ForeignKey(activity_cls.id), nullable=False)
            clock_properties['activity'] = orm.relationship(
                activity_cls, backref=backref_name)
            table_args.append(sa.UniqueConstraint('entity_id', 'activity_id'))

        table_args.append({'schema': schema})
        clock_properties['__table_args__'] = tuple(table_args)

        clock_table = build_clock_class(
            cls.__name__, cls.metadata, clock_properties)
        # Add relationships for the latest and first clock ticks. These are
        # often accessed in list views and should be eagerly joined on when
        # doing so like this: `query.options(orm.joinedload('latest_tick'))`
        latest_tick = orm.relationship(
            clock_table,
            primaryjoin=sa.and_(cls.id == clock_table.entity_id,
                                cls.vclock == clock_table.tick),
            innerjoin=True,
            uselist=False,  # We are looking up a single child record
        )
        mapper.add_property('latest_tick', latest_tick)

        first_tick = orm.relationship(
            clock_table,
            primaryjoin=sa.and_(cls.id == clock_table.entity_id,
                                clock_table.tick == 1),
            innerjoin=True,
            uselist=False,  # We are looking up a single child record
        )
        mapper.add_property('first_tick', first_tick)

        temporal_options = ClockedOption(
            temporal_props=local_props | relationship_props,
            history_models=history_tables,
            clock_model=clock_table,
            activity_cls=activity_cls,
        )
        cls.temporal_options = temporal_options
        event.listen(cls, 'init', init_clock)

        return cls

    return make_temporal


def _copy_column(column: sa.Column) -> sa.Column:
    """copy a column, set some properties on it for history table creation"""
    original = column
    new = column.copy()
    original.info['history_copy'] = new
    for fk in column.foreign_keys:
        new.append_foreign_key(sa.ForeignKey(fk.target_fullname))
    new.unique = False
    new.default = new.server_default = None

    return new


def truncate_identifier(identifier: str) -> str:
    """ensure identifier doesn't exceed max characters postgres allows"""
    max_len = (sap.dialect.max_index_name_length
               or sap.dialect.max_identifier_length)
    if len(identifier) > max_len:
        return "%s_%s" % (identifier[0:max_len - 8],
                          sa_util.md5_hex(identifier)[-4:])
    return identifier


def build_clock_class(
        name: str,
        metadata: sa.MetaData,
        props: typing.Dict) -> nine.Type[EntityClock]:
    base_classes = (
        EntityClock,
        declarative.declarative_base(metadata=metadata),
    )
    return type('%sClock' % name, base_classes, props)


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
def _(type_, name):
    return sa.cast(sa.text(name), sap.TEXT), '='


def build_history_table(
        cls: declarative.DeclarativeMeta,
        prop: T_PROPS,
        schema: str = None) -> sa.Table:
    """build a sql alchemy table for given prop"""
    if isinstance(prop, orm.RelationshipProperty):
        columns = [_copy_column(column) for column in prop.local_columns]
    else:
        columns = [_copy_column(column) for column in prop.columns]

    local_table = cls.__table__
    table_name = truncate_identifier(
        _generate_history_table_name(local_table, columns)
    )
    entity_foreign_keys = list(util.foreign_key_to(local_table))
    entity_constraints = [
        _exclusion_in(fk.type, fk.key)
        for fk in entity_foreign_keys
    ]

    constraints = [
        sa.Index(
            truncate_identifier('%s_effective_idx' % table_name),
            'effective',
            postgresql_using='gist'
        ),
        sap.ExcludeConstraint(
            *itertools.chain(entity_constraints, [('vclock', '&&')]),
            name=truncate_identifier('%s_excl_vclock' % table_name)
        ),
        sap.ExcludeConstraint(
            *itertools.chain(entity_constraints, [('effective', '&&')]),
            name=truncate_identifier('%s_excl_effective' % table_name)
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
                  default=effective_now,
                  nullable=False),
        sa.Column('vclock', sap.INT4RANGE, nullable=False),
        *itertools.chain(entity_foreign_keys, columns, constraints),
        schema=schema or local_table.schema,
        keep_existing=True
    )  # memoization ftw
