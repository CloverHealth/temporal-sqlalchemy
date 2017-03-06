import abc
import collections
import contextlib
import datetime as dt
import typing
import uuid  # noqa: F401
import warnings

import sqlalchemy as sa
import sqlalchemy.orm as orm
import sqlalchemy.orm.attributes as attributes
import psycopg2.extras as psql_extras

from temporal_sqlalchemy import nine

_ClockSet = collections.namedtuple('_ClockSet', ('effective', 'vclock'))

T_PROPS = typing.TypeVar(
    'T_PROP', orm.RelationshipProperty, orm.ColumnProperty)


class EntityClock(object):
    entity_id = None  # type: typing.Union[int, uuid.UUID]

    tick = sa.Column(sa.Integer, primary_key=True, autoincrement=False)
    timestamp = sa.Column(sa.DateTime(True),
                          server_default=sa.func.current_timestamp())


class TemporalProperty(object):
    """mixin when constructing a property history table"""
    __table__ = None  # type: sa.Table
    entity_id = None  # type: orm.ColumnProperty
    entity = None  # type: orm.RelationshipProperty
    effective = None  # type: psql_extras.DateTimeRange
    vclock = None  # type: psql_extras.NumericRange


class TemporalActivityMixin(object):
    @abc.abstractproperty
    def id(self):
        pass


class ClockedOption(object):
    def __init__(
            self,
            history_models: typing.Dict[T_PROPS, nine.Type[TemporalProperty]],
            temporal_props: typing.Iterable[T_PROPS],
            clock_model: nine.Type[EntityClock],
            activity_cls: nine.Type[TemporalActivityMixin] = None):
        self.history_models = history_models
        self.temporal_props = temporal_props

        self.clock_model = clock_model
        self.activity_cls = activity_cls

    @property
    def clock_table(self):
        warnings.warn('use ClockedOption.clock_model instead', PendingDeprecationWarning)
        return self.clock_model

    @property
    def history_tables(self):
        warnings.warn('use ClockedOption.history_models instead', PendingDeprecationWarning)
        return self.history_models

    @staticmethod
    def make_clock(effective_lower: dt.datetime,
                   vclock_lower: int,
                   **kwargs) -> _ClockSet:
        """construct a clock set tuple"""
        effective_upper = kwargs.get('effective_upper', None)
        vclock_upper = kwargs.get('vclock_upper', None)

        effective = psql_extras.DateTimeTZRange(
            effective_lower, effective_upper)
        vclock = psql_extras.NumericRange(vclock_lower, vclock_upper)

        return _ClockSet(effective, vclock)

    def record_history(self,
                       clocked: 'Clocked',
                       session: orm.Session,
                       timestamp: dt.datetime):
        """record all history for a given clocked object"""
        state = attributes.instance_state(clocked)
        try:
            new_tick = state.dict['vclock']
        except KeyError:
            # TODO understand why this is necessary
            new_tick = getattr(clocked, 'vclock')

        new_clock = self.make_clock(timestamp, new_tick)
        attr = {'entity': clocked}

        for prop, cls in self.history_tables.items():
            hist = attr.copy()
            # fires a load on any deferred columns
            if prop.key not in state.dict:
                getattr(clocked, prop.key)

            if isinstance(prop, orm.RelationshipProperty):
                changes = attributes.get_history(
                    clocked, prop.key,
                    passive=attributes.PASSIVE_NO_INITIALIZE)
            else:
                changes = attributes.get_history(clocked, prop.key)

            if changes.added:
                # Cap previous history row if exists
                if sa.inspect(clocked).identity is not None:
                    # but only if it already exists!!
                    effective_close = sa.func.tstzrange(
                        sa.func.lower(cls.effective),
                        new_clock.effective.lower,
                        '[)')
                    vclock_close = sa.func.int4range(
                        sa.func.lower(cls.vclock),
                        new_clock.vclock.lower,
                        '[)')

                    history_query = getattr(
                        clocked, cls.entity.property.backref[0])
                    history_query.filter(
                        sa.and_(
                            sa.func.upper_inf(cls.effective),
                            sa.func.upper_inf(cls.vclock),
                        )
                    ).update(
                        {
                            cls.effective: effective_close,
                            cls.vclock: vclock_close,
                        }, synchronize_session=False
                    )

                # Add new history row
                hist[prop.key] = changes.added[0]
                session.add(
                    cls(vclock=new_clock.vclock, effective=new_clock.effective, **hist)
                )


class Clocked(object):
    """Clocked Mixin gives you the default implementations for working
    with clocked data

    use with add_clock to make your model temporal:

    >>> import sqlalchemy as sa
    >>> import sqlalchemy.ext.declarative as declarative
    >>> import temporal_sqlalchemy
    >>>
    >>> @temporal_sqlalchemy.add_clock('prop1', 'prop2')
    >>> class MyModel(Clocked, declarative.declarative_base()):
    >>>     prop1 = sa.Column(sa.INTEGER)
    >>>     prop2 = sa.Column(sa.TEXT)
    >>>
    >>> my_instance = MyModel(prop1=1, prop2='foo')
    >>> assert my_instance.temporal_options is MyModel.temporal_options
    >>> assert my_instance.vclock == 1
    """
    vclock = sa.Column(sa.Integer, default=1)

    clock = None  # type: orm.relationship
    temporal_options = None  # type: ClockedOption
    first_tick = None  # type:  EntityClock
    latest_tick = None  # type:  EntityClock

    @property
    def date_created(self):
        return self.first_tick.timestamp

    @property
    def date_modified(self):
        return self.latest_tick.timestamp

    @contextlib.contextmanager
    def clock_tick(self, activity: TemporalActivityMixin = None):
        """Increments vclock by 1 with changes scoped to the session"""
        if self.temporal_options.activity_cls is not None and activity is None:
            raise ValueError("activity is missing on edit") from None

        session = orm.object_session(self)
        with session.no_autoflush:
            yield self

        if session.is_modified(self):
            self.vclock += 1

            new_clock_tick = self.temporal_options.clock_table(
                entity=self, tick=self.vclock)
            if activity is not None:
                new_clock_tick.activity = activity

            session.add(new_clock_tick)
