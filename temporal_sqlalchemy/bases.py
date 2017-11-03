import abc
import collections
import contextlib
import datetime as dt
import typing
import uuid
import warnings

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sap
import sqlalchemy.orm as orm
import sqlalchemy.orm.base as base
import sqlalchemy.orm.attributes as attributes
import psycopg2.extras as psql_extras

from temporal_sqlalchemy import nine

_PersistentClockPair = collections.namedtuple('_PersistentClockPairs',
                                              ('effective', 'vclock'))

T_PROPS = typing.TypeVar(
    'T_PROP', orm.RelationshipProperty, orm.ColumnProperty)


class ActivityState:
    def __set__(self, instance, value):
        if not instance.temporal_options.activity_cls:
            raise ValueError(
                "Can't set activity state on instance of %r "
                "because the activity class is None."
                % type(instance).__name__)

        # TODO should not be able to change activity once changes have
        # TODO been made to temporal properties
        setattr(instance, '__temporal_current_activity', value)

        if value:
            current_clock = instance.current_clock
            current_clock.activity = value

    def __get__(self, instance, owner):
        if not instance:
            return self

        return getattr(instance, '__temporal_current_activity', None)

    @staticmethod
    def reset_activity(target, attr):
        target.activity = None

    @staticmethod
    def activity_required(target, value, oldvalue, initiator):
        if not target.activity and oldvalue is not base.NEVER_SET:
            raise ValueError("activity required")


class ClockState:
    def __set__(self, instance, value: 'EntityClock'):
        setattr(instance, '__temporal_current_tick', value)
        if value:
            instance.clock.append(value)

    def __get__(self, instance, owner):
        if not instance:
            return self

        vclock = getattr(instance, 'vclock', 0) or 0  # start at 0 if None
        if not getattr(instance, '__temporal_current_tick', None):
            new_version = vclock + 1
            instance.vclock = new_version
            clock_tick = instance.temporal_options.clock_model(tick=new_version)
            setattr(instance, '__temporal_current_tick', clock_tick)
            instance.clock.append(clock_tick)

        return getattr(instance, '__temporal_current_tick')

    @staticmethod
    def reset_tick(target, attr):
        if target:
            setattr(target, '__temporal_current_tick', None)


class EntityClock(object):
    id = sa.Column(sap.UUID(as_uuid=True), default=uuid.uuid4, primary_key=True)
    tick = sa.Column(sa.Integer, nullable=False)
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
    @abc.abstractmethod
    def id(self):
        pass


class TemporalOption(object):
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
        self.model = None

    @property
    def clock_table(self):
        warnings.warn(
            'use TemporalOption.clock_model instead',
            PendingDeprecationWarning)
        return self.clock_model

    @property
    def history_tables(self):
        warnings.warn(
            'use TemporalOption.history_models instead',
            PendingDeprecationWarning)
        return self.history_models

    @staticmethod
    def make_clock(effective_lower: dt.datetime,
                   vclock_lower: int,
                   **kwargs) -> _PersistentClockPair:
        """construct a clock set tuple"""
        effective_upper = kwargs.get('effective_upper', None)
        vclock_upper = kwargs.get('vclock_upper', None)

        effective = psql_extras.DateTimeTZRange(
            effective_lower, effective_upper)
        vclock = psql_extras.NumericRange(vclock_lower, vclock_upper)

        return _PersistentClockPair(effective, vclock)

    def record_history(self,
                       clocked: 'Clocked',
                       session: orm.Session,
                       timestamp: dt.datetime):
        """record all history for a given clocked object"""
        state = attributes.instance_state(clocked)
        attr = {'entity': clocked}

        for prop, cls in self.history_models.items():
            hist = attr.copy()
            # fires a load on any deferred columns
            if prop.key not in state.dict:
                getattr(clocked, prop.key)

            if isinstance(prop, orm.RelationshipProperty):
                changes = attributes.get_history(
                    clocked,
                    prop.key,
                    passive=attributes.PASSIVE_NO_INITIALIZE)
            else:
                changes = attributes.get_history(clocked, prop.key)

            if changes.added:
                new_clock = self.make_clock(timestamp, clocked.current_clock.tick)
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
                    cls(
                        vclock=new_clock.vclock,
                        effective=new_clock.effective,
                        **hist
                    )
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
    temporal_options = None  # type: TemporalOption
    first_tick = None  # type:  EntityClock
    latest_tick = None  # type:  EntityClock

    # temporal descriptors
    current_clock = None  # type: ClockState
    activity = None  # type: typing.Optional[ActivityState]

    @property
    def date_created(self):
        return self.first_tick.timestamp

    @property
    def date_modified(self):
        return self.latest_tick.timestamp

    @contextlib.contextmanager
    def clock_tick(self, activity: TemporalActivityMixin = None):
        warnings.warn("clock_tick is deprecated, assign an activity directly",
                      DeprecationWarning)
        if self.temporal_options.activity_cls:
            if not activity:
                raise ValueError("activity is missing on edit") from None
            self.activity = activity

        yield self
