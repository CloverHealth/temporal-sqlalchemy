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
import sqlalchemy.orm.attributes as attributes
import psycopg2.extras as psql_extras

from temporal_sqlalchemy import nine
from temporal_sqlalchemy.metadata import get_session_metadata

_ClockSet = collections.namedtuple('_ClockSet', ('effective', 'vclock'))

T_PROPS = typing.TypeVar(
    'T_PROP', orm.RelationshipProperty, orm.ColumnProperty)

NOT_FOUND_SENTINEL = object()


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
            activity_cls: nine.Type[TemporalActivityMixin] = None,
            allow_persist_on_commit: bool = False):
        self.history_models = history_models
        self.temporal_props = temporal_props

        self.clock_model = clock_model
        self.activity_cls = activity_cls

        self.allow_persist_on_commit = allow_persist_on_commit

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
        new_tick = self._get_new_tick(clocked)

        is_strict_mode = get_session_metadata(session).get('strict_mode', False)
        vclock_history = attributes.get_history(clocked, 'vclock')
        is_vclock_unchanged = vclock_history.unchanged and new_tick == vclock_history.unchanged[0]

        new_clock = self.make_clock(timestamp, new_tick)
        attr = {'entity': clocked}

        for prop, cls in self.history_models.items():
            value = self._get_prop_value(clocked, prop)

            if value is not NOT_FOUND_SENTINEL:
                if is_strict_mode:
                    assert not is_vclock_unchanged, \
                        'flush() has triggered for a changed temporalized property outside of a clock tick'

                self._cap_previous_history_row(clocked, new_clock, cls)

                # Add new history row
                hist = attr.copy()
                hist[prop.key] = value
                session.add(
                    cls(
                        vclock=new_clock.vclock,
                        effective=new_clock.effective,
                        **hist
                    )
                )

    def record_history_on_commit(self,
                       clocked: 'Clocked',
                       changes: dict,
                       session: orm.Session,
                       timestamp: dt.datetime):
        """record all history for a given clocked object"""
        new_tick = self._get_new_tick(clocked)

        new_clock = self.make_clock(timestamp, new_tick)
        attr = {'entity': clocked}

        for prop, cls in self.history_models.items():
            if prop in changes:
                value = changes[prop]

                self._cap_previous_history_row(clocked, new_clock, cls)

                # Add new history row
                hist = attr.copy()
                hist[prop.key] = value
                session.add(
                    cls(
                        vclock=new_clock.vclock,
                        effective=new_clock.effective,
                        **hist
                    )
                )

    def get_history(self, clocked: 'Clocked'):
        history = {}

        for prop, cls in self.history_models.items():
            value = self._get_prop_value(clocked, prop)

            if value is not NOT_FOUND_SENTINEL:
                history[prop] = value

        return history

    def _cap_previous_history_row(self, clocked, new_clock, cls):
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

    def _get_prop_value(self, clocked, prop):
        state = attributes.instance_state(clocked)

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
            return changes.added[0]

        return NOT_FOUND_SENTINEL

    def _get_new_tick(self, clocked):
        state = attributes.instance_state(clocked)
        try:
            new_tick = state.dict['vclock']
        except KeyError:
            # TODO understand why this is necessary
            new_tick = getattr(clocked, 'vclock')

        return new_tick


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

    @property
    def date_created(self):
        return self.first_tick.timestamp

    @property
    def date_modified(self):
        return self.latest_tick.timestamp

    @contextlib.contextmanager
    def clock_tick(self, activity: TemporalActivityMixin = None):
        warnings.warn("clock_tick is going away in 0.5.0",
                      PendingDeprecationWarning)
        """Increments vclock by 1 with changes scoped to the session"""
        if self.temporal_options.activity_cls is not None and activity is None:
            raise ValueError("activity is missing on edit") from None

        session = orm.object_session(self)
        with session.no_autoflush:
            yield self

        if session.is_modified(self):
            self.vclock += 1

            new_clock_tick = self.temporal_options.clock_model(
                entity=self, tick=self.vclock)
            if activity is not None:
                new_clock_tick.activity = activity

            session.add(new_clock_tick)
