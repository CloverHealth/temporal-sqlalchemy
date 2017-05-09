import datetime
import functools
import itertools
import typing

import sqlalchemy.event as event
import sqlalchemy.orm as orm

from temporal_sqlalchemy.bases import ClockedOption, Clocked

TEMPORAL_FLAG = '__temporal'


def _temporal_models(session: orm.Session) -> typing.Iterable[Clocked]:
    for obj in session:
        if isinstance(getattr(obj, 'temporal_options', None), ClockedOption):
            yield obj


def persist_history(session: orm.Session, flush_context, instances):
    if any(_temporal_models(session.deleted)):
        raise ValueError("Cannot delete temporal objects.")

    correlate_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
    for obj in _temporal_models(itertools.chain(session.dirty, session.new)):
        obj.temporal_options.record_history(obj, session, correlate_timestamp)


@functools.singledispatch
def _flag_as_temporal(session_obj):
    raise ValueError('Invalid session')


@_flag_as_temporal.register(orm.Session)
def _(session_obj):
    session_obj.info[TEMPORAL_FLAG] = True


@_flag_as_temporal.register(orm.sessionmaker)
def _(session_obj):
    session_obj.configure(info={TEMPORAL_FLAG: True})


def temporal_session(session: typing.Union[orm.Session, orm.sessionmaker]) -> orm.Session:
    if not is_temporal_session(session):
        event.listen(session, 'before_flush', persist_history)
        _flag_as_temporal(session)

    return session


def is_temporal_session(session: orm.Session) -> bool:
    return isinstance(session, orm.Session) and session.info.get(TEMPORAL_FLAG)
