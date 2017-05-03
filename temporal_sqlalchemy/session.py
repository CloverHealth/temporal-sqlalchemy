import datetime
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


def temporal_session(session: typing.Union[orm.Session, orm.sessionmaker]) -> orm.Session:
    if not is_temporal_session(session):
        event.listen(session, 'before_flush', persist_history)
        if isinstance(session, orm.Session):
            session.info[TEMPORAL_FLAG] = True
        elif isinstance(session, orm.sessionmaker):
            session.configure(info={TEMPORAL_FLAG: True})
        else:
            raise ValueError('Invalid session')

    return session


def is_temporal_session(session: orm.Session) -> bool:
    return isinstance(session, orm.Session) and session.info.get(TEMPORAL_FLAG)
