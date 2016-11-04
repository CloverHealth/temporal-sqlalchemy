import datetime
import itertools

import pytz
import sqlalchemy.event as event
import sqlalchemy.orm as orm

from temporal_sqlalchemy.bases import ClockedOption


def _temporal_models(session):
    # type: (orm.Session) -> Iterable[Clocked]
    for obj in session:
        if isinstance(getattr(obj, 'temporal_options', None), ClockedOption):
            yield obj


def persist_history(session, flush_context, instances):
    # type: (orm.Session, ...) -> None
    if any(_temporal_models(session.deleted)):
        raise ValueError("Cannot delete temporal objects.")

    correlate_timestamp = datetime.datetime.now(tz=pytz.utc)
    for obj in _temporal_models(itertools.chain(session.dirty, session.new)):
        obj.temporal_options.record_history(obj, session, correlate_timestamp)


def temporal_session(session):
    # type: (orm.Session) -> orm.Session
    event.listen(session, 'before_flush', persist_history)
    return session
