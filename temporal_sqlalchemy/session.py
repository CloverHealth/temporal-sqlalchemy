import datetime
import itertools
import typing

import sqlalchemy.event as event
import sqlalchemy.orm as orm
import sqlalchemy.util as util

from temporal_sqlalchemy.bases import TemporalOption, Clocked


TEMPORAL_METADATA_KEY = '__temporal'


def set_session_metadata(session: orm.Session, metadata: dict):
    if isinstance(session, orm.Session):
        session.info[TEMPORAL_METADATA_KEY] = metadata
    elif isinstance(session, orm.sessionmaker):
        session.configure(info={TEMPORAL_METADATA_KEY: metadata})
    else:
        raise ValueError('Invalid session')


def _temporal_models(iset: util.IdentitySet) -> typing.Iterable[Clocked]:
    for obj in iset:
        if isinstance(getattr(obj, 'temporal_options', None), TemporalOption):
            yield obj


def persist_history(session: orm.Session, flush_context, instances):
    if any(_temporal_models(session.deleted)):
        raise ValueError("Cannot delete temporal objects.")

    correlate_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
    for obj in _temporal_models(itertools.chain(session.dirty, session.new)):
        obj.temporal_options.record_history(obj, session, correlate_timestamp)


def temporal_session(session: typing.Union[orm.Session, orm.sessionmaker],
                     **opt) -> orm.Session:
    """
    Setup the session to track changes via temporal

    :param session: SQLAlchemy ORM session to temporalize
    :return: temporalized SQLALchemy ORM session
    """
    if is_temporal_session(session):
        return session

    opt.setdefault('ENABLED', True)  # TODO make this significant
    # update to the latest metadata
    set_session_metadata(session, opt)
    event.listen(session, 'before_flush', persist_history)

    return session


def is_temporal_session(session: orm.Session) -> bool:
    return isinstance(session, orm.Session) and \
           TEMPORAL_METADATA_KEY in session.info
