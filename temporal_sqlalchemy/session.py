import datetime
import itertools
import typing

import sqlalchemy.event as event
import sqlalchemy.orm as orm

from temporal_sqlalchemy.bases import ClockedOption, Clocked
from temporal_sqlalchemy.metadata import (
    get_session_metadata,
    set_session_metadata
)


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


def temporal_session(session: typing.Union[orm.Session, orm.sessionmaker], strict_mode=False) -> orm.Session:
    """
    Setup the session to track changes via temporal

    :param session: SQLAlchemy ORM session to temporalize
    :param strict_mode: if True, will raise exceptions when improper flush() calls are made (default is False)
    :return: temporalized SQLALchemy ORM session
    """
    temporal_metadata = {
        'strict_mode': strict_mode
    }

    # defer listening to the flush hook until after we update the metadata
    install_flush_hook = not is_temporal_session(session)

    # update to the latest metadata
    set_session_metadata(session, temporal_metadata)

    if install_flush_hook:
        event.listen(session, 'before_flush', persist_history)

    return session


def is_temporal_session(session: orm.Session) -> bool:
    return isinstance(session, orm.Session) and get_session_metadata(session) is not None
