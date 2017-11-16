import datetime
import itertools
import typing

import sqlalchemy.event as event
import sqlalchemy.orm as orm

from temporal_sqlalchemy.bases import TemporalOption, Clocked
from temporal_sqlalchemy.metadata import (
    get_session_metadata,
    set_session_metadata
)


CHANGESET_KEY = 'changeset'
IS_COMMITTING_KEY = 'is_committing'


def _temporal_models(session: orm.Session) -> typing.Iterable[Clocked]:
    for obj in session:
        if isinstance(getattr(obj, 'temporal_options', None), TemporalOption):
            yield obj


def _build_history(session, correlate_timestamp):
    metadata = get_session_metadata(session)

    items = list(metadata[CHANGESET_KEY].items())
    metadata[CHANGESET_KEY].clear()

    for obj, changes in items:
        obj.temporal_options.record_history_on_commit(obj, changes, session, correlate_timestamp)


def persist_history(session: orm.Session, flush_context, instances):
    if any(_temporal_models(session.deleted)):
        raise ValueError("Cannot delete temporal objects.")

    metadata = get_session_metadata(session)

    correlate_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)

    for obj in _temporal_models(itertools.chain(session.dirty, session.new)):
        if obj.temporal_options.allow_persist_on_commit:
            new_changes = obj.temporal_options.get_history(obj)

            if new_changes:
                if obj not in metadata[CHANGESET_KEY]:
                    metadata[CHANGESET_KEY][obj] = {}

                old_changes = metadata[CHANGESET_KEY][obj]
                old_changes.update(new_changes)
        else:
            obj.temporal_options.record_history(obj, session, correlate_timestamp)

    # if this is the last flush, build all the history
    if metadata[IS_COMMITTING_KEY]:
        _build_history(session, correlate_timestamp)

        metadata[IS_COMMITTING_KEY] = False


def enable_is_committing_flag(session):
    metadata = get_session_metadata(session)
    metadata[IS_COMMITTING_KEY] = True

    if session._is_clean():
        correlate_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        _build_history(session, correlate_timestamp)

    if session._is_clean():
        metadata[IS_COMMITTING_KEY] = False


def temporal_session(session: typing.Union[orm.Session, orm.sessionmaker],
    strict_mode=False, persist_on_commit=False) -> orm.Session:
    """
    Setup the session to track changes via temporal

    :param session: SQLAlchemy ORM session to temporalize
    :param strict_mode: if True, will raise exceptions when improper flush() calls are made (default is False)
    :return: temporalized SQLALchemy ORM session
    """
    temporal_metadata = {
        'strict_mode': strict_mode,
        'persist_on_commit': persist_on_commit,
        CHANGESET_KEY: {},
        IS_COMMITTING_KEY: False,
    }

    # defer listening to the flush hook until after we update the metadata
    install_flush_hook = not is_temporal_session(session)

    # update to the latest metadata
    set_session_metadata(session, temporal_metadata)

    if install_flush_hook:
        event.listen(session, 'before_flush', persist_history)
        event.listen(session, 'before_commit', enable_is_committing_flag)

    return session


def is_temporal_session(session: orm.Session) -> bool:
    return isinstance(session, orm.Session) and get_session_metadata(session) is not None
