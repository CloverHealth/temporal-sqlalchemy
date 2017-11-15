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


def _temporal_models(session: orm.Session) -> typing.Iterable[Clocked]:
    for obj in session:
        if isinstance(getattr(obj, 'temporal_options', None), TemporalOption):
            yield obj


def persist_history(session: orm.Session, flush_context, instances):
    if any(_temporal_models(session.deleted)):
        raise ValueError("Cannot delete temporal objects.")

    correlate_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
    for obj in _temporal_models(itertools.chain(session.dirty, session.new)):
        obj.temporal_options.record_history(obj, session, correlate_timestamp)


def persist_history_on_commit(session: orm.Session, flush_context, instances):
    if any(_temporal_models(session.deleted)):
        raise ValueError("Cannot delete temporal objects.")

    metadata = get_session_metadata(session)

    for obj in _temporal_models(itertools.chain(session.dirty, session.new)):
        metadata['changeset'].add(obj)

    if metadata['is_committing']:
        correlate_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)

        for obj in metadata['changeset']:
            obj.temporal_options.record_history(obj, session, correlate_timestamp)

        metadata['changeset'].clear()


def enable_is_committing_flag(session):
    metadata = get_session_metadata(session)
    metadata['is_committing'] = True


def disable_is_committing_flag(session):
    metadata = get_session_metadata(session)
    metadata['is_committing'] = False


PERSIST_ON_COMMIT_LISTENERS = (
    ('before_flush', persist_history_on_commit),
    ('before_commit', enable_is_committing_flag),
    ('after_commit', disable_is_committing_flag),
)


PERSIST_LISTENERS = (
    ('before_flush', persist_history),
)


def set_listeners(session, to_enable=(), to_disable=()):
    for listener_args in to_disable:
        if event.contains(session, *listener_args):
            event.remove(session, *listener_args)

    for listener_args in to_enable:
        event.listen(session, *listener_args)


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
    }
    if persist_on_commit:
        temporal_metadata.update({
            'persist_on_commit': persist_on_commit,
            'changeset': set(),
            'is_committing': False,
        })

    # defer listening to the flush hook until after we update the metadata
    old_metadata = get_session_metadata(session) or {}
    install_flush_hook = not is_temporal_session(session) \
        or persist_on_commit != old_metadata.get('persist_on_commit', False)

    # update to the latest metadata
    set_session_metadata(session, temporal_metadata)

    if install_flush_hook:
        if persist_on_commit:
            set_listeners(
                session,
                to_enable=PERSIST_ON_COMMIT_LISTENERS,
                to_disable=PERSIST_LISTENERS,
            )
        else:
            set_listeners(
                session,
                to_enable=PERSIST_LISTENERS,
                to_disable=PERSIST_ON_COMMIT_LISTENERS,
            )

    return session


def is_temporal_session(session: orm.Session) -> bool:
    return isinstance(session, orm.Session) and get_session_metadata(session) is not None
