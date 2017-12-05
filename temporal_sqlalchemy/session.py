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


CHANGESET_STACK = 'changeset_stack'
IS_COMMITTING = 'is_committing'
IS_VCLOCK_UNCHANGED = 'is_vclock_unchanged'


def get_current_changeset(metadata):
    return metadata[CHANGESET_STACK][-1]


def _temporal_models(session: orm.Session) -> typing.Iterable[Clocked]:
    for obj in session:
        if isinstance(getattr(obj, 'temporal_options', None), TemporalOption):
            yield obj


def _build_history(session, correlate_timestamp):
    metadata = get_session_metadata(session)
    changeset = get_current_changeset(metadata)
    items = list(changeset.items())
    changeset.clear()

    is_strict_mode = metadata.get('strict_mode', False)
    is_vclock_unchanged = metadata.get(IS_VCLOCK_UNCHANGED, False)
    if items and is_strict_mode:
        assert not is_vclock_unchanged, \
            'commit() has triggered for a changed temporalized property without a clock tick'

    for obj, changes in items:
        obj.temporal_options.record_history_on_commit(obj, changes, session, correlate_timestamp)


def persist_history(session: orm.Session, flush_context, instances):
    if any(_temporal_models(session.deleted)):
        raise ValueError("Cannot delete temporal objects.")

    correlate_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
    changed_rows = _temporal_models(itertools.chain(session.dirty, session.new))

    metadata = get_session_metadata(session)
    changeset = get_current_changeset(metadata)
    for obj in changed_rows:
        if obj.temporal_options.allow_persist_on_commit:
            new_changes, is_vclock_unchanged = obj.temporal_options.get_history(obj)

            if new_changes:
                if obj not in changeset:
                    changeset[obj] = {}

                old_changes = changeset[obj]
                old_changes.update(new_changes)

            metadata[IS_VCLOCK_UNCHANGED] = metadata[IS_VCLOCK_UNCHANGED] and is_vclock_unchanged
        else:
            obj.temporal_options.record_history(obj, session, correlate_timestamp)

    # if this is the last flush, build all the history
    if metadata[IS_COMMITTING]:
        _build_history(session, correlate_timestamp)

        metadata[IS_COMMITTING] = False


def enable_is_committing_flag(session):
    metadata = get_session_metadata(session)

    metadata[IS_COMMITTING] = True

    if session._is_clean():
        correlate_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        _build_history(session, correlate_timestamp)

    # building the history can cause the session to be dirtied, which will in turn call another
    # flush(), so we want to check this before reseting
    if session._is_clean():
        metadata[IS_COMMITTING] = False


def start_transaction(session, transaction):
    metadata = get_session_metadata(session)

    if transaction.parent is None:
        metadata[CHANGESET_STACK] = []
        metadata[IS_COMMITTING] = False
        metadata[IS_VCLOCK_UNCHANGED] = True

    metadata[CHANGESET_STACK].append({})


def end_transaction(session, transaction):
    metadata = get_session_metadata(session)

    # clear out bookkeeping fields if we're ending a top most transaction
    if transaction.parent is None:
        del metadata[CHANGESET_STACK]
        del metadata[IS_COMMITTING]
        del metadata[IS_VCLOCK_UNCHANGED]
    else:
        metadata[CHANGESET_STACK].pop()


def temporal_session(session: typing.Union[orm.Session, orm.sessionmaker], strict_mode=False) -> orm.Session:
    """
    Setup the session to track changes via temporal

    :param session: SQLAlchemy ORM session to temporalize
    :param strict_mode: if True, will raise exceptions when improper flush() calls are made (default is False)
    :return: temporalized SQLALchemy ORM session
    """
    # sqlalchemy does some weird memoizing / localizing the info dict to each session, so we have
    # to copy the dictionary if we're just updating it. We now have additional metadata beyond
    # strict_mode that can't be destroyed between function calls.
    old_metadata = get_session_metadata(session)
    temporal_metadata = old_metadata.copy() if old_metadata else {}
    temporal_metadata['strict_mode'] = strict_mode

    # defer listening to the flush hook until after we update the metadata
    install_flush_hook = not is_temporal_session(session)

    # update to the latest metadata
    set_session_metadata(session, temporal_metadata)

    if install_flush_hook:
        event.listen(session, 'before_flush', persist_history)
        event.listen(session, 'before_commit', enable_is_committing_flag)

        # nested transaction handling
        event.listen(session, 'after_transaction_create', start_transaction)
        event.listen(session, 'after_transaction_end', end_transaction)

    return session


def is_temporal_session(session: orm.Session) -> bool:
    return isinstance(session, orm.Session) and get_session_metadata(session) is not None
