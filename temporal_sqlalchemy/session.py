import datetime
import itertools
import random
import typing
import warnings

import sqlalchemy.event as event
import sqlalchemy.orm as orm

from temporal_sqlalchemy.bases import TemporalOption, Clocked
from temporal_sqlalchemy.metadata import (
    get_session_metadata,
    set_session_metadata
)


CHANGESET_STACK = '__temporal_changeset_stack'
IS_COMMITTING = '__temporal_is_committing'
IS_VCLOCK_UNCHANGED = '__temporal_is_vclock_unchanged'


def get_current_changeset(session):
    return session.info[CHANGESET_STACK][-1]


def _temporal_models(session: orm.Session) -> typing.Iterable[Clocked]:
    for obj in session:
        if isinstance(getattr(obj, 'temporal_options', None), TemporalOption):
            yield obj


def _build_history(session, correlate_timestamp):
    changeset = get_current_changeset(session)
    items = list(changeset.items())
    changeset.clear()

    metadata = get_session_metadata(session)
    is_strict_mode = metadata.get('strict_mode', False)

    is_vclock_unchanged = session.info.get(IS_VCLOCK_UNCHANGED, False)
    if items and is_strict_mode:
        assert not is_vclock_unchanged, \
            'commit() has triggered for a changed temporalized property without a clock tick'

    for obj, changes in items:
        obj.temporal_options.record_history_on_commit(obj, changes, session, correlate_timestamp)


def persist_history(session: orm.Session, flush_context, instances):
    if any(_temporal_models(session.deleted)):
        raise ValueError("Cannot delete temporal objects.")

    # its possible the temporal session was initialized after the transaction has started
    _initialize_metadata(session)

    correlate_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
    changed_rows = _temporal_models(itertools.chain(session.dirty, session.new))

    changeset = get_current_changeset(session)
    for obj in changed_rows:
        if obj.temporal_options.allow_persist_on_commit:
            new_changes, is_vclock_unchanged = obj.temporal_options.get_history(obj)

            if new_changes:
                if obj not in changeset:
                    changeset[obj] = {}

                old_changes = changeset[obj]
                old_changes.update(new_changes)

            session.info[IS_VCLOCK_UNCHANGED] = session.info[IS_VCLOCK_UNCHANGED] and is_vclock_unchanged
        else:
            obj.temporal_options.record_history(obj, session, correlate_timestamp)

    # if this is the last flush, build all the history
    if session.info[IS_COMMITTING]:
        _build_history(session, correlate_timestamp)

        session.info[IS_COMMITTING] = False


def enable_is_committing_flag(session):
    """before_commit happens before before_flush, and we need to make sure the history gets built
    during the final one of these two events, so we need to use the gross IS_COMMITTING flag to
    control this behavior"""
    session.info[IS_COMMITTING] = True

    # if the session is clean, a final flush won't happen, so try to build the history now
    if session._is_clean():
        correlate_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        _build_history(session, correlate_timestamp)

    # building the history can cause the session to be dirtied, which will in turn call another
    # flush(), so we want to check this before reseting
    # if there are more changes, flush will build them itself
    if session._is_clean():
        session.info[IS_COMMITTING] = False


def _get_transaction_stack_depth(transaction):
    depth = 0

    current = transaction
    while current:
        depth += 1
        current = transaction.parent

    return depth


ARGH = {}


def _initialize_metadata(session):
    if CHANGESET_STACK not in session.info:
        session.info[CHANGESET_STACK] = []

    if IS_COMMITTING not in session.info:
        session.info[IS_COMMITTING] = False

    if IS_VCLOCK_UNCHANGED not in session.info:
        session.info[IS_VCLOCK_UNCHANGED] = True

    # sometimes temporalize a session after a transaction has already been open, so we need to
    # backfill any missing stack entries
    if len(session.info[CHANGESET_STACK]) == 0:
        depth = _get_transaction_stack_depth(session.transaction)
        for i in range(depth):
            session.info[CHANGESET_STACK].append({})


def start_transaction(session, transaction):
    _initialize_metadata(session)

    session.info[CHANGESET_STACK].append({})

    if len(session.info[CHANGESET_STACK]) > 2:
        pass #import pdb; pdb.set_trace()


def end_transaction(session, transaction):
    # wrap in if statement for cases when the session is temporalized after a transaction has
    # started, and then there are no actual temporal changes
    if len(session.info[CHANGESET_STACK]):
        session.info[CHANGESET_STACK].pop()

    # reset bookkeeping fields if we're ending a top most transaction
    if transaction.parent is None:
        session.info[IS_VCLOCK_UNCHANGED] = True
        session.info[IS_COMMITTING] = False

        # there should be no more changeset stacks at this point, otherwise there is a mismatch
        assert len(session.info[CHANGESET_STACK]) == 0


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
        event.listen(session, 'before_commit', enable_is_committing_flag)

        # nested transaction handling
        event.listen(session, 'after_transaction_create', start_transaction)
        event.listen(session, 'after_transaction_end', end_transaction)

    return session


def is_temporal_session(session: orm.Session) -> bool:
    return isinstance(session, orm.Session) and get_session_metadata(session) is not None
