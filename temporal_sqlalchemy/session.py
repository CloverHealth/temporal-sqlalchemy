""" temporal session handling """
import datetime
import itertools
import typing
import warnings

import sqlalchemy.event as event
import sqlalchemy.orm as orm

from temporal_sqlalchemy.bases import TemporalOption, Clocked
from temporal_sqlalchemy.metadata import (
    STRICT_MODE_KEY,
    CHANGESET_STACK_KEY,
    IS_COMMITTING_KEY,
    IS_VCLOCK_UNCHANGED_KEY,
)


def get_current_changeset(session):
    stack = session.info[CHANGESET_STACK_KEY]
    assert stack

    return stack[-1]


def _temporal_models(session: orm.Session) -> typing.Iterable[Clocked]:
    for obj in session:
        if isinstance(getattr(obj, 'temporal_options', None), TemporalOption):
            yield obj


def _build_history(session, correlate_timestamp):
    # this shouldn't happen, but it might happen, log a warning and investigate
    if not session.info.get(CHANGESET_STACK_KEY):
        warnings.warn('changeset_stack is missing but we are in _build_history()')
        return

    changeset = get_current_changeset(session)
    items = list(changeset.items())
    changeset.clear()

    is_strict_mode = session.info[STRICT_MODE_KEY]
    is_vclock_unchanged = session.info[IS_VCLOCK_UNCHANGED_KEY]
    if items and is_strict_mode:
        assert not is_vclock_unchanged, \
            'commit() has triggered for a changed temporalized property without a clock tick'

    for obj, changes in items:
        obj.temporal_options.record_history_on_commit(obj, changes, session, correlate_timestamp)


def persist_history(session: orm.Session, flush_context, instances):  # pylint: disable=unused-argument
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

            session.info[IS_VCLOCK_UNCHANGED_KEY] = session.info[IS_VCLOCK_UNCHANGED_KEY] and is_vclock_unchanged
        else:
            obj.temporal_options.record_history(obj, session, correlate_timestamp)

    # if this is the last flush, build all the history
    if session.info[IS_COMMITTING_KEY]:
        _build_history(session, correlate_timestamp)

        session.info[IS_COMMITTING_KEY] = False


def enable_is_committing_flag(session):
    """before_commit happens before before_flush, and we need to make sure the history gets built
    during the final one of these two events, so we need to use the gross IS_COMMITTING_KEY flag to
    control this behavior"""
    session.info[IS_COMMITTING_KEY] = True

    # if the session is clean, a final flush won't happen, so try to build the history now
    if session._is_clean():  # pylint: disable=protected-access
        correlate_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        _build_history(session, correlate_timestamp)

    # building the history can cause the session to be dirtied, which will in turn call another
    # flush(), so we want to check this before reseting
    # if there are more changes, flush will build them itself
    if session._is_clean():  # pylint: disable=protected-access
        session.info[IS_COMMITTING_KEY] = False


def _get_transaction_stack_depth(transaction):
    depth = 0

    current = transaction
    while current:
        depth += 1
        current = transaction.parent

    return depth


def _initialize_metadata(session):
    if CHANGESET_STACK_KEY not in session.info:
        session.info[CHANGESET_STACK_KEY] = []

    if IS_COMMITTING_KEY not in session.info:
        session.info[IS_COMMITTING_KEY] = False

    if IS_VCLOCK_UNCHANGED_KEY not in session.info:
        session.info[IS_VCLOCK_UNCHANGED_KEY] = True

    # sometimes temporalize a session after a transaction has already been open, so we need to
    # backfill any missing stack entries
    if not session.info[CHANGESET_STACK_KEY]:
        depth = _get_transaction_stack_depth(session.transaction)
        for _ in range(depth):
            session.info[CHANGESET_STACK_KEY].append({})


def start_transaction(session, transaction):  # pylint: disable=unused-argument
    _initialize_metadata(session)

    session.info[CHANGESET_STACK_KEY].append({})


def end_transaction(session, transaction):
    # there are some edge cases where no temporal changes actually happen, so don't bother
    if not session.info.get(CHANGESET_STACK_KEY):
        return

    session.info[CHANGESET_STACK_KEY].pop()

    # reset bookkeeping fields if we're ending a top most transaction
    if transaction.parent is None:
        session.info[IS_VCLOCK_UNCHANGED_KEY] = True
        session.info[IS_COMMITTING_KEY] = False

        # there should be no more changeset stacks at this point, otherwise there is a mismatch
        assert not session.info[CHANGESET_STACK_KEY]


def temporal_session(session: typing.Union[orm.Session, orm.sessionmaker], strict_mode=False) -> orm.Session:
    """
    Setup the session to track changes via temporal

    :param session: SQLAlchemy ORM session to temporalize
    :param strict_mode: if True, will raise exceptions when improper flush() calls are made (default is False)
    :return: temporalized SQLALchemy ORM session
    """
    # defer listening to the flush hook until after we update the metadata
    install_flush_hook = not is_temporal_session(session)

    if isinstance(session, orm.Session):
        session.info[STRICT_MODE_KEY] = strict_mode
    elif isinstance(session, orm.sessionmaker):
        session.configure(info={STRICT_MODE_KEY: strict_mode})
    else:
        raise ValueError('Invalid session')

    if install_flush_hook:
        event.listen(session, 'before_flush', persist_history)
        event.listen(session, 'before_commit', enable_is_committing_flag)

        # nested transaction handling
        event.listen(session, 'after_transaction_create', start_transaction)
        event.listen(session, 'after_transaction_end', end_transaction)

    return session


def is_temporal_session(session: orm.Session) -> bool:
    return isinstance(session, orm.Session) and session.info.get(STRICT_MODE_KEY) is not None
