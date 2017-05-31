import sqlalchemy.orm as orm

TEMPORAL_METADATA_KEY = '__temporal'

__all__ = [
    'get_session_metadata',
    'set_session_metadata',
]


def set_session_metadata(session: orm.Session, metadata: dict):
    if isinstance(session, orm.Session):
        session.info[TEMPORAL_METADATA_KEY] = metadata
    elif isinstance(session, orm.sessionmaker):
        session.configure(info={TEMPORAL_METADATA_KEY: metadata})
    else:
        raise ValueError('Invalid session')


def get_session_metadata(session: orm.Session) -> dict:
    """
    :return: metadata dictionary, or None if it was never installed
    """
    return session.info.get(TEMPORAL_METADATA_KEY)
