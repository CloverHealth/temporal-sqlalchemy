import pytest
import sqlalchemy as sa
import sqlalchemy.orm as orm
import testing.postgresql

import temporal_sqlalchemy as temporal


@pytest.yield_fixture(scope='session')
def engine():
    db = testing.postgresql.Postgresql()

    engine_ = sa.create_engine(db.url())

    yield engine_

    engine_.dispose()
    db.stop()


@pytest.yield_fixture(scope='session')
def connection(engine):
    """Session-wide test database."""
    conn = engine.connect()
    for extension in ['uuid-ossp', 'btree_gist']:
        conn.execute("""\
            CREATE EXTENSION IF NOT EXISTS "%s"
            WITH SCHEMA pg_catalog
        """ % extension)

    yield conn

    conn.close()


@pytest.yield_fixture(scope="session")
def sessionmaker():
    Session = orm.sessionmaker()

    yield temporal.temporal_session(Session)

    Session.close_all()


@pytest.yield_fixture()
def session(connection: sa.engine.Connection, sessionmaker: orm.sessionmaker):
    transaction = connection.begin()
    sess = sessionmaker(bind=connection)

    yield sess

    transaction.rollback()
    sess.close()
