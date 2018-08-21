""" pytest fixtures for test suite """
import pytest
import sqlalchemy as sa
import sqlalchemy.orm as orm
import testing.postgresql

import temporal_sqlalchemy as temporal
from . import models


@pytest.yield_fixture(scope='session')
def engine():
    """Creates a postgres database for testing, returns a sqlalchemy engine"""
    db = testing.postgresql.Postgresql()

    engine_ = sa.create_engine(db.url())

    yield engine_

    engine_.dispose()
    db.stop()


@pytest.yield_fixture(scope='session')
def connection(engine):  # pylint: disable=redefined-outer-name
    """Session-wide test database."""
    conn = engine.connect()
    for extension in ['uuid-ossp', 'btree_gist']:
        conn.execute("""\
            CREATE EXTENSION IF NOT EXISTS "%s"
            WITH SCHEMA pg_catalog
        """ % extension)

    for schema in [models.SCHEMA, models.TEMPORAL_SCHEMA]:
        conn.execute('CREATE SCHEMA IF NOT EXISTS ' + schema)

    models.basic_metadata.create_all(conn)

    yield conn

    conn.close()


@pytest.yield_fixture(scope="session")
def sessionmaker():
    """ yields a temporalized sessionmaker -- per test session """
    Session = orm.sessionmaker()

    yield temporal.temporal_session(Session)

    Session.close_all()


@pytest.yield_fixture()
def session(connection: sa.engine.Connection, sessionmaker: orm.sessionmaker):  # pylint: disable=redefined-outer-name
    """ yields temporalized session -- per test """
    transaction = connection.begin()
    sess = sessionmaker(bind=connection)

    yield sess

    transaction.rollback()
    sess.close()
