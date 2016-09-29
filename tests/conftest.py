import pytest
import sqlalchemy as sa
import sqlalchemy.orm as orm
import testing.postgresql


@pytest.yield_fixture(scope='session')
def connection():
    """Session-wide test database."""
    db = testing.postgresql.Postgresql()
    engine = sa.create_engine(db.url())
    conn = engine.connect()
    
    conn.execute('CREATE EXTENSION IF NOT EXISTS "%s" WITH SCHEMA pg_catalog' % 'uuid-ossp')
    conn.execute('CREATE EXTENSION IF NOT EXISTS "%s" WITH SCHEMA pg_catalog' % 'btree_gist')
    
    yield conn
    
    conn.close()
    engine.dispose()
    db.stop()


@pytest.yield_fixture(scope="session")
def sessionmaker():
    Session = orm.sessionmaker()
    
    yield Session
    
    Session.close_all()


@pytest.yield_fixture()
def session(connection: sa.engine.Connection, sessionmaker: orm.sessionmaker):
    transaction = connection.begin()
    sess = sessionmaker(bind=connection)
    
    yield sess
    
    transaction.rollback()
    sess.close()
