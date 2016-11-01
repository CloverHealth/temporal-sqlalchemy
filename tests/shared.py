import pytest

from . import models


class DatabaseTest:

    @pytest.fixture(autouse=True)
    def schemas(self, session, engine):
        self.engine = engine

        conn = session.bind
        for schema in [models.SCHEMA, models.TEMPORAL_SCHEMA]:
            conn.execute('CREATE SCHEMA IF NOT EXISTS ' + schema)

    def has_table(self, conn, name, schema=models.TEMPORAL_SCHEMA):
        return self.engine.dialect.has_table(conn, name, schema)
