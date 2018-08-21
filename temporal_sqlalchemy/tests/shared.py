# pylint: disable=missing-docstring
import pytest

from . import models


class DatabaseTest:

    engine = None
    connection = None

    @pytest.fixture(autouse=True)
    def setup(self, engine, session):
        self.engine = engine
        self.connection = session.bind

    def has_table(self, conn, name, schema=models.TEMPORAL_SCHEMA):
        return self.engine.dialect.has_table(conn, name, schema)
