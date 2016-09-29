import pytest

from . import models


class DatabaseTest:
    
    @pytest.fixture(autouse=True)
    def setup(self, session):
        conn = session.bind
        conn.execute('CREATE SCHEMA IF NOT EXISTS %s' % models.SCHEMA)
        conn.execute('CREATE SCHEMA IF NOT EXISTS %s' % models.TEMPORAL_SCHEMA)
