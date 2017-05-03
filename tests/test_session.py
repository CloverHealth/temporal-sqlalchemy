import datetime

import pytest
import sqlalchemy.orm as orm

from . import shared, models
from temporal_sqlalchemy import is_temporal_session, temporal_session


class TestSession(shared.DatabaseTest):
    @pytest.fixture()
    def newstylemodel(self):
        return models.NewStyleModel(
            description="desc",
            int_prop=1,
            bool_prop=True,
            activity=models.Activity(description="Activity Description"),
            datetime_prop=datetime.datetime.now(datetime.timezone.utc)
        )

    @pytest.fixture()
    def raw_session_maker(self):
        session = orm.sessionmaker()

        yield session

        session.close_all()

    @pytest.fixture()
    def raw_session(self, connection, raw_session_maker):
        transaction = connection.begin()
        sess = raw_session_maker(bind=connection)

        yield sess

        transaction.rollback()
        sess.close()

    def test_errors_on_delete(self, session, newstylemodel):
        session.add(newstylemodel)
        session.commit()

        with pytest.raises(ValueError):
            session.delete(newstylemodel)
            session.commit()

    def test_is_temporal_session(self, session, raw_session):
        # verify temporal session
        assert is_temporal_session(session)

        # verify double-wrapped session
        double_wrapped_session = temporal_session(session)
        assert is_temporal_session(double_wrapped_session)

        # verify plain session
        assert not is_temporal_session(raw_session)
