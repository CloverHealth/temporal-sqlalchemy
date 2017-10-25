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

    def test_errors_on_delete(self, session, newstylemodel):
        session.add(newstylemodel)
        session.commit()

        with pytest.raises(ValueError):
            session.delete(newstylemodel)
            session.commit()

    def test_is_temporal_session_on_temporal_session(self, session):
        # verify temporal session
        assert is_temporal_session(session)

        # verify double-wrapped session
        double_wrapped_session = temporal_session(session)
        assert is_temporal_session(double_wrapped_session)

    def test_is_temporal_session_on_raw_session(self, session, connection):
        with connection.begin():
            session_maker = orm.sessionmaker()
            raw_session = session_maker(bind=connection)
            try:
                assert not is_temporal_session(raw_session)
            finally:
                raw_session.close()

    def test_different_sessions_update_vclock(self, session, connection, sessionmaker, newstylemodel):
        session.add(newstylemodel)
        assert newstylemodel.vclock == 1
        session.commit()

        # create different session
        transaction = connection.begin()
        second_session = sessionmaker(bind=connection)
        refreshed_model = second_session.query(models.NewStyleModel).first()

        # update row within new session
        refreshed_model.activity = models.Activity(description="Activity Description")
        refreshed_model.description = "a new str"
        second_session.add(refreshed_model)
        assert refreshed_model.vclock == 2
        second_session.commit()

        # clear out db
        transaction.rollback()
        second_session.close()
