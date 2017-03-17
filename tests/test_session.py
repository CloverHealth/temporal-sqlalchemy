import datetime

import pytest

from . import shared, models


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
