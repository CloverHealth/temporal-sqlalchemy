# pylint: disable=unused-variable, missing-docstring
import datetime

import pytest
import sqlalchemy as sa
import sqlalchemy.exc as exc
import sqlalchemy.dialects.postgresql as sap

import temporal_sqlalchemy as temporal

from . import models


def test_add_clock_column_verification():
    with pytest.raises(exc.InvalidRequestError):
        @temporal.add_clock('prop_a', 'prop_b', 'prop_c')
        class TempFail(temporal.Clocked, models.ExpectedFailBase):
            __tablename__ = 'temp_fail'
            __table_args__ = {'schema': models.SCHEMA}

            id = models.auto_uuid()
            prop_a = sa.Column(sa.Integer)
            prop_b = sa.Column(sap.TEXT)


def test_fails_with_onupdate():
    with pytest.raises(AssertionError):
        @temporal.add_clock('prop_a', 'prop_b', 'prop_c')
        class TemporalTableWithOnUpdate(
                temporal.Clocked, models.ExpectedFailBase):
            __tablename__ = 'temporal_with_onupdate'
            __table_args__ = {'schema': models.SCHEMA}

            id = models.auto_uuid()
            prop_a = sa.Column(sa.Integer)
            prop_b = sa.Column(sap.TEXT)
            prop_c = sa.Column(sa.DateTime, onupdate=datetime.datetime.now)


def test_fails_with_server_default():
    with pytest.raises(AssertionError):
        @temporal.add_clock('prop_a', 'prop_b', 'prop_c')
        class TemporalTableWithServerDefault(
                temporal.Clocked, models.ExpectedFailBase):
            __tablename__ = 'temporal_with_server_default'
            __table_args__ = {'schema': models.SCHEMA}

            id = models.auto_uuid()
            prop_a = sa.Column(sa.Integer)
            prop_b = sa.Column(sap.TEXT)
            prop_c = sa.Column(sa.DateTime,
                               server_default=sa.func.current_timestamp())


def test_fails_with_server_onupdate():
    with pytest.raises(AssertionError):
        @temporal.add_clock('prop_a', 'prop_b', 'prop_c')
        class TemporalTableWithServerOnUpdate(
                temporal.Clocked, models.ExpectedFailBase):
            __tablename__ = 'temporal_with_server_onupdate'
            __table_args__ = {'schema': models.SCHEMA}

            id = models.auto_uuid()
            prop_a = sa.Column(sa.Integer)
            prop_b = sa.Column(sap.TEXT)
            prop_c = sa.Column(sa.DateTime,
                               server_onupdate=sa.func.current_timestamp())
