import datetime

import sqlalchemy as sa
import sqlalchemy.ext.mutable as mutable
import sqlalchemy.orm as orm
import sqlalchemy.testing.entities as entities
import sqlalchemy.dialects.postgresql as sap
import sqlalchemy.ext.declarative as declarative

import temporal_sqlalchemy

SCHEMA = 'temporal_test'
TEMPORAL_SCHEMA = 'temporal_history'

basic_metadata = sa.MetaData()
expected_fail_metadata = sa.MetaData()
activity_metadata = sa.MetaData()
related_metadata = sa.MetaData()
edgecase_metadata = sa.MetaData()

Base = declarative.declarative_base(cls=entities.ComparableEntity, metadata=basic_metadata)
EdgeCaseBase = declarative.declarative_base(cls=entities.ComparableEntity, metadata=edgecase_metadata)
ExpectedFailBase = declarative.declarative_base(cls=entities.ComparableEntity, metadata=expected_fail_metadata)
ActivityBase = declarative.declarative_base(cls=entities.ComparableEntity, metadata=activity_metadata)
RelatedBase = declarative.declarative_base(cls=entities.ComparableEntity, metadata=related_metadata)
AbstractConcreteBase = declarative.AbstractConcreteBase


def auto_uuid():
    uuid_gen_expr = sa.text('uuid_generate_v4()')
    return sa.Column(sap.UUID(as_uuid=True), primary_key=True, server_default=uuid_gen_expr)


def utcnow() -> datetime.datetime:
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)


@temporal_sqlalchemy.add_clock('prop_a', 'prop_b', 'prop_c', 'prop_d', 'prop_e', 'prop_f', temporal_schema=TEMPORAL_SCHEMA)
class SimpleTableTemporal(temporal_sqlalchemy.Clocked, Base):
    __tablename__ = 'simple_table_temporal'
    __table_args__ = {'schema': SCHEMA}

    id = auto_uuid()
    prop_a = sa.Column(sa.Integer)
    prop_b = sa.Column(sap.TEXT)
    prop_c = sa.Column(sa.DateTime(True))
    prop_d = sa.Column(mutable.MutableDict.as_mutable(sap.JSON))
    prop_e = sa.Column(sap.DATERANGE)
    prop_f = sa.Column(sap.ARRAY(sap.TEXT))


def prop_callable_func():
    return "a value here"


class SimpleAbstractConcreteBaseTable(AbstractConcreteBase):
    prop_a = sa.Column(sa.Integer)
    prop_b = sa.Column(sap.TEXT)


@temporal_sqlalchemy.add_clock('prop_a', 'prop_b', 'prop_c', 'prop_d', 'prop_e', 'prop_f', temporal_schema=TEMPORAL_SCHEMA)
class SimpleConcreteChildTemporalTable(temporal_sqlalchemy.Clocked, SimpleAbstractConcreteBaseTable, Base):
    __tablename__ = 'simple_concrete_child_a_temporal'
    __table_args__ = {'schema': SCHEMA}

    id = auto_uuid()
    prop_c = sa.Column(sa.DateTime(True))
    prop_d = sa.Column(mutable.MutableDict.as_mutable(sap.JSON))
    prop_e = sa.Column(sap.DATERANGE)
    prop_f = sa.Column(sap.ARRAY(sap.TEXT))

    __mapper_args__ = {'polymorphic_identity': 'child_a', 'concrete': True}


@temporal_sqlalchemy.add_clock('prop_a', 'prop_b', 'prop_default', 'prop_callable', 'prop_func', temporal_schema=TEMPORAL_SCHEMA)
class TemporalTableWithDefault(temporal_sqlalchemy.Clocked, Base):
    __tablename__ = 'temporal_with_default'
    __table_args__ = {'schema': SCHEMA}

    id = auto_uuid()
    prop_a = sa.Column(sa.Integer)
    prop_b = sa.Column(sap.TEXT)
    prop_default = sa.Column(sa.Integer, default=10)
    prop_callable = sa.Column(sa.TEXT, default=prop_callable_func)
    prop_func = sa.Column(sa.DateTime, default=sa.func.now())


class RelatedTable(RelatedBase):
    __tablename__ = 'relational_related'
    __table_args__ = {'schema': SCHEMA}

    id = auto_uuid()
    prop_a = sa.Column(sa.Integer)


@temporal_sqlalchemy.add_clock('prop_a', 'prop_b', 'rel_id', temporal_schema=TEMPORAL_SCHEMA)
class RelationalTemporalModel(temporal_sqlalchemy.Clocked, RelatedBase):
    __tablename__ = 'relational_temporal'
    __table_args__ = {'schema': SCHEMA}

    id = auto_uuid()
    prop_a = sa.Column(sa.Integer)
    prop_b = sa.Column(sap.TEXT)
    rel_id = sa.Column(sa.ForeignKey(RelatedTable.id))
    rel = orm.relationship(RelatedTable, info={'temporal_on': 'rel_id'})


class Activity(temporal_sqlalchemy.TemporalActivityMixin, ActivityBase):
    __tablename__ = 'temp_activity_table'
    __table_args__ = {'schema': SCHEMA}

    id = auto_uuid()
    description = sa.Column(sap.TEXT)
    date_created = sa.Column(sa.DateTime(True), default=utcnow, nullable=False)
    date_modified = sa.Column(sa.DateTime(True), default=utcnow, onupdate=utcnow, nullable=False)


@temporal_sqlalchemy.add_clock('column', activity_cls=Activity, temporal_schema=TEMPORAL_SCHEMA)
class FirstTemporalWithActivity(temporal_sqlalchemy.Clocked, ActivityBase):
    __tablename__ = 'temporal_with_activity_1'
    __table_args__ = {'schema': SCHEMA}

    id = auto_uuid()
    column = sa.Column(sa.Integer)


@temporal_sqlalchemy.add_clock('column', activity_cls=Activity, temporal_schema=TEMPORAL_SCHEMA)
class SecondTemporalWithActivity(temporal_sqlalchemy.Clocked, ActivityBase):
    __tablename__ = 'temporal_with_activity_2'
    __table_args__ = {'schema': SCHEMA}

    id = auto_uuid()
    column = sa.Column(sa.Integer)


class SimpleTable(RelatedBase):
    __tablename__ = 'simple_table'
    __table_args__ = {'schema': SCHEMA}

    id = auto_uuid()
    prop_a = sa.Column(sa.Integer)
    prop_b = sa.Column(sap.TEXT)
    rel_id = sa.Column(sa.ForeignKey(RelatedTable.id))
    rel = orm.relationship(RelatedTable, info={'temporal_on': 'rel_id'})


@temporal_sqlalchemy.add_clock('really_really_really_really_really_long_column')
class HugeIndices(temporal_sqlalchemy.Clocked, EdgeCaseBase):
    __tablename__ = 'testing_a_really_really_really_really_really_long_table_name'
    __table_args__ = {'schema': SCHEMA}

    id = auto_uuid()
    really_really_really_really_really_long_column = sa.Column(sa.Integer)
