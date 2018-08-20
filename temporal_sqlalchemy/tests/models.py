import datetime

import sqlalchemy as sa
import sqlalchemy.ext.mutable as mutable
import sqlalchemy.orm as orm
import sqlalchemy.dialects.postgresql as sap
import sqlalchemy.ext.declarative as sa_decl

import temporal_sqlalchemy

SCHEMA = 'temporal_test'
TEMPORAL_SCHEMA = 'temporal_history'

basic_metadata = sa.MetaData()
expected_fail_metadata = sa.MetaData()
edgecase_metadata = sa.MetaData()

Base = sa_decl.declarative_base(metadata=basic_metadata)
EdgeCaseBase = sa_decl.declarative_base(metadata=edgecase_metadata)
ExpectedFailBase = sa_decl.declarative_base(metadata=expected_fail_metadata)
AbstractConcreteBase = sa_decl.AbstractConcreteBase


def auto_uuid():
    uuid_gen_expr = sa.text('uuid_generate_v4()')
    return sa.Column(sap.UUID(as_uuid=True),
                     primary_key=True, server_default=uuid_gen_expr)


def utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


@temporal_sqlalchemy.add_clock(
    'prop_a', 'prop_b', 'prop_c', 'prop_d', 'prop_e', 'prop_f',
    temporal_schema=TEMPORAL_SCHEMA)
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


@temporal_sqlalchemy.add_clock(
    'prop_a', 'prop_b', 'prop_c', 'prop_d', 'prop_e', 'prop_f',
    temporal_schema=TEMPORAL_SCHEMA)
class SimpleConcreteChildTemporalTable(
        temporal_sqlalchemy.Clocked, SimpleAbstractConcreteBaseTable, Base):
    __tablename__ = 'simple_concrete_child_a_temporal'
    __table_args__ = {'schema': SCHEMA}

    id = auto_uuid()
    prop_c = sa.Column(sa.DateTime(True))
    prop_d = sa.Column(mutable.MutableDict.as_mutable(sap.JSON))
    prop_e = sa.Column(sap.DATERANGE)
    prop_f = sa.Column(sap.ARRAY(sap.TEXT))

    __mapper_args__ = {'polymorphic_identity': 'child_a', 'concrete': True}


@temporal_sqlalchemy.add_clock(
    'prop_a', 'prop_b', 'prop_default', 'prop_callable', 'prop_func',
    temporal_schema=TEMPORAL_SCHEMA)
class TemporalTableWithDefault(temporal_sqlalchemy.Clocked, Base):
    __tablename__ = 'temporal_with_default'
    __table_args__ = {'schema': SCHEMA}

    id = auto_uuid()
    prop_a = sa.Column(sa.Integer)
    prop_b = sa.Column(sap.TEXT)
    prop_default = sa.Column(sa.Integer, default=10)
    prop_callable = sa.Column(sa.TEXT, default=prop_callable_func)
    prop_func = sa.Column(sa.DateTime, default=sa.func.now())


class RelatedTable(Base):
    __tablename__ = 'relational_related'
    __table_args__ = {'schema': SCHEMA}

    id = auto_uuid()
    prop_a = sa.Column(sa.Integer)


@temporal_sqlalchemy.add_clock(
    'prop_a', 'prop_b', 'rel_id', 'rel', temporal_schema=TEMPORAL_SCHEMA)
class RelationalTemporalModel(temporal_sqlalchemy.Clocked, Base):
    __tablename__ = 'relational_temporal'
    __table_args__ = {'schema': SCHEMA}

    id = auto_uuid()
    prop_a = sa.Column(sa.Integer)
    prop_b = sa.Column(sap.TEXT)
    rel_id = sa.Column(sa.ForeignKey(RelatedTable.id))
    rel = orm.relationship(RelatedTable)


class Activity(temporal_sqlalchemy.TemporalActivityMixin, Base):
    __tablename__ = 'temp_activity_table'
    __table_args__ = {'schema': SCHEMA}

    id = auto_uuid()
    description = sa.Column(sap.TEXT)
    date_created = sa.Column(sa.DateTime(True), default=utcnow, nullable=False)
    date_modified = sa.Column(sa.DateTime(True), default=utcnow,
                              onupdate=utcnow, nullable=False)


@temporal_sqlalchemy.add_clock(
    'column', activity_cls=Activity, temporal_schema=TEMPORAL_SCHEMA)
class FirstTemporalWithActivity(temporal_sqlalchemy.Clocked, Base):
    __tablename__ = 'temporal_with_activity_1'
    __table_args__ = {'schema': SCHEMA}

    id = auto_uuid()
    column = sa.Column(sa.Integer)


@temporal_sqlalchemy.add_clock(
    'column', activity_cls=Activity, temporal_schema=TEMPORAL_SCHEMA)
class SecondTemporalWithActivity(temporal_sqlalchemy.Clocked, Base):
    __tablename__ = 'temporal_with_activity_2'
    __table_args__ = {'schema': SCHEMA}

    id = auto_uuid()
    column = sa.Column(sa.Integer)


class SimpleTable(Base):
    __tablename__ = 'simple_table'
    __table_args__ = {'schema': SCHEMA}

    id = auto_uuid()
    prop_a = sa.Column(sa.Integer)
    prop_b = sa.Column(sap.TEXT)
    rel_id = sa.Column(sa.ForeignKey(RelatedTable.id))
    rel = orm.relationship(RelatedTable)


REALLY_REALLY = 'really_' * 5


@temporal_sqlalchemy.add_clock(REALLY_REALLY + 'long_column')
class HugeIndices(temporal_sqlalchemy.Clocked, EdgeCaseBase):
    __tablename__ = 'testing_a_' + REALLY_REALLY + 'long_table_name'
    __table_args__ = {'schema': SCHEMA}

    id = auto_uuid()
    really_really_really_really_really_long_column = sa.Column(sa.Integer)


class JoinedEnumBase(Base):
    __tablename__ = 'joined_enum_base'
    __table_args__ = {'schema': SCHEMA}

    id = auto_uuid()
    kind = sa.Column(
        sap.ENUM('default', 'enum_a', 'enum_b', name='joined_enum_kind'))
    is_deleted = sa.Column(sa.Boolean, default=False)

    __mapper_args__ = {
        'polymorphic_on': kind,
        'polymorphic_identity': 'default',
    }


@temporal_sqlalchemy.add_clock(
    'val',
    'is_deleted',
    temporal_schema=TEMPORAL_SCHEMA)
class JoinedEnumA(temporal_sqlalchemy.Clocked, JoinedEnumBase):
    __tablename__ = 'joined_enum_a'

    id = sa.Column(sa.ForeignKey(JoinedEnumBase.id), primary_key=True)
    val = sa.Column(sap.ENUM('foo', 'foobar', name='joined_enum_a_val'))

    __mapper_args__ = {'polymorphic_identity': 'enum_a'}


@temporal_sqlalchemy.add_clock(
    'val',
    'is_deleted', temporal_schema=TEMPORAL_SCHEMA)
class JoinedEnumB(temporal_sqlalchemy.Clocked, JoinedEnumBase):
    __tablename__ = 'joined_enum_b'

    id = sa.Column(sa.ForeignKey(JoinedEnumBase.id), primary_key=True)
    val = sa.Column(sap.ENUM('bar', 'barfoo', name='joined_enum_b_val'))

    __mapper_args__ = {'polymorphic_identity': 'enum_b'}


class NewStyleModel(Base, temporal_sqlalchemy.TemporalModel):
    __tablename__ = 'new_style_temporal_model'
    __table_args__ = {'schema': SCHEMA}

    id = auto_uuid()
    description = sa.Column(sa.TEXT)

    int_prop = sa.Column(sa.Integer)
    bool_prop = sa.Column(sa.Boolean)
    datetime_prop = sa.Column(sa.DateTime(True))

    class Temporal:
        activity_class = Activity
        track = ('description', 'int_prop', 'bool_prop', 'datetime_prop')
        schema = TEMPORAL_SCHEMA


class NewStyleModelWithRelationship(Base, temporal_sqlalchemy.TemporalModel):
    __tablename__ = 'new_style_temporal_model_with_relationship'
    __table_args__ = {'schema': SCHEMA}

    id = auto_uuid()
    description = sa.Column(sa.TEXT)

    int_prop = sa.Column(sa.Integer)
    bool_prop = sa.Column(sa.Boolean)
    datetime_prop = sa.Column(sa.DateTime(True))
    rel_id = sa.Column(sa.ForeignKey(RelatedTable.id))
    rel = orm.relationship(RelatedTable)

    class Temporal:
        activity_class = Activity
        track = (
            'description',
            'int_prop',
            'bool_prop',
            'datetime_prop',
            'rel_id',
            'rel',
        )
        schema = TEMPORAL_SCHEMA


class PersistenceStrategy(temporal_sqlalchemy.Clocked, Base):
    __abstract__ = True

    id = auto_uuid()
    prop_a = sa.Column(sa.Integer)
    prop_b = sa.Column(sap.TEXT)
    prop_c = sa.Column(sa.DateTime(True))
    prop_d = sa.Column(mutable.MutableDict.as_mutable(sap.JSON))
    prop_e = sa.Column(sap.DATERANGE)
    prop_f = sa.Column(sap.ARRAY(sap.TEXT))


@temporal_sqlalchemy.add_clock(
    'prop_a', 'prop_b', 'prop_c', 'prop_d', 'prop_e', 'prop_f',
    temporal_schema=TEMPORAL_SCHEMA,
    activity_cls=Activity)
class PersistOnFlushTable(PersistenceStrategy):
    __tablename__ = 'persist_on_flush_table'
    __table_args__ = {'schema': SCHEMA}


@temporal_sqlalchemy.add_clock(
    'prop_a', 'prop_b', 'prop_c', 'prop_d', 'prop_e', 'prop_f',
    temporal_schema=TEMPORAL_SCHEMA,
    activity_cls=Activity,
    allow_persist_on_commit=True)
class PersistOnCommitTable(PersistenceStrategy):
    __tablename__ = 'persist_on_commit_table'
    __table_args__ = {'schema': SCHEMA}


class NonTemporalTable(Base):
    __tablename__ = 'non_temporal_table'
    __table_args__ = {'schema': SCHEMA}

    id = auto_uuid()
