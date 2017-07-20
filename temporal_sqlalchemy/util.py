import datetime as dt
import typing

import psycopg2.extras as psql_extras
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sap
import sqlalchemy.orm as orm
import sqlalchemy.orm.attributes as attributes
import sqlalchemy.util as sa_util

from temporal_sqlalchemy import bases


def foreign_key_to(table: sa.Table, prefix='entity', **opts) -> typing.Iterable[sa.Column]:  # pylint: disable=unsubscriptable-object
    """ generate a columns that support scalar or composite foreign keys to given table """
    for pk in table.primary_key:
        name = '%s_%s' % (prefix, pk.name)
        yield sa.Column(name, pk.type, sa.ForeignKey(pk), **opts)


def effective_now() -> psql_extras.DateTimeTZRange:
    utc_now = dt.datetime.now(tz=dt.timezone.utc)
    return psql_extras.DateTimeTZRange(utc_now, None)


def copy_column(column: sa.Column) -> sa.Column:
    """copy a column, set some properties on it for history table creation"""
    original = column
    new = column.copy()
    original.info['history_copy'] = new
    for fk in column.foreign_keys:
        new.append_foreign_key(sa.ForeignKey(fk.target_fullname))
    new.unique = False
    new.default = new.server_default = None

    return new


def truncate_identifier(identifier: str) -> str:
    """ensure identifier doesn't exceed max characters postgres allows"""
    max_len = (sap.dialect.max_index_name_length
               or sap.dialect.max_identifier_length)
    if len(identifier) > max_len:
        return "%s_%s" % (identifier[0:max_len - 8],
                          sa_util.md5_hex(identifier)[-4:])
    return identifier


def get_activity_clock_backref(
        activity: bases.TemporalActivityMixin,
        entity: bases.Clocked) -> orm.RelationshipProperty:
    """Get the backref'd clock history for a given entity."""
    assert (
        activity is entity.temporal_options.activity_cls or
        isinstance(activity, entity.temporal_options.activity_cls)
    ), "cannot inspect %r for mapped activity %r" % (entity, activity)

    inspected = sa.inspect(entity.temporal_options.activity_cls)
    backref = entity.temporal_options.clock_table.activity.property.backref

    return inspected.relationships[backref]


def get_history_model(
        target: attributes.InstrumentedAttribute) -> bases.TemporalProperty:
    """Get the history model for given entity class."""
    assert hasattr(target.class_, 'temporal_options')

    return target.class_.temporal_options.history_tables[target.property]
