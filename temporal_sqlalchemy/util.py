import typing

import sqlalchemy as sa


def foreign_key_to(table: sa.Table, prefix='entity', **opts) -> typing.Iterable[sa.Column]:  # pylint: disable=unsubscriptable-object
    """ generate a columns that support scalar or composite foreign keys to given table """
    for pk in table.primary_key:
        name = '%s_%s' % (prefix, pk.name)
        yield sa.Column(name, pk.type, sa.ForeignKey(pk), **opts)
