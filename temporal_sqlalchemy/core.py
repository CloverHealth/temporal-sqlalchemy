""" core mixin for temporal Models """

import sqlalchemy.ext.declarative as declarative
import sqlalchemy.orm as orm

from temporal_sqlalchemy import bases, clock


class TemporalModel(bases.Clocked):

    @declarative.declared_attr
    def __mapper_cls__(cls):  # pylint: disable=no-self-argument
        assert hasattr(cls, 'Temporal')

        def mapper(cls_, *args, **kwargs):
            options = cls_.Temporal

            mp = orm.mapper(cls_, *args, **kwargs)
            clock.temporal_map(
                *options.track,
                mapper=mp,
                activity_class=getattr(options, 'activity_class'),
                schema=getattr(options, 'schema'))
            return mp

        return mapper
