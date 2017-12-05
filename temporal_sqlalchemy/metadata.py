import sqlalchemy.orm as orm

__all__ = [
    'STRICT_MODE_KEY',
    'CHANGESET_STACK_KEY',
    'IS_COMMITTING_KEY',
    'IS_VCLOCK_UNCHANGED_KEY',
]


STRICT_MODE_KEY = '__temporal_strict_mode'
CHANGESET_STACK_KEY = '__temporal_changeset_stack'
IS_COMMITTING_KEY = '__temporal_is_committing'
IS_VCLOCK_UNCHANGED_KEY = '__temporal_is_vclock_unchanged'