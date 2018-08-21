"""Python compatibility helpers."""

# Work around typing.Type existing in >= 3.5.2 but not before
try:
    from typing import Type  # pylint: disable=unused-import
except ImportError:  # pragma: no cover
    import typing

    CT = typing.TypeVar('CT', covariant=True, bound=type)

    class Type(type, typing.Generic[CT], extra=type):
        pass


# single dispatch is in functools from >=3.4
try:
    from functools import singledispatch   # pylint: disable=unused-import
except ImportError:  # pragma: no cover
    from singledispatch import singledispatch  # noqa: F401
