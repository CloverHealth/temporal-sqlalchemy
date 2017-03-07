"""Python compatibility helpers."""

# Work around typing.Type existing in >= 3.5.2 but not before
try:
    from typing import Type
except ImportError:  # pragma: no cover
    import typing

    CT = typing.TypeVar('CT', covariant=True, bound=type)

    class Type(type, typing.Generic[CT], extra=type):
        pass
