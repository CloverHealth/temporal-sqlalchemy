# THANKS PYTHON

try:
    from typing import Type
except ImportError:
    import typing

    CT = typing.TypeVar('CT', covariant=True, bound=type)
    
    class Type(type, typing.Generic[CT], extra=type):
        pass
