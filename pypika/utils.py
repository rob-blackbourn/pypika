import copy
from types import TracebackType
from typing import Any, Callable, Generic, List, Optional, Type, TypeVar

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"

T = TypeVar('T')


class QueryException(Exception):
    pass


class GroupingException(Exception):
    pass


class CaseException(Exception):
    pass


class JoinException(Exception):
    pass


class SetOperationException(Exception):
    pass


class RollupException(Exception):
    pass


class DialectNotSupported(Exception):
    pass


class FunctionException(Exception):
    pass


def copy_immutable(obj: T) -> T:
    return copy.copy(obj) if getattr(obj, "immutable", True) else obj


class copy_if_immutable(Generic[T]):

    def __init__(self, obj: T) -> None:
        self.obj = copy.copy(obj) if getattr(obj, "immutable", True) else obj

    def __enter__(self) -> T:
        return self.obj

    def __exit__(self, exc_type: Type[BaseException], exc_value: BaseException, traceback: TracebackType):
        pass


def ignore_copy(func: Callable) -> Callable:
    """
    Decorator for wrapping the __getattr__ function for classes that are copied via deepcopy.  This prevents infinite
    recursion caused by deepcopy looking for magic functions in the class. Any class implementing __getattr__ that is
    meant to be deepcopy'd should use this decorator.

    deepcopy is used by pypika in builder functions (decorated by @builder) to make the results immutable.  Any data
    model type class (stored in the Query instance) is copied.
    """

    def _getattr(self, name):
        if name in [
            "__copy__",
            "__deepcopy__",
            "__getstate__",
            "__setstate__",
            "__getnewargs__",
        ]:
            raise AttributeError(
                "'%s' object has no attribute '%s'" % (self.__class__.__name__, name)
            )

        return func(self, name)

    return _getattr


def resolve_is_aggregate(values: List[Optional[bool]]) -> Optional[bool]:
    """
    Resolves the is_aggregate flag for an expression that contains multiple terms.  This works like a voter system,
    each term votes True or False or abstains with None.

    :param values: A list of booleans (or None) for each term in the expression
    :return: If all values are True or None, True is returned.  If all values are None, None is returned. Otherwise,
        False is returned.
    """
    result = [x for x in values if x is not None]
    if result:
        return all(result)
    return None


def format_quotes(value: Any, quote_char: Optional[str]) -> str:
    return "{quote}{value}{quote}".format(value=value, quote=quote_char or "")


def format_alias_sql(sql: str, alias: Optional[str], quote_char: Optional[str] = None, alias_quote_char: Optional[str] = None, as_keyword: bool = False, **kwargs: Any) -> str:
    if alias is None:
        return sql
    return "{sql}{_as}{alias}".format(
        sql=sql,
        _as=' AS ' if as_keyword else ' ',
        alias=format_quotes(alias, alias_quote_char or quote_char)
    )


def validate(*args: Any, exc: Optional[Exception] = None, type: Optional[Type] = None) -> None:
    if type is not None:
        for arg in args:
            if not isinstance(arg, type):
                raise exc
