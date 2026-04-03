"""Decorators for wrapping functions to return Result types."""

import functools
from collections.abc import Callable
from typing import cast, overload

from .exceptions import PipelineError, PolarsResultError
from .result import Err, Ok, Result


@overload
def resultify[**P, R](
    func: Callable[P, R],
) -> Callable[P, Result[R, PipelineError]]: ...


@overload
def resultify[**P, R](
    *,
    catch_types: type[Exception] | tuple[type[Exception], ...] = Exception,
    error_type: type[PolarsResultError] = PipelineError,
) -> Callable[[Callable[P, R]], Callable[P, Result[R, PolarsResultError]]]: ...


def resultify[**P, R](
    func: Callable[P, R] | None = None,
    *,
    catch_types: type[Exception] | tuple[type[Exception], ...] = Exception,
    error_type: type[PolarsResultError] = PipelineError,
) -> (
    Callable[P, Result[R, PolarsResultError]]
    | Callable[[Callable[P, R]], Callable[P, Result[R, PolarsResultError]]]
):
    """Decorator that wraps a function to return a Result.

    Can be used bare or with parameters::

        @resultify
        def load(path: str) -> pl.DataFrame:
            return pl.read_csv(path)

        @resultify(catch_types=FileNotFoundError, error_type=PipelineError)
        def load(path: str) -> pl.DataFrame:
            return pl.read_csv(path)

    Functions returning ``None`` produce ``Ok(None)`` — not an error.
    Functions already returning a ``Result`` are passed through unchanged.
    Exceptions not matched by ``catch_types`` propagate normally.

    Parameters
    ----------
    catch_types:
        Exception type(s) to catch. Defaults to ``Exception`` (catches all).
        Unmatched exceptions propagate normally.
    error_type:
        ``PolarsResultError`` subclass used to wrap caught errors.
        Defaults to ``PipelineError``. The original exception is preserved
        as ``error_type.cause``.
    """

    def decorator(fn: Callable[P, R]) -> Callable[P, Result[R, PolarsResultError]]:
        fn_name = getattr(fn, "__name__", repr(fn))

        @functools.wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Result[R, PolarsResultError]:
            try:
                value = fn(*args, **kwargs)
                if isinstance(value, (Ok, Err)):
                    return cast(Result[R, PolarsResultError], value)
                return cast(Result[R, PolarsResultError], Ok(value))
            except catch_types as e:
                return Err(error_type(f"{fn_name} failed: {e}", cause=e))

        return wrapper

    if func is not None:
        return decorator(func)
    return decorator
