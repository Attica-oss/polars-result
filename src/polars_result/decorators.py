"""Decorators for wrapping functions to return Result types."""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import cast, overload

from .exceptions import PipelineError
from .result import Err, Ok, Result


@overload
def resultify[**P, R](func: Callable[P, R]) -> Callable[P, Result[R, Exception]]: ...


@overload
def resultify[**P, R](
    *,
    catch: type[Exception] | tuple[type[Exception], ...] = Exception,
    error_type: type[Exception] = PipelineError,
) -> Callable[[Callable[P, R]], Callable[P, Result[R, Exception]]]: ...


def resultify[**P, R](
    func: Callable[P, R] | None = None,
    *,
    catch: type[Exception] | tuple[type[Exception], ...] = Exception,
    error_type: type[Exception] = PipelineError,
) -> (
    Callable[P, Result[R, Exception]]
    | Callable[[Callable[P, R]], Callable[P, Result[R, Exception]]]
):
    """Decorator that wraps a function to return a Result.

    Can be used bare or with parameters::

        @resultify
        def load(path: str) -> pl.DataFrame:
            return pl.read_csv(path)

        @resultify(catch=FileNotFoundError, error_type=PipelineError)
        def load(path: str) -> pl.DataFrame:
            return pl.read_csv(path)

    Functions returning ``None`` produce ``Result.ok(None)`` (not an error).
    Functions already returning a ``Result`` are passed through unchanged.

    Parameters
    ----------
    catch
        Exception type(s) to catch. Uncaught exceptions propagate normally.
    error_type
        Exception class used to wrap caught errors. Defaults to ``PipelineError``.
    """

    def decorator(fn: Callable[P, R]) -> Callable[P, Result[R, Exception]]:
        fn_name = getattr(fn, "__name__", repr(fn))

        @functools.wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Result[R, Exception]:
            try:
                value = fn(*args, **kwargs)

                # If the function already returns a Result, pass it through
                if isinstance(value, (Ok, Err)):
                    return cast(Result[R, Exception], value)

                # Wrap the value in Ok (handles None as well)
                return Ok(value)

            except catch as e:
                return Err(error_type(f"{fn_name} failed: {e}"))

        return wrapper

    if func is not None:
        # Used as @resultify without parentheses
        return decorator(func)

    # Used as @resultify(...) with parameters
    return decorator
