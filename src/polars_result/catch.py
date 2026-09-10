"""Run a callable and convert a raised exception into an ``Err``."""

from collections.abc import Callable
from typing import cast

from polars.exceptions import PolarsError

from .exceptions import PipelineError, PolarsResultError
from .result import Err, Ok, Result


def catch[R](
    fn: Callable[[], R],
    *,
    catch_types: type[Exception] | tuple[type[Exception], ...] = Exception,
    error_type: type[PolarsResultError] = PipelineError,
    context: str | None = None,
) -> Result[R, PolarsResultError]:
    """Call ``fn`` and turn a caught exception into ``Err``.

    ``fn`` returning ``None`` produces ``Ok(None)`` — not an error. If ``fn``
    already returns a ``Result`` it is passed through unchanged. Exceptions
    not matched by ``catch_types`` propagate normally.

    A caught ``polars.exceptions.PolarsError`` is mapped to its structured
    :class:`PolarsResultError` subclass via
    :meth:`PolarsResultError.from_polars`; ``error_type`` wraps every other
    caught exception. The original exception is preserved as ``.cause``.

    Examples:
        >>> catch(lambda: int("7"))
        Ok(7)

        >>> r = catch(lambda: int("bad"), catch_types=ValueError, context="parse count")
        >>> r.is_err(), type(r.unwrap_err()).__name__
        (True, 'PipelineError')
    """
    try:
        value = fn()
    except catch_types as e:
        if isinstance(e, PolarsError):
            return Err(PolarsResultError.from_polars(e, context or "operation"))
        message = f"{context}: {e}" if context else str(e)
        return Err(error_type(message, cause=e))

    if isinstance(value, (Ok, Err)):
        return cast(Result[R, PolarsResultError], value)
    return cast(Result[R, PolarsResultError], Ok(value))
