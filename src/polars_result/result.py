"""Result type for railway-oriented programming."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Generic, TypeVar

from .exceptions import PolarsResultError
from .formatting import format_result_data

T = TypeVar("T")
E = TypeVar("E", bound=Exception)
U = TypeVar("U")


@dataclass
class Result(Generic[T, E]):
    """Result type: either success (with data) or failure (with error).

    Enables railway-oriented programming where operations are chained
    and errors short-circuit through the pipeline.

    Examples
    --------
    >>> result = Result.ok(42)
    >>> result.map(lambda x: x * 2).unwrap()
    84

    >>> result = Result.err(ValueError("bad"))
    >>> result.map(lambda x: x * 2).is_err()
    True
    """

    success: bool
    data: T | None = None
    error: E | None = None

    # -- constructors --------------------------------------------------------

    @classmethod
    def ok(cls, value: T) -> Result[T, E]:
        """Create a successful Result."""
        if value is None:
            msg = "Cannot create a successful Result with None value"
            raise ValueError(msg)
        return cls(success=True, data=value, error=None)

    @classmethod
    def err(cls, error: E) -> Result[T, E]:
        """Create a failed Result."""
        if error is None:
            msg = "Cannot create an error Result with None error"
            raise ValueError(msg)
        return cls(success=False, data=None, error=error)

    # -- predicates ----------------------------------------------------------

    def is_ok(self) -> bool:
        """Check if Result is successful."""
        return self.success

    def is_err(self) -> bool:
        """Check if Result is an error."""
        return not self.success

    # -- extractors ----------------------------------------------------------

    def unwrap(self) -> T:
        """Get the value, raising if Result is an error.

        Raises
        ------
        GoogleSheetError
            If the Result is an error.
        """
        if not self.success or self.data is None:
            raise PolarsResultError(f"Called unwrap on error Result: {self.error}")
        return self.data

    def unwrap_or(self, default: T) -> T:
        """Get the value or return *default* if Result is an error."""
        if self.success and self.data is not None:
            return self.data
        return default

    def unwrap_err(self) -> E:
        """Get the error, raising if Result is success.

        Raises
        ------
        GoogleSheetError
            If the Result is a success.
        """
        if self.success:
            raise PolarsResultError("Called unwrap_err on success Result")
        assert self.error is not None
        return self.error

    def expect(self, msg: str) -> T:
        """Unwrap with a custom error message.

        Raises
        ------
        GoogleSheetError
            If the Result is an error, with the custom message.
        """
        if not self.success or self.data is None:
            raise PolarsResultError(f"{msg}: {self.error}")
        return self.data

    # -- transformers --------------------------------------------------------

    def map(self, func: Callable[[T], U]) -> Result[U, E]:
        """Apply *func* to the success value; pass errors through unchanged."""
        if not self.success or self.data is None:
            return Result.err(self.error or PolarsResultError("Unknown error"))
        return Result.ok(func(self.data))

    def bind(self, func: Callable[[T], Result[U, E]]) -> Result[U, E]:
        """Chain Result-returning functions (flatMap / and_then)."""
        if not self.success or self.data is None:
            return Result.err(self.error or PolarsResultError("Unknown error"))
        return func(self.data)

    def and_then(self, func: Callable[[T], Result[U, E]]) -> Result[U, E]:
        """Alias for :meth:`bind`."""
        return self.bind(func)

    def map_err(self, func: Callable[[E], Exception]) -> Result[T, Exception]:
        """Transform the error value while leaving success unchanged."""
        if self.success:
            return Result.ok(self.data)
        assert self.error is not None
        return Result.err(func(self.error))

    def or_else(self, func: Callable[[E], Result[T, E]]) -> Result[T, E]:
        """Handle an error with a recovery function."""
        if self.success:
            return self
        assert self.error is not None
        return func(self.error)

    # -- dunder --------------------------------------------------------------

    def __repr__(self) -> str:
        if self.success:
            return f"Result.ok({self.data!r})"
        return f"Result.err({self.error!r})"

    def __bool__(self) -> bool:
        """Allow using Result in boolean context."""
        return self.success

    # -- display -------------------------------------------------------------

    def display(self) -> None:
        """Pretty-print the Result, rendering DataFrames as formatted tables."""
        from .formatting import format_dataframe

        if not self.success:
            print(f"Result.err({self.error!r})")
            return

        try:
            import polars as pl

            if isinstance(self.data, pl.DataFrame):
                print(f"Result.ok - {self.data.height} rows x {self.data.width} cols")
                print(format_dataframe(self.data))
                return
        except ImportError:
            pass

        print(f"Result.ok({self.data!r})")
