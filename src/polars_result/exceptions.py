"""Custom exceptions for polars-result."""

from polars.exceptions import (
    ColumnNotFoundError,
    ComputeError,
    DuplicateError,
    InvalidOperationError,
    NoDataError,
    PolarsError,
    SchemaError,
    ShapeError,
)

# ---- Base ----------------------------------------------------------------


class PolarsResultError(Exception):
    """Base exception for polars-result operations.

    All exceptions in this package carry an optional ``cause`` that preserves
    the original exception — either a raw Polars error or any other exception
    caught during pipeline execution.

    Examples:
        >>> PolarsResultError("something went wrong")
        PolarsResultError('something went wrong')

        >>> try:
        ...     int("bad")
        ... except ValueError as e:
        ...     err = PolarsResultError("parse failed", cause=e)
        >>> err.__cause__ is err.cause
        True
    """

    def __init__(self, message: str, *, cause: BaseException | None = None) -> None:
        super().__init__(message)
        self.cause = cause
        if cause is not None:
            # Mirror ``raise ... from cause``: chain the cause and suppress the
            # implicit "During handling of the above exception" context so the
            # traceback shows one clean chain.
            self.__cause__ = cause
            self.__suppress_context__ = True

    def __repr__(self) -> str:
        if self.cause:
            return f"{type(self).__name__}({self.args[0]!r}, cause={self.cause!r})"
        return f"{type(self).__name__}({self.args[0]!r})"

    @classmethod
    def from_polars(cls, error: PolarsError, operation: str) -> "PolarsResultError":
        """Wrap a raw Polars exception with operation context.

        Maps known Polars exception types (and their subclasses) to the
        appropriate subclass. Falls back to ``PolarsResultError`` for
        unmapped types.

        Args:
            error: The original Polars exception.
            operation: Human-readable description of the operation that failed.

        Returns:
            A ``PolarsResultError`` subclass instance with ``cause`` set.

        Examples:
            >>> from polars.exceptions import ComputeError
            >>> err = PolarsResultError.from_polars(ComputeError("overflow"), "compute totals")
            >>> type(err).__name__
            'PipelineError'
            >>> isinstance(err.cause, ComputeError)
            True
        """
        subtype = next(
            (mapped for base, mapped in _POLARS_ERROR_MAP.items() if isinstance(error, base)),
            cls,
        )
        return subtype(f"{operation}: {error}", cause=error)


# ---- Subclasses ----------------------------------------------------------


class ValidationError(PolarsResultError):
    """Raised when data fails a validation rule.

    Carries the ``field`` and ``value`` that triggered the failure so that
    downstream ``match``/``case`` branches can act on them rather than just
    log the message.

    Examples:
        >>> err = ValidationError("Expected positive", field="amount", value=-3)
        >>> err.field
        'amount'
        >>> err.value
        -3
    """

    def __init__(
        self,
        message: str,
        *,
        field: str | None = None,
        value: object = None,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message, cause=cause)
        self.field = field
        self.value = value

    def __repr__(self) -> str:
        parts = [repr(self.args[0])]
        if self.field is not None:
            parts.append(f"field={self.field!r}")
        if self.value is not None:
            parts.append(f"value={self.value!r}")
        if self.cause is not None:
            parts.append(f"cause={self.cause!r}")
        return f"ValidationError({', '.join(parts)})"


class ResultSchemaError(ValidationError):
    """Raised when a DataFrame schema does not match expectations.

    Maps from ``pl.SchemaError``, ``pl.ColumnNotFoundError``, and
    ``pl.DuplicateError``.

    Carries ``expected`` and ``got`` schema dicts when available, so callers
    can diff them directly.

    Examples:
        >>> err = ResultSchemaError(
        ...     "Schema mismatch on 'sales'",
        ...     expected={"amount": "Float64"},
        ...     got={"amount": "Int32"},
        ... )
        >>> err.expected
        {'amount': 'Float64'}
    """

    def __init__(
        self,
        message: str,
        *,
        expected: dict | None = None,
        got: dict | None = None,
        field: str | None = None,
        value: object = None,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message, field=field, value=value, cause=cause)
        self.expected = expected
        self.got = got

    def __repr__(self) -> str:
        parts = [repr(self.args[0])]
        if self.expected is not None:
            parts.append(f"expected={self.expected!r}")
        if self.got is not None:
            parts.append(f"got={self.got!r}")
        if self.field is not None:
            parts.append(f"field={self.field!r}")
        if self.cause is not None:
            parts.append(f"cause={self.cause!r}")
        return f"ResultSchemaError({', '.join(parts)})"


class PipelineError(PolarsResultError):
    """Raised when a pipeline step fails during execution.

    Maps from ``pl.ComputeError``, ``pl.InvalidOperationError``,
    ``pl.ShapeError``, and ``pl.NoDataError``.

    Carries a ``step`` label so the failing stage is identifiable without
    parsing the message string.

    Examples:
        >>> err = PipelineError("Overflow in aggregation", step="compute_totals")
        >>> err.step
        'compute_totals'
    """

    def __init__(
        self,
        message: str,
        *,
        step: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message, cause=cause)
        self.step = step

    def __repr__(self) -> str:
        parts = [repr(self.args[0])]
        if self.step is not None:
            parts.append(f"step={self.step!r}")
        if self.cause is not None:
            parts.append(f"cause={self.cause!r}")
        return f"PipelineError({', '.join(parts)})"


# ---- Polars error mapping ------------------------------------------------

# Maps raw Polars exception types to the most appropriate subclass.
# from_polars() uses this to produce structured errors without the caller
# needing to know the mapping.
_POLARS_ERROR_MAP: dict[type[PolarsError], type[PolarsResultError]] = {
    SchemaError: ResultSchemaError,
    ColumnNotFoundError: ResultSchemaError,
    DuplicateError: ResultSchemaError,
    ComputeError: PipelineError,
    InvalidOperationError: PipelineError,
    ShapeError: PipelineError,
    NoDataError: PipelineError,
}
