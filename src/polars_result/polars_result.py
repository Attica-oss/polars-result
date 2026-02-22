"""Polars operations that return Result types instead of raising exceptions."""

from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar, overload

import polars as pl
from polars.exceptions import PolarsError

from .result import Err, Ok, Result

T = TypeVar("T")
E = TypeVar("E", bound=Exception)


# ---- Exception to Result Converter ----


@overload
def catch[T](fn: Callable[[], T]) -> Result[T, Exception]: ...


@overload
def catch[T, E: Exception](fn: Callable[[], T], error_type: type[E]) -> Result[T, E]: ...


def catch[T](fn: Callable[[], T], error_type: type[Exception] = Exception) -> Result[T, Exception]:
    """Execute a function and convert any exception to a Result.

    Args:
        fn: Function to execute
        error_type: Type of exception to catch (defaults to Exception)

    Returns:
        Ok with the result, or Err with the caught exception

    Examples:
        >>> catch(lambda: 1 / 0)
        Err(ZeroDivisionError('division by zero'))

        >>> catch(lambda: int("42"))
        Ok(42)

        >>> catch(lambda: int("bad"), ValueError)
        Err(ValueError("invalid literal..."))
    """
    try:
        return Ok(fn())
    except error_type as e:
        return Err(e)


def polars_catch[T](fn: Callable[[], T], operation: str) -> Result[T, PolarsError]:
    """Execute a Polars operation and wrap exceptions as PolarsError.

    Args:
        fn: Polars operation to execute
        operation: Description of the operation for error messages

    Returns:
        Ok with the result, or Err with PolarsError
    """
    return catch(fn).map_err(lambda e: PolarsError(f"{operation}: {e}"))


class PolarsResult:
    """Namespace for Polars operations that return Results."""

    # ---- I/O Operations ----

    @staticmethod
    def read_csv(path: str | Path, **kwargs: Any) -> Result[pl.DataFrame, PolarsError]:
        """Read a CSV file, returning a Result instead of raising."""
        return polars_catch(lambda: pl.read_csv(path, **kwargs), f"Failed to read CSV '{path}'")

    @staticmethod
    def read_parquet(path: str | Path, **kwargs: Any) -> Result[pl.DataFrame, PolarsError]:
        """Read a Parquet file, returning a Result instead of raising."""
        return polars_catch(
            lambda: pl.read_parquet(path, **kwargs), f"Failed to read Parquet '{path}'"
        )

    @staticmethod
    def read_json(path: str | Path, **kwargs: Any) -> Result[pl.DataFrame, PolarsError]:
        """Read a JSON file, returning a Result instead of raising."""
        return polars_catch(lambda: pl.read_json(path, **kwargs), f"Failed to read JSON '{path}'")

    @staticmethod
    def read_excel(path: str | Path, **kwargs: Any) -> Result[pl.DataFrame, PolarsError]:
        """Read an Excel file, returning a Result instead of raising."""
        return polars_catch(
            lambda: pl.read_excel(path, **kwargs), f"Failed to read Excel '{path}'"
        )

    @staticmethod
    def scan_parquet(path: str | Path, **kwargs: Any) -> Result[pl.LazyFrame, PolarsError]:
        """Scan a Parquet file lazily, returning a Result instead of raising."""
        return polars_catch(
            lambda: pl.scan_parquet(path, **kwargs), f"Failed to scan Parquet '{path}'"
        )

    @staticmethod
    def scan_csv(path: str | Path, **kwargs: Any) -> Result[pl.LazyFrame, PolarsError]:
        """Scan a CSV file lazily, returning a Result instead of raising."""
        return polars_catch(lambda: pl.scan_csv(path, **kwargs), f"Failed to scan CSV '{path}'")

    # ---- DataFrame Construction ----

    @staticmethod
    def from_dict(data: dict, **kwargs: Any) -> Result[pl.DataFrame, PolarsError]:
        """Create a DataFrame from dict, returning a Result instead of raising."""
        return polars_catch(
            lambda: pl.DataFrame(data, **kwargs), "Failed to create DataFrame from dict"
        )

    @staticmethod
    def from_records(data: list[dict], **kwargs: Any) -> Result[pl.DataFrame, PolarsError]:
        """Create a DataFrame from records, returning a Result instead of raising."""
        return polars_catch(
            lambda: pl.DataFrame(data, **kwargs), "Failed to create DataFrame from records"
        )

    @staticmethod
    def from_arrow(data: Any, **kwargs: Any) -> Result[pl.DataFrame, PolarsError]:
        """Create a DataFrame from Arrow table, returning a Result instead of raising."""
        return polars_catch(
            lambda: pl.from_arrow(data, **kwargs), "Failed to create DataFrame from Arrow"
        )

    @staticmethod
    def from_pandas(data: Any, **kwargs: Any) -> Result[pl.DataFrame, PolarsError]:
        """Create a DataFrame from Pandas, returning a Result instead of raising."""
        return polars_catch(
            lambda: pl.from_pandas(data, **kwargs), "Failed to create DataFrame from Pandas"
        )

    # ---- LazyFrame Operations ----

    @staticmethod
    def collect(lf: pl.LazyFrame, **kwargs: Any) -> Result[pl.DataFrame, PolarsError]:
        """Collect a LazyFrame, returning errors as Result."""
        return polars_catch(lambda: lf.collect(**kwargs), "LazyFrame collection failed")

    # ---- Write Operations ----

    @staticmethod
    def write_csv(df: pl.DataFrame, path: str | Path, **kwargs: Any) -> Result[None, PolarsError]:
        """Write DataFrame to CSV, returning a Result instead of raising."""
        return polars_catch(lambda: df.write_csv(path, **kwargs), f"Failed to write CSV '{path}'")

    @staticmethod
    def write_parquet(
        df: pl.DataFrame, path: str | Path, **kwargs: Any
    ) -> Result[None, PolarsError]:
        """Write DataFrame to Parquet, returning a Result instead of raising."""
        return polars_catch(
            lambda: df.write_parquet(path, **kwargs), f"Failed to write Parquet '{path}'"
        )

    @staticmethod
    def write_json(df: pl.DataFrame, path: str | Path, **kwargs: Any) -> Result[None, PolarsError]:
        """Write DataFrame to JSON, returning a Result instead of raising."""
        return polars_catch(
            lambda: df.write_json(path, **kwargs), f"Failed to write JSON '{path}'"
        )

    # ---- DataFrame Operations ----

    @staticmethod
    def select(df: pl.DataFrame, *exprs: Any) -> Result[pl.DataFrame, PolarsError]:
        """Select columns, returning a Result instead of raising."""
        return polars_catch(lambda: df.select(*exprs), "Select operation failed")

    @staticmethod
    def filter(df: pl.DataFrame, *predicates: Any) -> Result[pl.DataFrame, PolarsError]:
        """Filter rows, returning a Result instead of raising."""
        return polars_catch(lambda: df.filter(*predicates), "Filter operation failed")

    @staticmethod
    def with_columns(
        df: pl.DataFrame, *exprs: Any, **named_exprs: Any
    ) -> Result[pl.DataFrame, PolarsError]:
        """Add/replace columns, returning a Result instead of raising."""
        return polars_catch(
            lambda: df.with_columns(*exprs, **named_exprs), "with_columns operation failed"
        )

    @staticmethod
    def join(
        df: pl.DataFrame, other: pl.DataFrame, **kwargs: Any
    ) -> Result[pl.DataFrame, PolarsError]:
        """Join DataFrames, returning a Result instead of raising."""
        return polars_catch(lambda: df.join(other, **kwargs), "Join operation failed")

    @staticmethod
    def group_by(df: pl.DataFrame, *by: Any, **kwargs: Any) -> Result[Any, PolarsError]:
        """Group by, returning a Result instead of raising."""
        cols = [c for c in by if isinstance(c, str)]
        missing = [c for c in cols if c not in df.columns]
        if missing:
            return Err(
                PolarsError(f"group_by operation failed: columns {missing} not found in DataFrame")
            )
        return polars_catch(lambda: df.group_by(*by, **kwargs), "group_by operation failed")


# Convenience aliases
read_csv = PolarsResult.read_csv
read_parquet = PolarsResult.read_parquet
read_json = PolarsResult.read_json
scan_csv = PolarsResult.scan_csv
scan_parquet = PolarsResult.scan_parquet
from_dict = PolarsResult.from_dict
collect = PolarsResult.collect
