"""Display and formatting utilities for Polars DataFrames."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

# ── Defaults ─────────────────────────────────────────────────────────────────

DATAFRAME_LEN_THRESHOLD: int = 50
MIN_LEN_DATAFRAME: int = 30
STRING_LENGTH: int = 500
CHAR_WIDTH: int = 1000


def format_dataframe(
    df: pl.DataFrame,
    *,
    max_rows: int = DATAFRAME_LEN_THRESHOLD,
    truncated_rows: int = MIN_LEN_DATAFRAME,
    string_length: int = STRING_LENGTH,
    char_width: int = CHAR_WIDTH,
) -> str:
    """Return a nicely formatted string representation of a DataFrame.

    Parameters
    ----------
    df : pl.DataFrame
        The DataFrame to format.
    max_rows : int
        If the DataFrame exceeds this many rows, display is truncated.
    truncated_rows : int
        Number of rows shown when truncation kicks in.
    string_length : int
        Maximum display length for string columns.
    char_width : int
        Table width in characters.
    """
    import polars as pl

    display_rows = truncated_rows if df.height > max_rows else df.height

    with pl.Config(
        fmt_str_lengths=string_length,
        tbl_width_chars=char_width,
        tbl_cols=df.width,
        tbl_rows=display_rows,
    ):
        formatted = str(df)

    if df.height > max_rows:
        formatted += f"\n… truncated ({df.height} total rows, showing {display_rows})"

    return formatted


def print_dataframe(
    df: pl.DataFrame,
    *,
    max_rows: int = DATAFRAME_LEN_THRESHOLD,
    truncated_rows: int = MIN_LEN_DATAFRAME,
    string_length: int = STRING_LENGTH,
    char_width: int = CHAR_WIDTH,
) -> None:
    """Print a DataFrame with formatted output, handling large DataFrames.

    Parameters
    ----------
    df : pl.DataFrame
        The DataFrame to print.
    max_rows : int
        If the DataFrame exceeds this many rows, display is truncated.
    truncated_rows : int
        Number of rows shown when truncation kicks in.
    string_length : int
        Maximum display length for string columns.
    char_width : int
        Table width in characters.

    Examples
    --------
    >>> import polars as pl
    >>> df = pl.DataFrame({"col1": range(5), "col2": ["example"] * 5})
    >>> print_dataframe(df)  # doctest: +SKIP
    """
    print(
        format_dataframe(
            df,
            max_rows=max_rows,
            truncated_rows=truncated_rows,
            string_length=string_length,
            char_width=char_width,
        )
    )


def format_result_data(data: object) -> str:
    """Format data for Result.__repr__, with special handling for DataFrames.

    Returns a compact summary for DataFrames instead of dumping the full table.
    """
    try:
        import polars as pl

        if isinstance(data, pl.DataFrame):
            cols = ", ".join(data.columns[:5])
            if data.width > 5:
                cols += f", … +{data.width - 5}"
            return f"DataFrame({data.height}x{data.width} [{cols}])"

        if isinstance(data, pl.LazyFrame):
            schema = data.collect_schema()
            cols = ", ".join(list(schema.names())[:5])
            if len(schema) > 5:
                cols += f", … +{len(schema) - 5}"
            return f"LazyFrame([{cols}])"
    except ImportError:
        pass

    return repr(data)
