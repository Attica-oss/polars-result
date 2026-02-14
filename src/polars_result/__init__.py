"""polars-result: Railway-oriented Result type for Polars data pipelines."""

from .exceptions import (
    PipelineError,
    PolarsResultError,
    ValidationError,
)
from .formatting import format_dataframe, print_dataframe
from .result import Result

__all__ = [
    "PipelineError",
    "PolarsResultError",
    "Result",
    "ValidationError",
    "format_dataframe",
    "print_dataframe",
]
