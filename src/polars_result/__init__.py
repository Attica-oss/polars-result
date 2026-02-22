"""polars-result: Railway-oriented Result type for Polars data pipelines."""

from .decorators import resultify
from .exceptions import (
    PipelineError,
    PolarsResultError,
    ValidationError,
)
from .formatting import format_dataframe, print_dataframe
from .option import Nothing, Option, Some
from .polars_result import (
    PolarsResult,
    catch,
    collect,
    from_dict,
    polars_catch,
    read_csv,
    read_json,
    read_parquet,
    scan_csv,
    scan_parquet,
)
from .result import Err, Ok, Result

__all__ = [
    "Err",
    "Nothing",
    "Ok",
    "Option",
    "PipelineError",
    "PolarsResult",
    "PolarsResultError",
    "Result",
    "Some",
    "ValidationError",
    "catch",
    "collect",
    "format_dataframe",
    "from_dict",
    "polars_catch",
    "print_dataframe",
    "read_csv",
    "read_json",
    "read_parquet",
    "resultify",
    "scan_csv",
    "scan_parquet",
]
