"""polars-result: Railway-oriented Result type for Polars data pipelines."""

from .catch import catch
from .decorators import resultify
from .exceptions import PipelineError, PolarsResultError, ResultSchemaError, ValidationError
from .option import Nothing, Option, Some, is_none, is_some
from .result import Err, Ok, Result, is_err, is_ok

__all__ = [
    "Err",
    "Nothing",
    "Ok",
    "Option",
    "PipelineError",
    "PolarsResultError",
    "Result",
    "ResultSchemaError",
    "Some",
    "ValidationError",
    "catch",
    "is_err",
    "is_none",
    "is_ok",
    "is_some",
    "resultify",
]
