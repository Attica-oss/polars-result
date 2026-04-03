"""polars-result: Railway-oriented Result type for Polars data pipelines."""

from .decorators import resultify
from .exceptions import PipelineError, PolarsResultError, ResultSchemaError, ValidationError
from .option import Nothing, Option, Some
from .result import Err, Ok, Result

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
    "resultify",
]
