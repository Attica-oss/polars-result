"""Custom exceptions for polars-result."""


class PolarsResultError(Exception):
    """Base exception for polars-result operations."""


# class GoogleSheetError(PolarsResultError):
#     """Error raised during Google Sheets operations."""


class ValidationError(PolarsResultError):
    """Error raised during data validation."""


class PipelineError(PolarsResultError):
    """Error raised during pipeline execution."""
