# polars-result

Railway-oriented `Result` type for building robust Polars data pipelines.

## Installation

```bash
uv add polars-result
```

## Quick Start

```python
from polars_result import Result, GoogleSheetError

# Successful pipeline
result = (
    Result.ok(raw_dataframe)
    .map(clean_columns)
    .bind(validate_schema)
    .map(compute_totals)
)

if result.is_ok():
    df = result.unwrap()
else:
    print(f"Pipeline failed: {result.unwrap_err()}")
```

## Development

```bash
# Install dev dependencies
uv sync --extra dev

# Run tests
uv run pytest

# Lint & format
uv run ruff check src/ tests/
uv run ruff format src/ tests/

# Type check
uv run ty check src/
```
