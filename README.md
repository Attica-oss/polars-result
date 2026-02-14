[![PyPI version](https://img.shields.io/pypi/v/polars-result)](https://pypi.org/project/polars-result/)
[![Python](https://img.shields.io/pypi/pyversions/polars-result)](https://pypi.org/project/polars-result/)
[![License](https://img.shields.io/github/license/Attica-OSS/polars-result)](https://github.com/Attica-OSS/polars-result/blob/main/LICENSE)

# polars-result

Railway-oriented `Result` type for building robust Polars data pipelines.

## Installation

```bash
uv add polars-result
```

## Quick Start

```python
from polars_result import Result, PolarsResultError

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
# Install dependencies
uv sync

# Run tests
uv run pytest

# Lint & format
uv run ruff check src/ tests/
uv run ruff format src/ tests/

# Type check
uv run ty check src/
```
