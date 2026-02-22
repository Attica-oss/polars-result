[![PyPI version](https://img.shields.io/pypi/v/polars-result)](https://pypi.org/project/polars-result/)
[![Python](https://img.shields.io/pypi/pyversions/polars-result)](https://pypi.org/project/polars-result/)
[![License](https://img.shields.io/github/license/Attica-OSS/polars-result)](https://github.com/Attica-OSS/polars-result/blob/main/LICENSE)

# polars-result

Railway-oriented `Result` type for building robust Polars data pipelines with Rust-inspired error handling.

> **Requires Python 3.12+** for generic type syntax (`Ok[T]`, `Err[E]`).  
> Pattern matching (`match`/`case`) requires Python 3.10+.

## Features

- 🚂 **Railway-oriented programming** — chain operations that short-circuit on the first error
- 🦀 **Rust-inspired Result API** — `Ok`, `Err`, `and_then`, `or_else`, `map`, and more
- 🐻‍❄️ **Polars integration** — safe wrappers for common Polars I/O and DataFrame operations
- 🎯 **Type-safe** — full type inference with Python 3.12+ type parameters
- 🔧 **Decorator support** — convert any function to return `Result` with `@resultify`
- 📦 **Zero dependencies** — only requires Polars

## Installation

```bash
uv add polars-result
```

```bash
pip install polars-result
```

## Quick Start

### Basic Result Usage

```python
from polars_result import Ok, Err

success = Ok(42)
failure = Err("something went wrong")

# Pattern matching (Python 3.10+)
match success:
    case Ok(value):
        print(f"Success: {value}")
    case Err(error):
        print(f"Error: {error}")

# Chain operations — short-circuits on the first Err
result = (
    Ok(10)
    .map(lambda x: x * 2)            # Ok(20)
    .and_then(lambda x: Ok(x + 5))       # Ok(25)
    .map(lambda x: x - 1)            # Ok(24)
)
```

### Safe Polars Operations

```python
from polars_result import read_csv, PolarsResult
import polars as pl

# Each operation returns Result[T, PolarsError] — never raises
pipeline = (
    read_csv("input.csv")
    .and_then(lambda df: PolarsResult.filter(df, pl.col("age") > 18))
    .and_then(lambda df: PolarsResult.select(df, "name", "age"))
    .and_then(lambda df: PolarsResult.write_parquet(df, "output.parquet"))
)

match pipeline:
    case Ok(_):
        print("Done")
    case Err(e):
        print(f"Failed: {e}")
```

### Decorator for Existing Functions

```python
from polars_result import resultify
import polars as pl

@resultify
def load_and_clean(path: str) -> pl.DataFrame:
    """Now returns Result[pl.DataFrame, Exception] instead of raising."""
    df = pl.read_csv(path)
    return df.filter(pl.col("age") > 0)

# Catch only specific exceptions — others propagate normally
@resultify(catch=FileNotFoundError)
def load_file(path: str) -> pl.DataFrame:
    return pl.read_parquet(path)
```

### Generic Exception Handling

```python
from polars_result import catch

result = catch(lambda: int("42"))           # Ok(42)
error  = catch(lambda: int("bad"))          # Err(ValueError(...))

# Catch a specific type — other exceptions still propagate
result = catch(lambda: int("bad"), ValueError)
```

---

## API Reference

### Result Methods

Both `Ok[T]` and `Err[E]` implement the full interface below. Methods that operate on the
"other" variant are no-ops that pass `self` through unchanged.

**Checking state**

| Method | Description |
|---|---|
| `is_ok() → bool` | `True` if `Ok` |
| `is_err() → bool` | `True` if `Err` |
| `is_ok_and(f: T → bool) → bool` | `True` if `Ok` and value satisfies `f` |
| `is_err_and(f: E → bool) → bool` | `True` if `Err` and error satisfies `f` |

**Extracting values**

| Method | On `Ok` | On `Err` |
|---|---|---|
| `unwrap()` | returns value | raises `ValueError` |
| `unwrap_err()` | raises `ValueError` | returns error |
| `unwrap_or(default)` | returns value | returns `default` |
| `unwrap_or_else(f)` | returns value | returns `f(error)` |
| `expect(msg)` | returns value | raises with `msg` |
| `expect_err(msg)` | raises with `msg` | returns error |
| `into_ok()` | returns value | raises `TypeError` |
| `into_err()` | raises `TypeError` | returns error |

> Use `unwrap` and `expect` in tests or where an `Err` is genuinely impossible.
> Prefer `unwrap_or` / `unwrap_or_else` in production code.
> `into_ok` / `into_err` signal a static contract: "I know this cannot be the other variant."

**Transforming**

| Method | Activates on | Description |
|---|---|---|
| `map(f: T → U)` | `Ok` | wraps `f(value)` in `Ok`; passes `Err` through |
| `map_err(f: E → F)` | `Err` | wraps `f(error)` in `Err`; passes `Ok` through |
| `map_or(default, f)` | both | `f(value)` if `Ok`, else `default` — returns plain value |
| `map_or_else(default_f, f)` | both | `f(value)` if `Ok`, else `default_f(error)` — returns plain value |
| `and_then(f: T → Result)` | `Ok` | calls `f(value)`; passes `Err` through.  |
| `or_else(f: E → Result)` | `Err` | calls `f(error)`; passes `Ok` through |
| `flatten()` | `Ok(Result)` | collapses `Ok(Ok(v))` → `Ok(v)`, `Ok(Err(e))` → `Err(e)` |

> **`map` vs `and_then`** — if the function you are chaining can fail (returns `Result`), use `and_then`.
> If it is a plain transform that cannot fail, use `map`.

**Side-effects**

| Method | Activates on | Description |
|---|---|---|
| `inspect(f: T → None)` | `Ok` | calls `f(value)` for logging; returns `self` unchanged |
| `inspect_err(f: E → None)` | `Err` | calls `f(error)` for logging; returns `self` unchanged |

**Iteration**

`Ok` is iterable and yields its value once. `Err` yields nothing. This lets you filter a
list of results without explicit `is_ok()` checks:

```python
results = [Ok(12.5), Err("bad"), Ok(33.0), Err("null"), Ok(8.75)]

ok_values = [v for r in results for v in r]  # [12.5, 33.0, 8.75]
total     = sum(v for r in results for v in r)  # 54.25
```

---

### Polars Operations

All operations return `Result[T, PolarsError]` and never raise.

**Reading**

```python
from polars_result import read_csv, read_parquet, read_json, read_excel
from polars_result import scan_csv, scan_parquet

result      = read_csv("data.csv", separator=";")   # Result[DataFrame, PolarsError]
lazy_result = scan_parquet("data.parquet")           # Result[LazyFrame, PolarsError]
```

**Constructing**

```python
from polars_result import from_dict, from_records

result = from_dict({"a": [1, 2, 3], "b": [4, 5, 6]})
result = from_records([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
```

**Writing**

Write operations return `Result[None, PolarsError]` — the `Ok` value is `None` since the
meaningful outcome is the file on disk, not a return value.

```python
from polars_result import PolarsResult

PolarsResult.write_csv(df, "output.csv")
PolarsResult.write_parquet(df, "output.parquet")
PolarsResult.write_json(df, "output.json")
```

**DataFrame operations**

```python
PolarsResult.select(df, "col1", "col2")
PolarsResult.filter(df, pl.col("age") > 18)
PolarsResult.with_columns(df, tax=pl.col("amount") * 0.08)
PolarsResult.join(df1, df2, on="id")
PolarsResult.group_by(df, "category")   # validates column names eagerly
```

**LazyFrame**

```python
from polars_result import collect

lf     = pl.LazyFrame({"a": [1, 2, 3]})
result = collect(lf)                     # Result[DataFrame, PolarsError]
```

---

## Error Handling Patterns

### Pattern 1: Railway chaining

The most common pattern — each step either advances the pipeline or short-circuits to `Err`.

```python
result = (
    read_csv("input.csv")
    .and_then(validate)
    .and_then(transform)
    .and_then(save)
)
```

### Pattern 2: Early return with match

```python
match read_csv("data.csv"):
    case Err(e):
        return handle_error(e)
    case Ok(df):
        return process(df)
```

### Pattern 3: Error recovery

```python
result = (
    read_csv("cache.csv")
    .or_else(lambda _: read_csv("backup.csv"))
    .or_else(lambda _: Ok(pl.DataFrame()))
)
```

### Pattern 4: Unwrap with default

```python
df    = read_csv("data.csv").unwrap_or(pl.DataFrame())
count = read_csv("data.csv").map(len).unwrap_or(0)
```

### Pattern 5: Logging without breaking the chain

```python
result = (
    read_csv("data.csv")
    .inspect(lambda df: logger.info(f"Loaded {len(df)} rows"))
    .and_then(transform)
    .inspect_err(lambda e: logger.error(f"Pipeline failed: {e}"))
)
```

---

## Real-World Example

```python
import polars as pl
from polars_result import read_csv, PolarsResult, Ok, Err

def process_sales(input_path: str, output_path: str) -> bool:
    result = (
        read_csv(input_path)

        # Validate
        .and_then(lambda df: PolarsResult.filter(
            df,
            pl.col("amount").is_not_null() & (pl.col("amount") > 0)
        ))

        # Enrich
        .and_then(lambda df: PolarsResult.with_columns(
            df,
            tax=pl.col("amount") * 0.08,
            total=pl.col("amount") * 1.08,
        ))

        # Aggregate using PolarsResult.group_by (validates columns eagerly)
        .and_then(lambda df: PolarsResult.group_by(df, "category"))
        .map(lambda gb: gb.agg([
            pl.col("amount").sum().alias("total_sales"),
            pl.col("amount").count().alias("transaction_count"),
        ]))

        # Write
        .and_then(lambda df: PolarsResult.write_parquet(df, output_path))
    )

    match result:
        case Ok(_):
            print(f"✓ Processed {input_path}")
            return True
        case Err(error):
            print(f"✗ Failed: {error}")
            return False


def load_with_fallback(primary: str, backup: str) -> pl.DataFrame:
    return (
        read_csv(primary)
        .inspect(lambda df: print(f"Loaded primary: {len(df)} rows"))
        .or_else(lambda _: read_csv(backup))
        .inspect(lambda df: print(f"Loaded backup: {len(df)} rows"))
        .unwrap_or_else(lambda _: pl.DataFrame())
    )
```

---

## Why Result Types?

**Traditional exception-based approach:**

```python
try:
    df = pl.read_csv("data.csv")
    df = df.filter(pl.col("age") > 18)
    df = df.select("name", "age")
    df.write_parquet("output.parquet")
except Exception as e:
    # Which operation failed?
    # What is the error type?
    # How do we recover gracefully?
    log_error(e)
```

**With Result types:**

```python
result = (
    read_csv("data.csv")                                          # Result[DataFrame, PolarsError]
    .and_then(lambda df: PolarsResult.filter(df, ...))            # short-circuits on Err
    .and_then(lambda df: PolarsResult.select(df, ...))            # type-safe at each step
    .and_then(lambda df: PolarsResult.write_parquet(df, "..."))   # clear error provenance
)

match result:
    case Ok(_):
        print("Success")
    case Err(error):
        print(f"Failed at: {error}")
```

The benefits: errors are values rather than exceptions, every failure is typed and traceable to
the exact step that produced it, and recovery is explicit and composable rather than buried in
`except` clauses.

---

## Development

```bash
uv sync                                                      # install dependencies
uv run pytest                                                # run tests
uv run pytest --cov=src/polars_result --cov-report=html     # with coverage
uv run ruff check src/ tests/                                # lint
uv run ruff format src/ tests/                               # format
uv run pyright                                               # type check
```

## Contributing

Contributions welcome — please open an issue or PR on [GitHub](https://github.com/Attica-OSS/polars-result).

## License

MIT — see [LICENSE](LICENSE) for details.
