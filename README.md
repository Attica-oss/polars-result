[![PyPI version](https://img.shields.io/pypi/v/polars-result)](https://pypi.org/project/polars-result/)
[![Python](https://img.shields.io/pypi/pyversions/polars-result)](https://pypi.org/project/polars-result/)
[![License](https://img.shields.io/github/license/Attica-OSS/polars-result)](https://github.com/Attica-OSS/polars-result/blob/main/LICENSE)

# polars-result

Railway-oriented `Result` and `Option` types for Python — Rust-inspired error handling
for data pipelines and beyond.

**Requires Python 3.13+.** No additional dependencies.

## Installation

```bash
uv add polars-result
```

```bash
pip install polars-result
```

## Quick Start

```python
from polars_result import Ok, Err, Some, Nothing, resultify
from polars_result.exceptions import ValidationError, PipelineError

# Pattern match on a Result
def parse_age(raw: str) -> Result[int, ValidationError]:
    match raw.strip().isdigit():
        case True:
            age = int(raw)
            return Ok(age) if age > 0 else Err(ValidationError("Age must be positive", field="age", value=age))
        case False:
            return Err(ValidationError("Age must be numeric", field="age", value=raw))

match parse_age("25"):
    case Ok(age):
        print(f"Valid age: {age}")
    case Err(ValidationError() as e):
        print(f"Invalid: {e} (field={e.field}, value={e.value})")

# Chain operations — short-circuits on the first Err
result = (
    Ok(10)
    .map(lambda x: x * 2)          # Ok(20)
    .and_then(lambda x: Ok(x + 5)) # Ok(25)
    .map(lambda x: x - 1)          # Ok(24)
)

# Wrap any function with @resultify
import polars as pl

@resultify
def load(path: str) -> pl.DataFrame:
    return pl.read_csv(path)

match load("data.csv"):
    case Ok(df):
        print(df)
    case Err(e):
        print(f"Failed: {e}")
```

---

## Why Result Types?

**Traditional exception-based approach:**

```python
try:
    df = pl.read_csv("data.csv")
    df = df.filter(pl.col("age") > 18)
    df.write_parquet("output.parquet")
except Exception as e:
    # Which step failed? What type is the error?
    # How do we recover selectively?
    log_error(e)
```

**With Result types:**

```python
result = (
    load("data.csv")                    # Result[DataFrame, PipelineError]
    .and_then(validate)                 # short-circuits on Err
    .and_then(transform)                # type-safe at each step
    .and_then(save)                     # clear error provenance
)

match result:
    case Ok(_):
        print("Done")
    case Err(PipelineError() as e):
        print(f"Failed at step '{e.step}': {e}")
    case Err(ValidationError() as e):
        print(f"Bad value for '{e.field}': {e.value}")
```

Errors are values — typed, traceable to the exact step that produced them, and
recoverable without `except` clauses scattered through your pipeline.

---

## API Reference

### Result

`Ok[T]` and `Err[E]` both implement the full interface below. Methods that operate
on the "other" variant are no-ops that return `self` unchanged.

```python
from polars_result import Ok, Err, Result
```

**Checking state**

| Method | `Ok` | `Err` |
|---|---|---|
| `is_ok() → bool` | `True` | `False` |
| `is_err() → bool` | `False` | `True` |
| `is_ok_and(f: T → bool) → bool` | `f(value)` | `False` |
| `is_err_and(f: E → bool) → bool` | `False` | `f(error)` |

**Extracting values**

| Method | `Ok` | `Err` |
|---|---|---|
| `unwrap()` | returns value | raises `ValueError` |
| `unwrap_err()` | raises `ValueError` | returns error |
| `unwrap_or(default)` | returns value | returns `default` |
| `unwrap_or_else(f)` | returns value | returns `f(error)` |
| `expect(msg)` | returns value | raises with `msg` |
| `expect_err(msg)` | raises with `msg` | returns error |
| `into_ok()` | returns value | raises `TypeError` |
| `into_err()` | raises `TypeError` | returns error |

> Use `unwrap` and `expect` in tests or where `Err` is genuinely impossible.
> Prefer `unwrap_or` / `unwrap_or_else` in production pipelines.
> `into_ok` / `into_err` express a static contract: "I know this cannot be the other variant."

**Transforming**

| Method | Activates on | Description |
|---|---|---|
| `map(f: T → U)` | `Ok` | wraps `f(value)` in `Ok`; passes `Err` through |
| `map_err(f: E → F)` | `Err` | wraps `f(error)` in `Err`; passes `Ok` through |
| `map_or(default, f)` | both | `f(value)` if `Ok`, else `default` — returns plain value |
| `map_or_else(default_f, f)` | both | `f(value)` if `Ok`, else `default_f(error)` — returns plain value |
| `map_or_default(f, default)` | both | same as `map_or` with argument order `f` first |
| `and_then(f: T → Result)` | `Ok` | calls `f(value)`; passes `Err` through |
| `bind(f: T → Result)` | `Ok` | alias for `and_then` — standard monadic name |
| `or_else(f: E → Result)` | `Err` | calls `f(error)`; passes `Ok` through |
| `flatten()` | `Ok(Result)` | collapses one level of nesting |

> **`map` vs `and_then`** — use `and_then` when the next step can fail (returns `Result`).
> Use `map` for plain transforms that cannot fail.

`flatten()` collapses one level of `Result` nesting:

```python
Ok(Ok(42)).flatten()    # → Ok(42)
Ok(Err("e")).flatten()  # → Err("e")
Err("e").flatten()      # → Err("e")  (no-op)
```

**Side-effects**

| Method | Activates on | Description |
|---|---|---|
| `inspect(f: T → None)` | `Ok` | calls `f(value)` for logging; returns `self` |
| `inspect_err(f: E → None)` | `Err` | calls `f(error)` for logging; returns `self` |

**Iteration**

`Ok` yields its value once; `Err` yields nothing — filter a list of results without
explicit `is_ok()` checks:

```python
results = [Ok(12.5), Err("bad"), Ok(33.0), Err("null"), Ok(8.75)]

ok_values = [v for r in results for v in r]   # [12.5, 33.0, 8.75]
total     = sum(v for r in results for v in r) # 54.25
```

---

### Option

`Some[T]` and `Nothing` provide optional value handling. Use `Option` when absence is
expected and normal; use `Result` when absence signals an error.

```python
from polars_result import Some, Nothing, Option
from polars_result.option import _NoneOption
```

> `Nothing` is a singleton instance of `_NoneOption` — `repr(Nothing)` returns
> `"Nothing"`. For boolean checks use `x.is_none()`. For pattern matching, use the
> class pattern `case _NoneOption():` rather than `case Nothing:` — bare names in
> `match` are capture patterns in Python and would silently match anything.

**Checking state**

| Method | `Some` | `Nothing` |
|---|---|---|
| `is_some() → bool` | `True` | `False` |
| `is_none() → bool` | `False` | `True` |
| `is_some_and(f: T → bool) → bool` | `f(value)` | `False` |

**Extracting values**

| Method | `Some` | `Nothing` |
|---|---|---|
| `unwrap()` | returns value | raises `ValueError` |
| `unwrap_or(default)` | returns value | returns `default` |
| `unwrap_or_else(f)` | returns value | returns `f()` |
| `expect(msg)` | returns value | raises with `msg` |

**Transforming**

| Method | Activates on | Description |
|---|---|---|
| `map(f: T → U)` | `Some` | returns `Some(f(value))`; passes `Nothing` through |
| `map_or(default, f)` | both | `f(value)` if `Some`, else `default` |
| `map_or_else(default_f, f)` | both | `f(value)` if `Some`, else `default_f()` |
| `and_then(f: T → Option)` | `Some` | calls `f(value)`; passes `Nothing` through |
| `or_else(f: → Option)` | `Nothing` | calls `f()`; passes `Some` through |
| `filter(predicate: T → bool)` | `Some` | returns `Nothing` if predicate fails |
| `flatten()` | `Some(Option)` | collapses one level of nesting |

**Side-effects**

| Method | Activates on | Description |
|---|---|---|
| `inspect(f: T → None)` | `Some` | calls `f(value)`; returns `self` |

**Converting to Result**

`ok_or` and `ok_or_else` convert an `Option` into a `Result`, bridging into a
`Result` pipeline at the point where absence becomes an error:

```python
Some(42).ok_or("missing")              # Ok(42)
Nothing.ok_or("missing")              # Err("missing")

Some(42).ok_or_else(lambda: "missing") # Ok(42)  — f is never called
Nothing.ok_or_else(lambda: "missing") # Err("missing")  — f called lazily
```

Prefer `ok_or_else` when constructing the error value is expensive.

**Example:**

```python
from polars_result.option import _NoneOption

match lookup(key):
    case Some(v):
        print(f"Found: {v}")
    case _NoneOption():
        print("Not found")

# Chain into a Result pipeline
result = (
    lookup("user_id")
    .map(str.strip)
    .ok_or(ValidationError("user_id is required", field="user_id"))
)
```

---

### Exceptions

All exceptions carry a `cause` that preserves the original exception for debugging.

```python
from polars_result.exceptions import (
    PolarsResultError,
    ValidationError,
    ResultSchemaError,
    PipelineError,
)
```

**Hierarchy**

```
Exception
└── PolarsResultError          # base — message + cause
    ├── ValidationError        # + field, value
    │   └── ResultSchemaError  # + expected, got
    └── PipelineError          # + step
```

**`PolarsResultError`**

```python
PolarsResultError(message: str, *, cause: BaseException | None = None)
```

Base class for all package exceptions. Use `from_polars()` to wrap raw Polars
exceptions with automatic subtype mapping:

```python
from polars.exceptions import ComputeError

err = PolarsResultError.from_polars(ComputeError("overflow"), "compute totals")
# → PipelineError('compute totals: overflow', cause=ComputeError('overflow'))
```

| Polars exception | Maps to |
|---|---|
| `SchemaError`, `ColumnNotFoundError`, `DuplicateError` | `ResultSchemaError` |
| `ComputeError`, `InvalidOperationError`, `ShapeError`, `NoDataError` | `PipelineError` |
| anything else | `PolarsResultError` |

**`ValidationError`**

```python
ValidationError(message: str, *, field: str | None = None, value: object = None, cause: BaseException | None = None)
```

```python
err = ValidationError("Expected positive", field="amount", value=-3)

case Err(ValidationError() as e):
    print(e.field)  # "amount"
    print(e.value)  # -3
```

**`ResultSchemaError`**

```python
ResultSchemaError(message: str, *, expected: dict | None = None, got: dict | None = None, field: str | None = None, cause: BaseException | None = None)
```

```python
err = ResultSchemaError(
    "Schema mismatch",
    expected={"amount": "Float64"},
    got={"amount": "Int32"},
)
```

**`PipelineError`**

```python
PipelineError(message: str, *, step: str | None = None, cause: BaseException | None = None)
```

```python
err = PipelineError("Transform failed", step="compute_totals", cause=original)

case Err(PipelineError() as e):
    print(e.step)   # "compute_totals"
    print(e.cause)  # original exception
```

---

### `@resultify`

Wraps any function to return `Result` instead of raising.

```python
from polars_result import resultify
```

**Bare usage** — catches all exceptions, wraps as `PipelineError`:

```python
@resultify
def load(path: str) -> pl.DataFrame:
    return pl.read_csv(path)

load("data.csv")   # Result[pl.DataFrame, PipelineError]
```

**With parameters** — control what is caught and how errors are wrapped:

```python
@resultify(catch_types=FileNotFoundError, error_type=PipelineError)
def load(path: str) -> pl.DataFrame:
    return pl.read_csv(path)
```

`catch_types` accepts a single type or a tuple of types. Exceptions not matched
propagate normally — never silently swallowed.

The original exception is always preserved on `error.cause`:

```python
match load("missing.csv"):
    case Err(PipelineError() as e):
        print(type(e.cause))  # FileNotFoundError
```

Functions that already return a `Result` are passed through without double-wrapping.
Functions returning `None` produce `Ok(None)`.

---

## Error Handling Patterns

### Pattern 1: Railway chaining

```python
result = (
    load("input.csv")
    .and_then(validate)
    .and_then(transform)
    .and_then(save)
)
```

### Pattern 2: Early return with match

```python
match load("data.csv"):
    case Err(e):
        return handle_error(e)
    case Ok(df):
        return process(df)
```

### Pattern 3: Error recovery

```python
result = (
    load("cache.csv")
    .or_else(lambda _: load("backup.csv"))
    .or_else(lambda _: Ok(pl.DataFrame()))
)
```

### Pattern 4: Unwrap with default

```python
df    = load("data.csv").unwrap_or(pl.DataFrame())
count = load("data.csv").map(len).unwrap_or(0)
```

### Pattern 5: Logging without breaking the chain

```python
result = (
    load("data.csv")
    .inspect(lambda df: logger.info(f"Loaded {len(df)} rows"))
    .and_then(transform)
    .inspect_err(lambda e: logger.error(f"Failed: {e}"))
)
```

### Pattern 6: Collecting results from a list

```python
results = [parse(row) for row in rows]

# Extract only the Ok values — Err yields nothing, Ok yields once
values = [v for r in results for v in r]

# Check if all succeeded
if all(r.is_ok() for r in results):
    ...

# Find the first error
first_err = next((r for r in results if r.is_err()), None)
```

### Pattern 7: Option to Result in a pipeline

```python
from polars_result.option import _NoneOption
from polars_result.exceptions import ValidationError

result = (
    find_column(lf, "invoice_date")      # Option[pl.Expr]
    .ok_or(ValidationError(             # Result[pl.Expr, ValidationError]
        "invoice_date is required",
        field="invoice_date",
    ))
    .map(lambda expr: lf.with_columns(
        expr.cast(pl.Date).alias("invoice_date")
    ))
)
```

---

## Development

```bash
uv sync                                                      # install dependencies
uv run pytest                                                # run tests
uv run pytest --cov=src/polars_result --cov-report=html     # with coverage
uv run ruff check src/ tests/                                # lint
uv run ruff format src/ tests/                               # format
uv run ty check src/                                         # type check (Astral ty)
```

> [`ty`](https://github.com/astral-sh/ty) is Astral's type checker — the team behind `ruff` and `uv`.
> `mypy` and `pyright` are also compatible.

## Contributing

Contributions welcome — please open an issue or PR on [GitHub](https://github.com/Attica-OSS/polars-result).

## License

MIT — see [LICENSE](LICENSE) for details.
