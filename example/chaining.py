"""Railway-oriented chaining with map, bind, and error recovery."""

from polars_result import PolarsResultError, Result, ValidationError

# ── Helper functions ─────────────────────────────────────────────────────────


def parse_int(raw: str) -> Result[int, Exception]:
    """Parse a string to int, returning Result instead of raising."""
    try:
        return Result.ok(int(raw))
    except ValueError as e:
        return Result.err(ValidationError(f"Cannot parse '{raw}': {e}"))


def validate_positive(n: int) -> Result[int, Exception]:
    """Ensure value is positive."""
    if n > 0:
        return Result.ok(n)
    return Result.err(ValidationError(f"Expected positive, got {n}"))


def double(n: int) -> int:
    """Pure function — just doubles."""
    return n * 2


# ── Success pipeline ─────────────────────────────────────────────────────────

print("── Success pipeline ──")
result = (
    parse_int("21")
    .bind(validate_positive)
    .map(double)
)
print(f"  parse '21' → validate → double = {result}")
print(f"  unwrap = {result.unwrap()}")
print()

# ── Short-circuit on parse error ─────────────────────────────────────────────

print("── Parse error (short-circuits) ──")
result = (
    parse_int("abc")
    .bind(validate_positive)
    .map(double)
)
print(f"  parse 'abc' → validate → double = {result}")
print()

# ── Short-circuit on validation error ────────────────────────────────────────

print("── Validation error (short-circuits) ──")
result = (
    parse_int("-5")
    .bind(validate_positive)
    .map(double)
)
print(f"  parse '-5' → validate → double = {result}")
print()

# ── Error recovery with or_else ──────────────────────────────────────────────

print("── Error recovery with or_else ──")
result = (
    parse_int("bad")
    .or_else(lambda _e: Result.ok(0))   # fallback to 0
    .map(double)
)
print(f"  parse 'bad' → recover(0) → double = {result}")
print(f"  unwrap = {result.unwrap()}")
print()

# ── Transform errors with map_err ────────────────────────────────────────────

print("── Error transformation with map_err ──")
result = (
    parse_int("xyz")
    .map_err(lambda e: PolarsResultError(f"Sheet cell error: {e}"))
)
print(f"  parse 'xyz' → map_err = {result}")
print(f"  error type = {type(result.unwrap_err()).__name__}")
