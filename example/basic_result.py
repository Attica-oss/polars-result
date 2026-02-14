"""Basic usage of the Result type."""

from polars_result import PolarsResultError, Result

# ── Creating Results ─────────────────────────────────────────────────────────

ok_result = Result.ok(42)
err_result = Result.err(PolarsResultError("Something went wrong"))

print(f"ok_result  → {ok_result}")
print(f"err_result → {err_result}")
print()

# ── Checking state ───────────────────────────────────────────────────────────

print(f"ok_result.is_ok()  = {ok_result.is_ok()}")
print(f"ok_result.is_err() = {ok_result.is_err()}")
print(f"bool(ok_result)    = {bool(ok_result)}")
print()

# ── Extracting values ────────────────────────────────────────────────────────

print(f"ok_result.unwrap()          = {ok_result.unwrap()}")
print(f"err_result.unwrap_or(0)     = {err_result.unwrap_or(0)}")
print(f"err_result.unwrap_err()     = {err_result.unwrap_err()}")
print()

# ── Using in conditionals ───────────────────────────────────────────────────

if ok_result:
    print(f"Success! Got: {ok_result.unwrap()}")

if not err_result:
    print(f"Failed with: {err_result.unwrap_err()}")
