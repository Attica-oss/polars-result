"""
Marimo showcase for the Result[T, E] module.
Run with: marimo edit result_showcase.py
"""

import marimo

__generated_with = "0.20.1"
app = marimo.App(width="medium", app_title="Result[T, E] — Method Showcase")


@app.cell
def _():
    from collections.abc import Callable, Iterator
    from dataclasses import dataclass
    from typing import TypeVar

    T = TypeVar("T")
    E = TypeVar("E")
    U = TypeVar("U")
    F = TypeVar("F")

    class Infallible:
        def __init__(self) -> None:
            raise TypeError("Infallible cannot be instantiated")

    @dataclass
    class Ok[T]:
        value: T

        def is_ok(self) -> bool:
            return True

        def is_err(self) -> bool:
            return False

        def unwrap(self) -> T:
            return self.value

        def unwrap_err(self):
            raise ValueError(f"Called unwrap_err on Ok: {self.value}")

        def unwrap_or(self, default):
            return self.value

        def unwrap_or_else(self, op):
            return self.value

        def expect(self, msg):
            return self.value

        def expect_err(self, msg):
            raise ValueError(f"{msg}: {self.value}")

        def map(self, fn):
            return Ok(fn(self.value))

        def map_err(self, op):
            return Ok(self.value)

        def map_or(self, default, f):
            return f(self.value)

        def map_or_else(self, default, f):
            return f(self.value)

        def map_or_default(self, f, default):
            return f(self.value)

        def and_then(self, fn):
            return fn(self.value)

        def bind(self, fn):
            return self.and_then(fn)

        def or_else(self, op):
            return Ok(self.value)

        def is_ok_and(self, f):
            return f(self.value)

        def is_err_and(self, f):
            return False

        def inspect(self, f):
            f(self.value)
            return self

        def inspect_err(self, f):
            return self

        def iter(self):
            yield self.value

        def __iter__(self):
            return self.iter()

        def flatten(self):
            if isinstance(self.value, (Ok, Err)):
                return self.value
            raise TypeError(f"flatten called on non-nested Result: {self.value}")

        def into_ok(self):
            return self.value

        def into_err(self):
            raise TypeError("Called into_err on Ok variant")

        def __repr__(self):
            return f"Ok({self.value!r})"

    @dataclass
    class Err[E]:
        error: E

        def is_ok(self) -> bool:
            return False

        def is_err(self) -> bool:
            return True

        def unwrap(self):
            raise ValueError(f"Called unwrap on Err: {self.error}")

        def unwrap_err(self):
            return self.error

        def unwrap_or(self, default):
            return default

        def unwrap_or_else(self, op):
            return op(self.error)

        def expect(self, msg):
            raise ValueError(f"{msg}: {self.error}")

        def expect_err(self, msg):
            return self.error

        def map(self, fn):
            return self

        def map_err(self, op):
            return Err(op(self.error))

        def map_or(self, default, f):
            return default

        def map_or_else(self, default, f):
            return default(self.error)

        def map_or_default(self, f, default):
            return default

        def and_then(self, fn):
            return self

        def bind(self, fn):
            return self.and_then(fn)

        def or_else(self, op):
            return op(self.error)

        def is_ok_and(self, f):
            return False

        def is_err_and(self, f):
            return f(self.error)

        def inspect(self, f):
            return self

        def inspect_err(self, f):
            f(self.error)
            return self

        def iter(self):
            return iter(())

        def __iter__(self):
            return self.iter()

        def flatten(self):
            return self

        def into_ok(self):
            raise TypeError(f"Called into_ok on Err: {self.error}")

        def into_err(self):
            return self.error

        def __repr__(self):
            return f"Err({self.error!r})"

    Result = Ok[T] | Err[E]
    return Err, Ok


@app.cell
def _(mo):
    mo.md("""
    # `Result[T, E]` — Method Showcase

    A complete interactive reference for railway-oriented programming with the `Result` type.
    Each section demonstrates a method group with runnable examples drawn from maritime port operations.

    > **Two tracks, one pipeline.** `Ok[T]` carries a success value forward.
    > `Err[E]` carries an error forward, bypassing every step that doesn't explicitly handle it.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 1 · Construction

    Create a `Result` by wrapping a value in `Ok` or an error in `Err`.
    """)
    return


@app.cell
def _():
    return


@app.cell
def _(Err, Ok, mo):
    ok_val = Ok(42.5)
    err_val = Err("Value not found")

    mo.md(f"""
    ```python
    Ok(42.5)                  # → {ok_val}
    Err("Value not found")   # → {err_val}
    ```

    Both are plain dataclasses — no magic, no exceptions during construction.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 2 · Checking State

    `is_ok()`, `is_err()`, `is_ok_and()`, `is_err_and()`
    """)
    return


@app.cell
def _(Err, Ok, mo):
    result_ok = Ok(150.0)
    result_err = Err("invalid container")

    checks = {
        "result_ok.is_ok()": result_ok.is_ok(),
        "result_ok.is_err()": result_ok.is_err(),
        "result_err.is_ok()": result_err.is_ok(),
        "result_err.is_err()": result_err.is_err(),
    }

    check_rows = "\n".join(f"| `{k}` | `{v}` |" for k, v in checks.items())
    mo.md(f"""
    ```python
    result_ok  = Ok(150.0)
    result_err = Err("invalid container")
    ```

    | Expression | Result |
    |---|---|
    {check_rows}
    """)
    return


@app.cell
def _(Err, Ok, mo):
    r1 = Ok(150.0)
    r2 = Err("Record missing")

    a = r1.is_ok_and(lambda t: t > 100)  # True  — Ok AND > 100
    b = r1.is_ok_and(lambda t: t > 200)  # False — Ok but NOT > 200
    c = r2.is_err_and(lambda e: "missing" in e)  # True  — Err AND keyword present

    mo.md(f"""
    ### Predicate checks: `is_ok_and` / `is_err_and`

    ```python
    Ok(150.0).is_ok_and(lambda t: t > 100)          # → {a}
    Ok(150.0).is_ok_and(lambda t: t > 200)          # → {b}
    Err("Record missing").is_err_and(
        lambda e: "mssing" in e)                     # → {c}
    ```

    Useful for guard conditions without unwrapping first.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 3 · Unwrapping

    Extract the inner value — safe and unsafe variants.
    """)
    return


@app.cell
def _(Err, Ok, mo):
    # unwrap
    v1 = Ok(88.5).unwrap()  # 88.5

    # unwrap_or — safe fallback
    v2 = Err("no data").unwrap_or(0.0)  # 0.0

    # unwrap_or_else — computed fallback
    v3 = Err("no data").unwrap_or_else(lambda e: f"[fallback: {e}]")

    # expect — custom panic message
    v4 = Ok("MyValue").expect("value must be present")

    mo.md(f"""
    ```python
    Ok(88.5).unwrap()                             # → {v1!r}
    Err("no data").unwrap_or(0.0)                 # → {v2!r}
    Err("no data").unwrap_or_else(
        lambda e: f"[fallback: {{e}}]")            # → {v3!r}
    Ok("MyValue").expect(
        "value must be present")              # → {v4!r}
    ```

    | Method | On `Ok` | On `Err` |
    |---|---|---|
    | `unwrap()` | returns value | **raises** `ValueError` |
    | `unwrap_err()` | **raises** `ValueError` | returns error |
    | `unwrap_or(default)` | returns value | returns `default` |
    | `unwrap_or_else(fn)` | returns value | calls `fn(error)` |
    | `expect(msg)` | returns value | **raises** with `msg` |
    | `expect_err(msg)` | **raises** with `msg` | returns error |

    > Prefer `unwrap_or` / `unwrap_or_else` in production; reserve `unwrap` / `expect` for tests or truly impossible error paths.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 4 · Mapping

    Transform the inner value with a plain (non-Result-returning) function.
    """)
    return


@app.cell
def _(Err, Ok, mo):
    # map: transform Ok value, pass Err through
    r_map_ok = Ok(12.5).map(lambda t: round(t * 1.1, 2))  # apply 10% surcharge
    r_map_err = Err("missing data").map(lambda t: t * 1.1)  # Err passes through unchanged

    mo.md(f"""
    ### `map(fn: T → U) → Result[U, E]`

    ```python
    Ok(12.5).map(lambda t: round(t * 1.1, 2))   # apply 10% surcharge  → {r_map_ok}
    Err("missing data").map(lambda t: t * 1.1) # Err unchanged        → {r_map_err}
    ```

    `map` **never touches** `Err` — it slides through untouched.
    """)
    return


@app.cell
def _(Err, Ok, mo):
    # map_err: transform Err, leave Ok alone
    r_me_ok = Ok(99.0).map_err(str.upper)
    r_me_err = Err("invalid service code").map_err(str.upper)

    # map_or: Ok → apply fn, Err → use default
    r_mo_ok = Ok(45.0).map_or(0.0, lambda t: t * 2)
    r_mo_err = Err("null").map_or(0.0, lambda t: t * 2)

    # map_or_else: Err path gets to inspect the error
    r_moe = Err("zero weight").map_or_else(
        lambda e: f"default due to: {e}",
        lambda t: t * 2,
    )

    mo.md(f"""
    ### `map_err`, `map_or`, `map_or_else`

    ```python
    # map_err — mirror of map, but for the Err side
    Ok(99.0).map_err(str.upper)                   # → {r_me_ok}  (Ok unchanged)
    Err("invalid service code").map_err(str.upper) # → {r_me_err}

    # map_or — collapse to a plain value with a fallback
    Ok(45.0).map_or(0.0, lambda t: t * 2)         # → {r_mo_ok}
    Err("null").map_or(0.0, lambda t: t * 2)      # → {r_mo_err}

    # map_or_else — fallback is computed from the error
    Err("zero weight").map_or_else(
        lambda e: f"default due to: {{e}}",
        lambda t: t * 2,
    )  # → {r_moe!r}
    ```

    | Method | Ok path | Err path | Returns |
    |---|---|---|---|
    | `map(f)` | `Ok(f(v))` | pass-through | `Result` |
    | `map_err(f)` | pass-through | `Err(f(e))` | `Result` |
    | `map_or(d, f)` | `f(v)` | `d` | plain value |
    | `map_or_else(df, f)` | `f(v)` | `df(e)` | plain value |
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 5 · Chaining — `bind` / `and_then`

    Chain operations that themselves return a `Result`.
    """)
    return


@app.cell
def _(Err, Ok, mo):
    def parse_currency(raw: str):
        try:
            t = float(raw)
            return Ok(t) if t > 0 else Err("currency must be positive")
        except ValueError:
            return Err(f"cannot parse '{raw}' as float")

    def apply_rate(value: float):
        RATE = 18.50
        return Ok(round(value * RATE, 2))

    def check_invoice_limit(amount: float):
        LIMIT = 5_000.0
        return Ok(amount) if amount <= LIMIT else Err(f"invoice ${amount} exceeds limit ${LIMIT}")

    good = Ok("42.5").and_then(parse_currency).and_then(apply_rate).and_then(check_invoice_limit)
    bad_1 = Ok("-5").and_then(parse_currency).and_then(apply_rate).and_then(check_invoice_limit)
    bad_2 = Ok("abc").and_then(parse_currency).and_then(apply_rate).and_then(check_invoice_limit)
    big = Ok("300").and_then(parse_currency).and_then(apply_rate).and_then(check_invoice_limit)

    mo.md(f"""
    ```python
    def parse_currency(raw):  ... # str → Result[float, str]
    def apply_rate(value): ... # float → Result[float, str]   (@$18.50/t)
    def check_invoice_limit(amount): ... # float → Result[float, str]

    Ok("42.5").and_then(parse_tonnes).and_then(apply_rate).and_then(check_invoice_limit)
    # → {good}

    Ok("-5").and_then(parse_tonnes).and_then(apply_rate).and_then(check_invoice_limit)
    # → {bad_1}  ← short-circuits here, apply_rate never called

    Ok("abc").and_then(parse_tonnes).and_then(apply_rate).and_then(check_invoice_limit)
    # → {bad_2}  ← short-circuits at parse step

    Ok("300").and_then(parse_tonnes).and_then(apply_rate).and_then(check_invoice_limit)
    # → {big}
    ```

    `and_then` vs `map` in one rule: if the function you're chaining **can fail** (returns `Result`), use `and_then`. If it's a plain transform that **can't fail**, use `map`.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 6 · Error Recovery — `or_else`

    Handle an `Err` and try to get back onto the `Ok` track.
    """)
    return


@app.cell
def _(Err, Ok, mo):
    KNOWN_SERVICES = {"transportation", "booking", "baggage_handling"}

    def strict_lookup(service: str):
        return Ok(service) if service in KNOWN_SERVICES else Err(f"unknown: '{service}'")

    def fallback_to_misc(err: str):
        return Ok("miscellaneous")  # recover by substituting a default

    def log_and_fail(err: str):
        return Err(f"[logged] {err}")  # stay on the Err track but enrich it

    r_recovered = strict_lookup("booking").or_else(fallback_to_misc)
    r_needed = strict_lookup("transportation").or_else(fallback_to_misc)
    r_enriched = strict_lookup("transportation").or_else(log_and_fail)

    mo.md(f"""
    ```python
    strict_lookup("booking").or_else(fallback_to_misc)  # → {r_recovered}  (Ok, no recovery needed)
    strict_lookup("transportation").or_else(fallback_to_misc)     # → {r_needed}      (recovered)
    strict_lookup("transportation").or_else(log_and_fail)         # → {r_enriched!r}  (stayed Err, enriched)
    ```

    `or_else` is the **mirror of `bind`**: it operates on the **Err** side. Its callback receives the error value and must return a `Result` — you can either recover to `Ok` or return a new (possibly transformed) `Err`.

    | | `bind` / `and_then` | `or_else` |
    |---|---|---|
    | Activates on | `Ok` | `Err` |
    | Skips | `Err` | `Ok` |
    | Callback returns | `Result` | `Result` |
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 7 · Side-Effects — `inspect` / `inspect_err`

    Tap into the pipeline for logging without altering the value.
    """)
    return


@app.cell
def _(Err, Ok, mo):
    log = []

    result = (
        Ok({"item": "Books", "value": 22.0})
        .inspect(lambda d: log.append(f"✓ loaded: {d['item']}"))
        .map(lambda d: d["value"])
        .inspect(lambda t: log.append(f"✓ value extracted: {t}"))
        .and_then(lambda t: Ok(t * 18.5) if t > 0 else Err("Free"))
        .inspect(lambda a: log.append(f"✓ invoice amount: ${a:.2f}"))
        .inspect_err(lambda e: log.append(f"✗ error: {e}"))
    )

    log_display = "\n".join(f"  {line}" for line in log)

    mo.md(f"""
    ```python
    log = []

    result = (
        Ok({{"item": "Books", "value": 22.0}})
        .inspect(lambda d: log.append(f"✓ loaded: {{d['item']}}"))
        .map(lambda d: d["value"])
        .inspect(lambda t: log.append(f"✓ value extracted: {{t}}"))
        .and_then(lambda t: Ok(t * 18.5) if t > 0 else Err("Free"))
        .inspect(lambda a: log.append(f"✓ invoice amount: ${{a:.2f}}"))
        .inspect_err(lambda e: log.append(f"✗ error: {{e}}"))
    )
    ```

    **Log output:**
    ```
    {log_display}
    ```

    **Final result:** `{result}`

    Both `inspect` and `inspect_err` **always return `self`** unchanged — they are purely for side-effects like logging, metrics, or debugging. They never alter the track you're on.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 8 · `flatten`

    Collapse a `Result[Result[T, E], E]` into `Result[T, E]`.
    """)
    return


@app.cell
def _(Err, Ok, mo):
    nested_ok = Ok(Ok(42.5))
    nested_err = Ok(Err("inner error"))
    outer_err = Err("outer error")

    f1 = nested_ok.flatten()
    f2 = nested_err.flatten()
    f3 = outer_err.flatten()

    mo.md(f"""
    ```python
    Ok(Ok(42.5)).flatten()        # → {f1}
    Ok(Err("inner error")).flatten() # → {f2}
    Err("outer error").flatten()  # → {f3}
    ```

    `flatten` comes up naturally when a function that returns `Result` is used inside `map` (instead of `bind`). The result is `Ok(Result(...))` — one level too deep. You can either switch to `bind`, or call `.flatten()` afterward:

    ```python
    # These are equivalent:
    Ok("42.5").bind(parse_tonnes)
    Ok("42.5").map(parse_tonnes).flatten()
    ```
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 9 · `iter` / `__iter__`

    Treat a `Result` as a zero-or-one element sequence.
    """)
    return


@app.cell
def _(Err, Ok, mo):
    iter_results = [Ok(12.5), Err("bad"), Ok(33.0), Err("null"), Ok(8.75)]

    # Flatten to only the Ok values using iteration
    ok_values = [v for r in iter_results for v in r]

    # Same with list-of-results pattern
    iter_total = sum(v for r in iter_results for v in r)

    mo.md(f"""
    ```python
    results = [Ok(12.5), Err("bad"), Ok(33.0), Err("null"), Ok(8.75)]

    # Comprehension — iterating Ok yields the value, Err yields nothing
    ok_values = [v for r in results for v in r]
    # → {ok_values}

    total = sum(v for r in results for v in r)
    # → {iter_total}
    ```

    `Ok` is iterable and yields its value **once**. `Err` yields **nothing**. This makes it trivial to filter a list of `Result` values down to just the successes without any explicit `if r.is_ok()` guard.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 10 · `into_ok` / `into_err`

    Express that the other variant is **statically impossible**.
    """)
    return


@app.cell
def _(Err, Ok, mo):
    # into_ok: safe only when you KNOW it's Ok (use at the boundary where Ok is guaranteed)
    v_ok = Ok("Books").into_ok()

    # into_err: safe only when you KNOW it's Err
    v_err = Err("permanent failure").into_err()

    mo.md(f"""
    ```python
    Ok("Books").into_ok()        # → {v_ok!r}
    Err("permanent failure").into_err() # → {v_err!r}
    ```

    These are **intentionally asymmetric** — calling `into_ok()` on an `Err` raises `TypeError` (not `ValueError`), signalling a programming contract violation rather than a data error.

    > Think of them as the "I promise this can't be the other variant" escape hatch, mirroring Rust's `Result::into_ok` which is only callable when `E = Infallible`. Use them at system boundaries where you've already filtered/validated and want clean extraction without `.unwrap()`.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 11 · Full Pipeline — Putting it all together
    """)
    return


@app.cell
def _(Err, Ok, mo):
    VALID_SERVICES = {"stevedoring", "cold_storage", "container_handling"}
    RATES = {"stevedoring": 18.50, "cold_storage": 24.00, "container_handling": 12.75}
    MAX_INVOICE = 10_000.0

    def validate_container(container: str):
        import re

        return (
            Ok(container)
            if re.match(r"^[A-Z]{4}\d{7}$", container)
            else Err(f"invalid container format: '{container}'")
        )

    def validate_service(service: str):
        return Ok(service) if service in VALID_SERVICES else Err(f"unknown service: '{service}'")

    def calculate_invoice(data: dict):
        service = data["service"]
        tonnes = data["tonnes"]
        if tonnes <= 0:
            return Err("tonnes must be positive")
        amount = round(tonnes * RATES[service], 2)
        return Ok(
            {
                "container": data["container"],
                "service": service,
                "tonnes": tonnes,
                "amount": amount,
            }
        )

    def check_limit(invoice: dict):
        return (
            Ok(invoice)
            if invoice["amount"] <= MAX_INVOICE
            else Err(f"invoice ${invoice['amount']:,.2f} exceeds limit ${MAX_INVOICE:,.2f}")
        )

    def process_row(container: str, service: str, tonnes: float):
        return (
            validate_container(container)
            .inspect(lambda _: None)  # could log here
            .bind(
                lambda c: validate_service(service).map(
                    lambda s: {"container": c, "service": s, "tonnes": tonnes}
                )
            )
            .bind(calculate_invoice)
            .bind(check_limit)
            .map_err(lambda e: f"[{container}] {e}")  # tag error with container
        )

    pipeline_rows = [
        ("MSCU1234567", "stevedoring", 42.5),
        ("TCLU9876543", "cold_storage", 18.0),
        ("BAD-ID", "stevedoring", 10.0),
        ("MSCU1112233", "unknown_service", 5.0),
        ("AMFU7654321", "container_handling", 600.0),  # will exceed limit
    ]

    pipeline_results = [process_row(*row) for row in pipeline_rows]
    successes = [r.unwrap() for r in pipeline_results if r.is_ok()]
    failures = [r.unwrap_err() for r in pipeline_results if r.is_err()]

    success_lines = "\n".join(
        f"  ✓ {inv['container']:15s} {inv['service']:22s} {inv['tonnes']:6.1f}t  → ${inv['amount']:>8,.2f}"
        for inv in successes
    )
    failure_lines = "\n".join(f"  ✗ {e}" for e in failures)

    pipeline_total = sum(inv["amount"] for inv in successes)

    mo.md(f"""
    ```python
    rows = [
        ("MSCU1234567", "stevedoring",        42.5),
        ("TCLU9876543", "cold_storage",       18.0),
        ("BAD-ID",      "stevedoring",        10.0),   # ← bad container
        ("MSCU1112233", "unknown_service",    5.0),    # ← bad service
        ("AMFU7654321", "container_handling", 600.0),  # ← over limit
    ]
    ```

    **Invoiced successfully ({len(successes)} rows):**
    ```
    {success_lines}

    {"─" * 60}
    TOTAL                                             ${pipeline_total:>8,.2f}
    ```

    **Rejected ({len(failures)} rows):**
    ```
    {failure_lines}
    ```

    Each step in the pipeline is a focused, testable function that knows nothing about the others. The `Result` type wires them together, handles all branching, and surfaces errors with full context — no `try/except` blocks, no `None` checks, no silent failures.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ## Quick Reference

    | Method | Signature | Activates on | Returns |
    |---|---|---|---|
    | `is_ok()` | `→ bool` | both | `bool` |
    | `is_err()` | `→ bool` | both | `bool` |
    | `is_ok_and(f)` | `(T→bool) → bool` | `Ok` | `bool` |
    | `is_err_and(f)` | `(E→bool) → bool` | `Err` | `bool` |
    | `unwrap()` | `→ T` | `Ok` | value or **raise** |
    | `unwrap_err()` | `→ E` | `Err` | error or **raise** |
    | `unwrap_or(d)` | `T → T` | `Ok` | value or default |
    | `unwrap_or_else(f)` | `(E→T) → T` | `Ok` | value or `f(err)` |
    | `expect(msg)` | `str → T` | `Ok` | value or **raise** msg |
    | `expect_err(msg)` | `str → E` | `Err` | error or **raise** msg |
    | `map(f)` | `(T→U) → Result[U,E]` | `Ok` | `Result` |
    | `map_err(f)` | `(E→F) → Result[T,F]` | `Err` | `Result` |
    | `map_or(d, f)` | `(U, T→U) → U` | both | plain value |
    | `map_or_else(df, f)` | `(E→U, T→U) → U` | both | plain value |
    | `bind` / `and_then(f)` | `(T→Result[U,E]) → Result[U,E]` | `Ok` | `Result` |
    | `or_else(f)` | `(E→Result[T,F]) → Result[T,F]` | `Err` | `Result` |
    | `inspect(f)` | `(T→None) → self` | `Ok` | `self` (unchanged) |
    | `inspect_err(f)` | `(E→None) → self` | `Err` | `self` (unchanged) |
    | `flatten()` | `→ Result[T,E]` | `Ok(Result)` | unwrapped `Result` |
    | `iter()` | `→ Iterator[T]` | `Ok` | yields value once |
    | `into_ok()` | `→ T` | `Ok` | value or `TypeError` |
    | `into_err()` | `→ E` | `Err` | error or `TypeError` |
    """)
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


if __name__ == "__main__":
    app.run()
