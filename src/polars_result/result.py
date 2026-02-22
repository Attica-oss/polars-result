"""Python implementation of the Result type."""

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import TypeVar

T = TypeVar("T")
E = TypeVar("E")
U = TypeVar("U")
F = TypeVar("F")  # For error type transformations


class Infallible:
    """A type that can never be instantiated (like Rust's !)"""

    def __init__(self) -> None:  # Fixed: was -> E
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

    def unwrap_err(self) -> E:
        """Returns the error value. Raises if Ok."""
        raise ValueError(f"Called unwrap_err on an Ok value: {self.value}")

    def unwrap_or(self, default: T) -> T:
        return self.value

    def unwrap_or_else(self, op: Callable[[E], T]) -> T:
        """Returns the Ok value, or computes it from error using op"""
        return self.value

    def expect(self, msg: str) -> T:
        """Unwraps the Ok value, or raises with custom message if Err"""
        return self.value

    def expect_err(self, msg: str) -> E:
        """Unwraps the Err value, or raises with custom message if Ok"""
        raise ValueError(f"{msg}: {self.value}")

    def map(self, fn: Callable[[T], U]) -> "Result[U, E]":
        return Ok(fn(self.value))

    def map_err(self, op: Callable[[E], F]) -> "Result[T, F]":
        """Maps Err value using op, leaves Ok unchanged"""
        return Ok(self.value)

    def map_or(self, default: U, f: Callable[[T], U]) -> U:
        """Maps Ok value using f, or returns default if Err"""
        return f(self.value)

    def map_or_else(self, default: Callable[[E], U], f: Callable[[T], U]) -> U:
        """Maps Ok value using f, or computes default from error"""
        return f(self.value)

    def map_or_default(self, f: Callable[[T], U], default: U) -> U:
        """Maps Ok value using f, or returns default if Err"""
        return f(self.value)

    def and_then(self, fn: Callable[[T], "Result[U, E]"]) -> "Result[U, E]":
        return fn(self.value)

    def or_else(self, op: Callable[[E], "Result[T, F]"]) -> "Result[T, F]":
        """Returns self if Ok, otherwise calls op with the error"""
        return Ok(self.value)

    def is_ok_and(self, f: Callable[[T], bool]) -> bool:
        """Returns True if Ok and the value matches the predicate"""
        return f(self.value)

    def is_err_and(self, f: Callable[[E], bool]) -> bool:
        """Returns True if Err and the error matches the predicate"""
        return False

    def inspect(self, f: Callable[[T], None]) -> "Ok[T]":
        """Calls function with the value if Ok, returns self unchanged"""
        f(self.value)
        return self

    def inspect_err(self, f: Callable[[E], None]) -> "Ok[T]":
        """Calls function with the error if Err, returns self unchanged"""
        return self

    def iter(self) -> Iterator[T]:
        """Returns an iterator that yields the value once"""
        yield self.value

    def __iter__(self) -> Iterator[T]:
        """Makes Ok directly iterable"""
        return self.iter()

    def flatten(self) -> "Result[U, E]":
        """Converts Result[Result[U, E], E] into Result[U, E]"""
        if isinstance(self.value, (Ok, Err)):
            return self.value
        raise TypeError(f"flatten called on non-nested Result: {self.value}")

    def into_ok(self) -> T:
        """Unwraps Ok value. Only callable when Err is impossible."""
        return self.value

    def into_err(self) -> E:
        """Unwraps Err value. Only callable when Ok is impossible."""
        raise TypeError("Called into_err on Ok variant")


@dataclass
class Err[E]:
    error: E

    def is_ok(self) -> bool:
        return False

    def is_err(self) -> bool:
        return True

    def unwrap(self) -> T:  # Fixed: returns T, not E
        raise ValueError(f"Called unwrap on an Err value: {self.error}")

    def unwrap_err(self) -> E:
        """Returns the error value. Raises if Ok."""
        return self.error

    def unwrap_or(self, default: T) -> T:
        return default

    def unwrap_or_else(self, op: Callable[[E], T]) -> T:
        """Returns the Ok value, or computes it from error using op"""
        return op(self.error)

    def expect(self, msg: str) -> T:
        """Unwraps the Ok value, or raises with custom message if Err"""
        raise ValueError(f"{msg}: {self.error}")

    def expect_err(self, msg: str) -> E:
        """Unwraps the Err value, or raises with custom message if Ok"""
        return self.error

    def map(self, fn: Callable[[T], U]) -> "Result[U, E]":
        return self

    def map_err(self, op: Callable[[E], F]) -> "Result[T, F]":
        """Maps Err value using op, leaves Ok unchanged"""
        return Err(op(self.error))

    def map_or(self, default: U, f: Callable[[T], U]) -> U:
        """Maps Ok value using f, or returns default if Err"""
        return default

    def map_or_else(self, default: Callable[[E], U], f: Callable[[T], U]) -> U:
        """Maps Ok value using f, or computes default from error"""
        return default(self.error)

    def map_or_default(self, f: Callable[[T], U], default: U) -> U:
        """Maps Ok value using f, or returns default if Err"""
        return default

    def and_then(self, fn: Callable[[T], "Result[U, E]"]) -> "Result[U, E]":
        return self

    def or_else(self, op: Callable[[E], "Result[T, F]"]) -> "Result[T, F]":
        """Returns self if Ok, otherwise calls op with the error"""
        return op(self.error)

    def is_ok_and(self, f: Callable[[T], bool]) -> bool:
        """Returns True if Ok and the value matches the predicate"""
        return False

    def is_err_and(self, f: Callable[[E], bool]) -> bool:
        """Returns True if Err and the error matches the predicate"""
        return f(self.error)

    def inspect(self, f: Callable[[T], None]) -> "Err[E]":
        """Calls function with the value if Ok, returns self unchanged"""
        return self

    def inspect_err(self, f: Callable[[E], None]) -> "Err[E]":
        """Calls function with the error if Err, returns self unchanged"""
        f(self.error)
        return self

    def iter(self) -> Iterator[T]:
        """Returns an empty iterator"""
        return iter(())  # Fixed: cleaner than return/yield

    def __iter__(self) -> Iterator[T]:
        """Makes Err directly iterable (yields nothing)"""
        return self.iter()

    def flatten(self) -> "Result[U, E]":
        """Converts Result[Result[U, E], E] into Result[U, E]"""
        return self

    def into_ok(self) -> T:
        """Unwraps Ok value. Only callable when Err is impossible."""
        raise TypeError(f"Called into_ok on Err variant: {self.error}")

    def into_err(self) -> E:
        """Unwraps Err value. Only callable when Ok is impossible."""
        return self.error


Result = Ok[T] | Err[E]
