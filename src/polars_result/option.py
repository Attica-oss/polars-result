"""Python implementation of the Option type."""

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import TypeVar

T = TypeVar("T")
U = TypeVar("U")


@dataclass
class Some[T]:
    """Represents a value that exists."""

    value: T

    def is_some(self) -> bool:
        """Returns True if the option is a Some value."""
        return True

    def is_none(self) -> bool:
        """Returns True if the option is None."""
        return False

    def unwrap(self) -> T:
        """Returns the contained value."""
        return self.value

    def unwrap_or(self, default: T) -> T:
        """Returns the contained value or a default."""
        return self.value

    def unwrap_or_else(self, f: Callable[[], T]) -> T:
        """Returns the contained value or computes it from a closure."""
        return self.value

    def expect(self, msg: str) -> T:
        """Returns the contained value or raises with a custom message."""
        return self.value

    def map(self, f: Callable[[T], U]) -> "Option[U]":
        """Maps an Option[T] to Option[U] by applying a function to the contained value."""
        return Some(f(self.value))

    def map_or(self, default: U, f: Callable[[T], U]) -> U:
        """Returns the provided default result (if none),
        or applies a function to the contained value."""
        return f(self.value)

    def map_or_else(self, default: Callable[[], U], f: Callable[[T], U]) -> U:
        """Computes a default function result (if none),
        or applies a different function to the contained value."""
        return f(self.value)

    def and_then(self, f: Callable[[T], "Option[U]"]) -> "Option[U]":
        """Returns None if the option is None,
        otherwise calls f with the wrapped value and returns the result."""
        return f(self.value)

    def or_else(self, f: Callable[[], "Option[T]"]) -> "Option[T]":
        """Returns the option if it contains a value,
        otherwise calls f and returns the result."""
        return self

    def filter(self, predicate: Callable[[T], bool]) -> "Option[T]":
        """Returns None if the option is None,
        otherwise calls predicate with the wrapped value and returns Some if true."""
        if predicate(self.value):
            return self
        return Nothing

    def is_some_and(self, f: Callable[[T], bool]) -> bool:
        """Returns True if the option is Some and the value inside matches a predicate."""
        return f(self.value)

    def inspect(self, f: Callable[[T], None]) -> "Some[T]":
        """Calls the provided closure with the contained value (if Some)."""
        f(self.value)
        return self

    def iter(self) -> Iterator[T]:
        """Returns an iterator over the possibly contained value."""
        yield self.value

    def __iter__(self) -> Iterator[T]:
        """Makes Some directly iterable."""
        return self.iter()

    def flatten(self) -> "Option[U]":
        """Converts from Option[Option[U]] to Option[U]."""
        if isinstance(self.value, (Some, NoneType)):
            return self.value
        raise TypeError(f"flatten called on non-nested Option: {self.value}")


@dataclass
class NoneType:
    """Represents the absence of a value."""

    def is_some(self) -> bool:
        """Returns True if the option is a Some value."""
        return False

    def is_none(self) -> bool:
        """Returns True if the option is None."""
        return True

    def unwrap(self) -> T:
        """Raises an error - cannot unwrap None."""
        raise ValueError("Called unwrap on a None value")

    def unwrap_or(self, default: T) -> T:
        """Returns the provided default value."""
        return default

    def unwrap_or_else(self, f: Callable[[], T]) -> T:
        """Returns the result of evaluating f."""
        return f()

    def expect(self, msg: str) -> T:
        """Raises with a custom message."""
        raise ValueError(msg)

    def map(self, f: Callable[[T], U]) -> "Option[U]":
        """Returns None."""
        return self

    def map_or(self, default: U, f: Callable[[T], U]) -> U:
        """Returns the provided default."""
        return default

    def map_or_else(self, default: Callable[[], U], f: Callable[[T], U]) -> U:
        """Returns the result of default function."""
        return default()

    def and_then(self, f: Callable[[T], "Option[U]"]) -> "Option[U]":
        """Returns None."""
        return self

    def or_else(self, f: Callable[[], "Option[T]"]) -> "Option[T]":
        """Calls f and returns the result."""
        return f()

    def filter(self, predicate: Callable[[T], bool]) -> "Option[T]":
        """Returns None."""
        return self

    def is_some_and(self, f: Callable[[T], bool]) -> bool:
        """Returns False."""
        return False

    def inspect(self, f: Callable[[T], None]) -> "NoneType":
        """Does nothing, returns self."""
        return self

    def iter(self) -> Iterator[T]:
        """Returns an empty iterator."""
        return iter(())

    def __iter__(self) -> Iterator[T]:
        """Makes NoneType directly iterable (yields nothing)."""
        return self.iter()

    def flatten(self) -> "Option[U]":
        """Returns None."""
        return self


# Singleton None instance
Nothing = NoneType()

# Type alias
Option = Some[T] | NoneType
