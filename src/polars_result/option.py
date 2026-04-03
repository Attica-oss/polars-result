"""Python implementation of the Option type."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Never

from .result import Err, Ok, Result


@dataclass
class Some[T]:
    """Represents a value that exists."""

    value: T

    def is_some(self) -> bool:
        """Returns True if the option is a Some value."""
        return True

    def is_none(self) -> bool:
        """Returns False — Some always contains a value."""
        return False

    def unwrap(self) -> T:
        """Returns the contained value."""
        return self.value

    def unwrap_or(self, default: T) -> T:
        """Returns the contained value, ignoring the default."""
        return self.value

    def unwrap_or_else(self, f: Callable[[], T]) -> T:
        """Returns the contained value without calling f."""
        return self.value

    def expect(self, msg: str) -> T:
        """Returns the contained value."""
        return self.value

    def map[U](self, f: Callable[[T], U]) -> Option[U]:
        """Applies f to the contained value and wraps the result in Some."""
        return Some(f(self.value))

    def map_or[U](self, default: U, f: Callable[[T], U]) -> U:
        """Applies f to the contained value and returns the result."""
        return f(self.value)

    def map_or_else[U](self, default: Callable[[], U], f: Callable[[T], U]) -> U:
        """Applies f to the contained value and returns the result."""
        return f(self.value)

    def and_then[U](self, f: Callable[[T], Option[U]]) -> Option[U]:
        """Calls f with the contained value and returns the result."""
        return f(self.value)

    def or_else(self, f: Callable[[], Option[T]]) -> Option[T]:
        """Returns self — Some short-circuits or_else."""
        return self

    def filter(self, predicate: Callable[[T], bool]) -> Option[T]:
        """Returns self if predicate holds, otherwise Nothing."""
        if predicate(self.value):
            return self
        return Nothing

    def is_some_and(self, f: Callable[[T], bool]) -> bool:
        """Returns True if the predicate holds for the contained value."""
        return f(self.value)

    def inspect(self, f: Callable[[T], None]) -> Some[T]:
        """Calls f with the contained value for side-effects; returns self."""
        f(self.value)
        return self

    def iter(self) -> Iterator[T]:
        """Returns an iterator that yields the contained value once."""
        yield self.value

    def __iter__(self) -> Iterator[T]:
        """Makes Some directly iterable — yields the contained value once."""
        return self.iter()

    def flatten[U](self) -> Option[U]:
        """Collapses Option[Option[U]] into Option[U].

        Raises TypeError if the contained value is not itself an Option.
        """
        if isinstance(self.value, (Some, _NoneOption)):
            return self.value
        raise TypeError(f"flatten called on non-nested Option: {self.value!r}")

    def ok_or[E](self, err: E) -> Result[T, E]:
        """Converts Some(v) to Ok(v)."""
        return Ok(self.value)

    def ok_or_else[E](self, f: Callable[[], E]) -> Result[T, E]:
        """Converts Some(v) to Ok(v) without calling f."""
        return Ok(self.value)

    def __repr__(self) -> str:
        return f"Some({self.value!r})"


@dataclass
class _NoneOption:
    """Represents the absence of a value.

    Not intended to be instantiated directly — use the ``Nothing`` singleton.
    Every method that would use the missing value uses an independent type
    variable so that ``Nothing`` (typed as ``_NoneOption[Never]``) does not
    propagate ``Never`` into call sites.
    """

    def is_some(self) -> bool:
        """Returns False — Nothing never contains a value."""
        return False

    def is_none(self) -> bool:
        """Returns True."""
        return True

    def unwrap(self) -> Never:
        """Always raises — Nothing has no value to unwrap."""
        raise ValueError("Called unwrap on Nothing")

    def unwrap_or[T](self, default: T) -> T:
        """Returns the provided default."""
        return default

    def unwrap_or_else[T](self, f: Callable[[], T]) -> T:
        """Computes and returns the default by calling f."""
        return f()

    def expect(self, msg: str) -> Never:
        """Always raises with the provided message."""
        raise ValueError(msg)

    def map[U](self, f: Callable[..., U]) -> Option[U]:
        """Returns Nothing — f is never called."""
        return self

    def map_or[U](self, default: U, f: Callable[..., U]) -> U:
        """Returns the provided default — f is never called."""
        return default

    def map_or_else[U](self, default: Callable[[], U], f: Callable[..., U]) -> U:
        """Calls default() and returns the result — f is never called."""
        return default()

    def and_then[U](self, f: Callable[..., Option[U]]) -> Option[U]:
        """Returns Nothing — f is never called."""
        return self

    def or_else[T](self, f: Callable[[], Option[T]]) -> Option[T]:
        """Calls f and returns the result."""
        return f()

    def filter(self, predicate: Callable[..., bool]) -> Option[Never]:
        """Returns Nothing — predicate is never called."""
        return self

    def is_some_and(self, f: Callable[..., bool]) -> bool:
        """Returns False — Nothing has no value to test."""
        return False

    def inspect(self, f: Callable[..., None]) -> _NoneOption:
        """Returns self — f is never called."""
        return self

    def iter(self) -> Iterator[Never]:
        """Returns an empty iterator."""
        return iter(())

    def __iter__(self) -> Iterator[Never]:
        """Makes Nothing directly iterable — yields nothing."""
        return self.iter()

    def flatten(self) -> _NoneOption:
        """Returns Nothing — no nesting to collapse."""
        return self

    def ok_or[T, E](self, err: E) -> Result[T, E]:
        """Converts Nothing to Err(err)."""
        return Err(err)

    def ok_or_else[T, E](self, f: Callable[[], E]) -> Result[T, E]:
        """Converts Nothing to Err(f())."""
        return Err(f())

    def __repr__(self) -> str:
        return "Nothing"


# Singleton — use this everywhere instead of constructing _NoneOption directly
Nothing = _NoneOption()

# Type alias
type Option[T] = Some[T] | _NoneOption
