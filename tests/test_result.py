"""Tests for the Result type."""

import pytest

from polars_result import PolarsResultError, Result


class TestResultOk:
    """Test creating a successful Result."""
    def test_ok_creation(self) -> None:
        """Test creating a successful Result."""
        result = Result.ok(42)
        assert result.is_ok()
        assert not result.is_err()
        assert result.unwrap() == 42

    def test_ok_none_raises(self) -> None:
        """Test creating a successful Result with None."""
        with pytest.raises(ValueError, match="Cannot create a successful Result"):
            Result.ok(None)

    def test_bool_truthy(self) -> None:
        """Test creating a successful Result with a truthy value."""
        assert Result.ok("hello")

    def test_repr(self) -> None:
        """Test creating a successful Result with a representation."""
        assert repr(Result.ok(42)) == "Result.ok(42)"


class TestResultErr:
    """Test creating an error Result."""
    def test_err_creation(self) -> None:
        """Test creating an error Result."""
        error = ValueError("bad")
        result = Result.err(error)
        assert result.is_err()
        assert not result.is_ok()
        assert result.unwrap_err() is error

    def test_err_none_raises(self) -> None:
        """Test creating an error Result with None raises ValueError."""
        with pytest.raises(ValueError, match="Cannot create an error Result"):
            Result.err(None)

    def test_bool_falsy(self) -> None:
        """Test creating an error Result with a falsy value."""
        assert not Result.err(ValueError("x"))

    def test_repr(self) -> None:
        """Test creating an error Result with a falsy value."""
        r = Result.err(ValueError("oops"))
        assert r"Result.err(" in repr(r)


class TestUnwrap:
    """Test the unwrap method of Result."""
    def test_unwrap_on_error_raises(self) -> None:
        """Test that calling unwrap on an error Result raises an error."""
        with pytest.raises(PolarsResultError, match="Called unwrap on error"):
            Result.err(ValueError("fail")).unwrap()

    def test_unwrap_or_returns_value(self) -> None:
        """Test that calling unwrap_or on an Ok Result returns the value."""
        assert Result.ok(10).unwrap_or(99) == 10

    def test_unwrap_or_returns_default(self) -> None:
        """Test that calling unwrap_or on an Err Result returns the default."""
        assert Result.err(ValueError("x")).unwrap_or(99) == 99

    def test_expect_on_error(self) -> None:
        """Test that calling expect on an Err Result raises an error with the expected message."""
        with pytest.raises(PolarsResultError, match="custom msg"):
            Result.err(ValueError("inner")).expect("custom msg")

    def test_unwrap_err_on_success_raises(self) -> None:
        """Test that calling unwrap_err on an Ok Result raises an error."""
        with pytest.raises(PolarsResultError, match="Called unwrap_err on success"):
            Result.ok(1).unwrap_err()


class TestTransformers:
    """Test transformers for Result."""
    def test_map_on_ok(self) -> None:
        """Test that calling map on an Ok Result applies the function to the value."""
        assert Result.ok(5).map(lambda x: x * 2).unwrap() == 10

    def test_map_on_err_passes_through(self) -> None:
        """Test that calling map on an Err Result passes through the error."""
        err = ValueError("nope")
        result = Result.err(err).map(lambda x: x * 2)
        assert result.is_err()

    def test_bind_on_ok(self) -> None:
        """Test that calling bind on an Ok Result applies the function to the value."""
        result = Result.ok(5).bind(lambda x: Result.ok(x + 1))
        assert result.unwrap() == 6

    def test_bind_on_err_passes_through(self) -> None:
        """Test that calling bind on an Err Result passes through the error."""
        err = ValueError("nope")
        result = Result.err(err).bind(lambda x: Result.ok(x + 1))
        assert result.is_err()

    def test_and_then_aliases_bind(self) -> None:
        """Test that calling and_then on an Ok Result applies the function to the value."""
        result = Result.ok(3).and_then(lambda x: Result.ok(x * 3))
        assert result.unwrap() == 9

    def test_map_err(self) -> None:
        """Test that calling map_err on an Err Result applies the function to the error."""
        original = ValueError("original")
        result = Result.err(original).map_err(lambda e: TypeError(str(e)))
        assert isinstance(result.unwrap_err(), TypeError)

    def test_or_else_on_ok(self) -> None:
        """Test that calling or_else on an Ok Result does not change the result."""
        result = Result.ok(1).or_else(lambda _e: Result.ok(99))
        assert result.unwrap() == 1

    def test_or_else_on_err_recovers(self) -> None:
        """Test that calling or_else on an Err Result applies the function to the error."""
        result = Result.err(ValueError("x")).or_else(lambda _e: Result.ok(99))
        assert result.unwrap() == 99


class TestChaining:
    """Test railway-oriented chaining patterns."""

    def test_pipeline_success(self) -> None:
        """Test that calling map_err on an Err Result applies the function to the error."""
        result = (
            Result.ok(10)
            .map(lambda x: x + 5)
            .bind(lambda x: Result.ok(x * 2))
            .map(lambda x: x - 1)
        )
        assert result.unwrap() == 29

    def test_pipeline_short_circuits_on_error(self) -> None:
        """Test that calling bind on an Err Result does not call the function."""
        call_count = 0

        def should_not_run(x: int) -> Result:
            """This function should not be called."""
            nonlocal call_count
            call_count += 1
            return Result.ok(x)

        result = (
            Result.ok(10)
            .bind(lambda _x: Result.err(ValueError("stop")))
            .bind(should_not_run)
            .map(lambda x: x * 100)
        )
        assert result.is_err()
        assert call_count == 0
