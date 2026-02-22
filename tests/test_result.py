"""Tests for the Result type."""

import pytest

from polars_result import Err, Ok, Result


class TestOk:
    """Test the Ok variant of Result."""

    def test_ok_creation(self) -> None:
        """Test creating an Ok Result."""
        result = Ok(42)
        assert result.is_ok()
        assert not result.is_err()
        assert result.unwrap() == 42

    def test_ok_with_none(self) -> None:
        """Test creating an Ok Result with None value."""
        result = Ok(None)
        assert result.is_ok()
        assert result.unwrap() is None

    def test_repr(self) -> None:
        """Test Ok representation."""
        assert repr(Ok(42)) == "Ok(value=42)"


class TestErr:
    """Test the Err variant of Result."""

    def test_err_creation(self) -> None:
        """Test creating an Err Result."""
        error = ValueError("bad")
        result = Err(error)
        assert result.is_err()
        assert not result.is_ok()
        assert result.unwrap_err() is error

    def test_err_with_string(self) -> None:
        """Test creating an Err Result with a string."""
        result = Err("error message")
        assert result.is_err()
        assert result.unwrap_err() == "error message"

    def test_repr(self) -> None:
        """Test Err representation."""
        r = Err(ValueError("oops"))
        assert "Err(error=" in repr(r)


class TestUnwrap:
    """Test the unwrap methods of Result."""

    def test_unwrap_on_error_raises(self) -> None:
        """Test that calling unwrap on an Err Result raises ValueError."""
        with pytest.raises(ValueError, match="Called unwrap on an Err value"):
            Err(ValueError("fail")).unwrap()

    def test_unwrap_or_returns_value(self) -> None:
        """Test that calling unwrap_or on an Ok Result returns the value."""
        assert Ok(10).unwrap_or(99) == 10

    def test_unwrap_or_returns_default(self) -> None:
        """Test that calling unwrap_or on an Err Result returns the default."""
        assert Err(ValueError("x")).unwrap_or(99) == 99

    def test_unwrap_or_else_with_ok(self) -> None:
        """Test that unwrap_or_else on Ok returns the value."""
        assert Ok(10).unwrap_or_else(lambda e: 99) == 10

    def test_unwrap_or_else_with_err(self) -> None:
        """Test that unwrap_or_else on Err computes default from error."""
        assert Err("error").unwrap_or_else(lambda e: len(e)) == 5

    def test_expect_on_ok(self) -> None:
        """Test that calling expect on an Ok Result returns the value."""
        assert Ok(42).expect("should work") == 42

    def test_expect_on_error(self) -> None:
        """Test that calling expect on an Err Result raises with custom message."""
        with pytest.raises(ValueError, match="custom msg"):
            Err(ValueError("inner")).expect("custom msg")

    def test_expect_err_on_ok_raises(self) -> None:
        """Test that calling expect_err on an Ok Result raises ValueError."""
        with pytest.raises(ValueError, match="expected error"):
            Ok(1).expect_err("expected error")

    def test_expect_err_on_err(self) -> None:
        """Test that calling expect_err on an Err Result returns the error."""
        error = ValueError("fail")
        assert Err(error).expect_err("should be error") is error

    def test_unwrap_err_on_success_raises(self) -> None:
        """Test that calling unwrap_err on an Ok Result raises ValueError."""
        with pytest.raises(ValueError, match="Called unwrap_err on an Ok value"):
            Ok(1).unwrap_err()


class TestTransformers:
    """Test transformers for Result."""

    def test_map_on_ok(self) -> None:
        """Test that calling map on an Ok Result applies the function."""
        assert Ok(5).map(lambda x: x * 2).unwrap() == 10

    def test_map_on_err_passes_through(self) -> None:
        """Test that calling map on an Err Result passes through unchanged."""
        err = ValueError("nope")
        result = Err(err).map(lambda x: x * 2)
        assert result.is_err()
        assert result.unwrap_err() is err

    def test_map_err_on_ok(self) -> None:
        """Test that calling map_err on an Ok Result passes through unchanged."""
        result = Ok(42).map_err(lambda e: TypeError(str(e)))
        assert result.is_ok()
        assert result.unwrap() == 42

    def test_map_err_on_err(self) -> None:
        """Test that calling map_err on an Err Result transforms the error."""
        original = ValueError("original")
        result = Err(original).map_err(lambda e: TypeError(f"wrapped: {e}"))
        error = result.unwrap_err()
        assert isinstance(error, TypeError)
        assert "wrapped: original" in str(error)

    def test_map_or(self) -> None:
        """Test map_or on Ok and Err."""
        assert Ok(5).map_or(0, lambda x: x * 2) == 10
        assert Err("error").map_or(0, lambda x: x * 2) == 0

    def test_map_or_else(self) -> None:
        """Test map_or_else on Ok and Err."""
        assert Ok(5).map_or_else(lambda e: 0, lambda x: x * 2) == 10
        assert Err("err").map_or_else(lambda e: len(e), lambda x: x * 2) == 3

    def test_and_then_on_ok(self) -> None:
        """Test that calling and_then on an Ok Result chains the function."""
        result = Ok(5).and_then(lambda x: Ok(x + 1))
        assert result.unwrap() == 6

    def test_and_then_on_err_passes_through(self) -> None:
        """Test that calling and_then on an Err Result passes through unchanged."""
        err = ValueError("nope")
        result = Err(err).and_then(lambda x: Ok(x + 1))
        assert result.is_err()
        assert result.unwrap_err() is err

    def test_or_else_on_ok(self) -> None:
        """Test that calling or_else on an Ok Result returns unchanged."""
        result = Ok(1).or_else(lambda _e: Ok(99))
        assert result.unwrap() == 1

    def test_or_else_on_err_recovers(self) -> None:
        """Test that calling or_else on an Err Result allows recovery."""
        result = Err(ValueError("x")).or_else(lambda _e: Ok(99))
        assert result.unwrap() == 99


class TestPredicates:
    """Test predicate methods."""

    def test_is_ok_and(self) -> None:
        """Test is_ok_and predicate."""
        assert Ok(10).is_ok_and(lambda x: x > 5)
        assert not Ok(3).is_ok_and(lambda x: x > 5)
        assert not Err("error").is_ok_and(lambda x: x > 5)

    def test_is_err_and(self) -> None:
        """Test is_err_and predicate."""
        assert Err(ValueError("bad")).is_err_and(lambda e: isinstance(e, ValueError))
        assert not Err(TypeError("bad")).is_err_and(lambda e: isinstance(e, ValueError))
        assert not Ok(42).is_err_and(lambda e: isinstance(e, ValueError))


class TestInspection:
    """Test inspection methods."""

    def test_inspect_on_ok(self) -> None:
        """Test that inspect is called on Ok and returns self."""
        called = []
        result = Ok(42).inspect(lambda x: called.append(x))
        assert called == [42]
        assert result.unwrap() == 42

    def test_inspect_on_err(self) -> None:
        """Test that inspect is not called on Err."""
        called = []
        result = Err("error").inspect(lambda x: called.append(x))
        assert called == []
        assert result.is_err()

    def test_inspect_err_on_ok(self) -> None:
        """Test that inspect_err is not called on Ok."""
        called = []
        result = Ok(42).inspect_err(lambda e: called.append(e))
        assert called == []
        assert result.unwrap() == 42

    def test_inspect_err_on_err(self) -> None:
        """Test that inspect_err is called on Err and returns self."""
        called = []
        result = Err("error").inspect_err(lambda e: called.append(e))
        assert called == ["error"]
        assert result.is_err()


class TestIteration:
    """Test iteration over Result."""

    def test_iter_on_ok(self) -> None:
        """Test that iterating over Ok yields the value once."""
        result = Ok(42)
        values = list(result)
        assert values == [42]

    def test_iter_on_err(self) -> None:
        """Test that iterating over Err yields nothing."""
        result = Err("error")
        values = list(result)
        assert values == []


class TestFlatten:
    """Test flatten method."""

    def test_flatten_ok_ok(self) -> None:
        """Test flattening Ok(Ok(value))."""
        nested = Ok(Ok(42))
        flat = nested.flatten()
        assert flat.unwrap() == 42

    def test_flatten_ok_err(self) -> None:
        """Test flattening Ok(Err(error))."""
        nested = Ok(Err("error"))
        flat = nested.flatten()
        assert flat.is_err()
        assert flat.unwrap_err() == "error"

    def test_flatten_err(self) -> None:
        """Test flattening Err."""
        nested = Err("outer error")
        flat = nested.flatten()
        assert flat.is_err()
        assert flat.unwrap_err() == "outer error"


class TestIntoMethods:
    """Test into_ok and into_err methods."""

    def test_into_ok_on_ok(self) -> None:
        """Test into_ok on Ok variant."""
        assert Ok(42).into_ok() == 42

    def test_into_ok_on_err_raises(self) -> None:
        """Test into_ok on Err variant raises."""
        with pytest.raises(TypeError, match="Called into_ok on Err variant"):
            Err("error").into_ok()

    def test_into_err_on_err(self) -> None:
        """Test into_err on Err variant."""
        assert Err("error").into_err() == "error"

    def test_into_err_on_ok_raises(self) -> None:
        """Test into_err on Ok variant raises."""
        with pytest.raises(TypeError, match="Called into_err on Ok variant"):
            Ok(42).into_err()


class TestChaining:
    """Test railway-oriented chaining patterns."""

    def test_pipeline_success(self) -> None:
        """Test successful pipeline with multiple transformations."""
        result = Ok(10).map(lambda x: x + 5).and_then(lambda x: Ok(x * 2)).map(lambda x: x - 1)
        assert result.unwrap() == 29

    def test_pipeline_short_circuits_on_error(self) -> None:
        """Test that pipeline short-circuits on first error."""
        call_count = 0

        def should_not_run(x: int) -> Result[int, str]:
            """This function should not be called."""
            nonlocal call_count
            call_count += 1
            return Ok(x)

        result = (
            Ok(10).and_then(lambda _x: Err("stop")).and_then(should_not_run).map(lambda x: x * 100)
        )
        assert result.is_err()
        assert call_count == 0

    def test_error_recovery_with_or_else(self) -> None:
        """Test error recovery using or_else."""
        result = (
            Err("first error").or_else(lambda _: Err("second error")).or_else(lambda _: Ok(42))
        )
        assert result.unwrap() == 42
