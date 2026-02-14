"""Tests for the Result type."""

import pytest

from polars_result import PolarsResultError, Result


class TestResultOk:
    def test_ok_creation(self) -> None:
        result = Result.ok(42)
        assert result.is_ok()
        assert not result.is_err()
        assert result.unwrap() == 42

    def test_ok_none_raises(self) -> None:
        with pytest.raises(ValueError, match="Cannot create a successful Result"):
            Result.ok(None)

    def test_bool_truthy(self) -> None:
        assert Result.ok("hello")

    def test_repr(self) -> None:
        assert repr(Result.ok(42)) == "Result.ok(42)"


class TestResultErr:
    def test_err_creation(self) -> None:
        error = ValueError("bad")
        result = Result.err(error)
        assert result.is_err()
        assert not result.is_ok()
        assert result.unwrap_err() is error

    def test_err_none_raises(self) -> None:
        with pytest.raises(ValueError, match="Cannot create an error Result"):
            Result.err(None)

    def test_bool_falsy(self) -> None:
        assert not Result.err(ValueError("x"))

    def test_repr(self) -> None:
        r = Result.err(ValueError("oops"))
        assert r"Result.err(" in repr(r)


class TestUnwrap:
    def test_unwrap_on_error_raises(self) -> None:
        with pytest.raises(PolarsResultError, match="Called unwrap on error"):
            Result.err(ValueError("fail")).unwrap()

    def test_unwrap_or_returns_value(self) -> None:
        assert Result.ok(10).unwrap_or(99) == 10

    def test_unwrap_or_returns_default(self) -> None:
        assert Result.err(ValueError("x")).unwrap_or(99) == 99

    def test_expect_on_error(self) -> None:
        with pytest.raises(PolarsResultError, match="custom msg"):
            Result.err(ValueError("inner")).expect("custom msg")

    def test_unwrap_err_on_success_raises(self) -> None:
        with pytest.raises(PolarsResultError, match="Called unwrap_err on success"):
            Result.ok(1).unwrap_err()


class TestTransformers:
    def test_map_on_ok(self) -> None:
        assert Result.ok(5).map(lambda x: x * 2).unwrap() == 10

    def test_map_on_err_passes_through(self) -> None:
        err = ValueError("nope")
        result = Result.err(err).map(lambda x: x * 2)
        assert result.is_err()

    def test_bind_on_ok(self) -> None:
        result = Result.ok(5).bind(lambda x: Result.ok(x + 1))
        assert result.unwrap() == 6

    def test_bind_on_err_passes_through(self) -> None:
        err = ValueError("nope")
        result = Result.err(err).bind(lambda x: Result.ok(x + 1))
        assert result.is_err()

    def test_and_then_aliases_bind(self) -> None:
        result = Result.ok(3).and_then(lambda x: Result.ok(x * 3))
        assert result.unwrap() == 9

    def test_map_err(self) -> None:
        original = ValueError("original")
        result = Result.err(original).map_err(lambda e: TypeError(str(e)))
        assert isinstance(result.unwrap_err(), TypeError)

    def test_or_else_on_ok(self) -> None:
        result = Result.ok(1).or_else(lambda _e: Result.ok(99))
        assert result.unwrap() == 1

    def test_or_else_on_err_recovers(self) -> None:
        result = Result.err(ValueError("x")).or_else(lambda _e: Result.ok(99))
        assert result.unwrap() == 99


class TestChaining:
    """Test railway-oriented chaining patterns."""

    def test_pipeline_success(self) -> None:
        result = (
            Result.ok(10)
            .map(lambda x: x + 5)
            .bind(lambda x: Result.ok(x * 2))
            .map(lambda x: x - 1)
        )
        assert result.unwrap() == 29

    def test_pipeline_short_circuits_on_error(self) -> None:
        call_count = 0

        def should_not_run(x: int) -> Result:
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
