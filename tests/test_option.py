"""Tests for the Option type."""

import pytest

from polars_result import Nothing, Option, Some
from polars_result.option import _NoneOption


class TestSome:
    """Test the Some variant of Option."""

    def test_some_creation(self) -> None:
        """Test creating a Some Option."""
        opt = Some(42)
        assert opt.is_some()
        assert not opt.is_none()
        assert opt.unwrap() == 42

    def test_some_with_none_value(self) -> None:
        """Test creating Some wrapping None — distinct from Nothing."""
        opt = Some(None)
        assert opt.is_some()
        assert opt.unwrap() is None

    def test_some_with_string(self) -> None:
        """Test Some wrapping a string."""
        opt = Some("hello")
        assert opt.unwrap() == "hello"

    def test_repr(self) -> None:
        """Test Some representation."""
        assert repr(Some(42)) == "Some(42)"


class TestNothing:
    """Test the Nothing singleton."""

    def test_nothing_is_none(self) -> None:
        """Test Nothing reports is_none correctly."""
        assert Nothing.is_none()
        assert not Nothing.is_some()

    def test_nothing_is_instance_of_none_option(self) -> None:
        """Test that Nothing is an instance of _NoneOption."""
        assert isinstance(Nothing, _NoneOption)

    def test_repr(self) -> None:
        """Test Nothing has a representation."""
        assert repr(Nothing) is not None


class TestUnwrap:
    """Test unwrap methods for both variants."""

    def test_unwrap_some(self) -> None:
        """Test unwrap on Some returns the value."""
        assert Some(99).unwrap() == 99

    def test_unwrap_nothing_raises(self) -> None:
        """Test unwrap on Nothing raises ValueError."""
        with pytest.raises(ValueError, match="Called unwrap on Nothing"):
            Nothing.unwrap()

    def test_unwrap_or_some(self) -> None:
        """Test unwrap_or on Some returns the value."""
        assert Some(10).unwrap_or(99) == 10

    def test_unwrap_or_nothing(self) -> None:
        """Test unwrap_or on Nothing returns the default."""
        assert Nothing.unwrap_or(99) == 99

    def test_unwrap_or_else_some(self) -> None:
        """Test unwrap_or_else on Some returns the value without calling f."""
        called: list[int] = []
        result = Some(10).unwrap_or_else(lambda: called.append(1) or 99)
        assert result == 10
        assert called == []

    def test_unwrap_or_else_nothing(self) -> None:
        """Test unwrap_or_else on Nothing computes the default."""
        assert Nothing.unwrap_or_else(lambda: 42) == 42

    def test_expect_some(self) -> None:
        """Test expect on Some returns the value."""
        assert Some(7).expect("should exist") == 7

    def test_expect_nothing_raises(self) -> None:
        """Test expect on Nothing raises with the custom message."""
        with pytest.raises(ValueError, match="not found"):
            Nothing.expect("not found")


class TestPredicates:
    """Test predicate methods."""

    def test_is_some_and_true(self) -> None:
        """Test is_some_and returns True when predicate matches."""
        assert Some(10).is_some_and(lambda x: x > 5)

    def test_is_some_and_false(self) -> None:
        """Test is_some_and returns False when predicate does not match."""
        assert not Some(3).is_some_and(lambda x: x > 5)

    def test_is_some_and_nothing(self) -> None:
        """Test is_some_and on Nothing always returns False."""
        assert not Nothing.is_some_and(lambda x: x > 5)


class TestTransformers:
    """Test transforming methods."""

    def test_map_some(self) -> None:
        """Test map on Some applies the function."""
        result = Some(5).map(lambda x: x * 2)
        assert result.unwrap() == 10

    def test_map_nothing(self) -> None:
        """Test map on Nothing returns Nothing."""
        result = Nothing.map(lambda x: x * 2)
        assert result.is_none()

    def test_map_or_some(self) -> None:
        """Test map_or on Some applies the function."""
        assert Some(5).map_or(0, lambda x: x * 2) == 10

    def test_map_or_nothing(self) -> None:
        """Test map_or on Nothing returns the default."""
        assert Nothing.map_or(0, lambda x: x * 2) == 0

    def test_map_or_else_some(self) -> None:
        """Test map_or_else on Some applies f."""
        assert Some(5).map_or_else(lambda: 0, lambda x: x * 2) == 10

    def test_map_or_else_nothing(self) -> None:
        """Test map_or_else on Nothing calls the default function."""
        assert Nothing.map_or_else(lambda: 99, lambda x: x * 2) == 99

    def test_and_then_some(self) -> None:
        """Test and_then on Some chains the function."""
        result = Some(5).and_then(lambda x: Some(x + 1))
        assert result.unwrap() == 6

    def test_and_then_some_returning_nothing(self) -> None:
        """Test and_then on Some where f returns Nothing."""
        result: Option[int] = Some(5).and_then(lambda _: Nothing)
        assert result.is_none()

    def test_and_then_nothing(self) -> None:
        """Test and_then on Nothing returns Nothing without calling f."""
        called: list[int] = []
        result = Nothing.and_then(lambda x: called.append(x) or Some(x))
        assert result.is_none()
        assert called == []

    def test_or_else_some(self) -> None:
        """Test or_else on Some returns self without calling f."""
        called: list[int] = []
        result = Some(42).or_else(lambda: called.append(1) or Some(99))
        assert result.unwrap() == 42
        assert called == []

    def test_or_else_nothing(self) -> None:
        """Test or_else on Nothing calls f and returns the result."""
        result = Nothing.or_else(lambda: Some(99))
        assert result.unwrap() == 99

    def test_or_else_nothing_returning_nothing(self) -> None:
        """Test or_else on Nothing where f also returns Nothing."""
        result: Option[int] = Nothing.or_else(lambda: Nothing)
        assert result.is_none()


class TestFilter:
    """Test the filter method."""

    def test_filter_some_predicate_true(self) -> None:
        """Test filter on Some when predicate passes returns Some."""
        result = Some(10).filter(lambda x: x > 5)
        assert result.unwrap() == 10

    def test_filter_some_predicate_false(self) -> None:
        """Test filter on Some when predicate fails returns Nothing."""
        result = Some(3).filter(lambda x: x > 5)
        assert result.is_none()

    def test_filter_nothing(self) -> None:
        """Test filter on Nothing always returns Nothing."""
        result = Nothing.filter(lambda x: x > 5)
        assert result.is_none()


class TestFlatten:
    """Test the flatten method."""

    def test_flatten_some_some(self) -> None:
        """Test flattening Some(Some(value))."""
        nested = Some(Some(42))
        assert nested.flatten().unwrap() == 42

    def test_flatten_some_nothing(self) -> None:
        """Test flattening Some(Nothing)."""
        nested: Some[Option[int]] = Some(Nothing)
        assert nested.flatten().is_none()

    def test_flatten_nothing(self) -> None:
        """Test flattening Nothing is a no-op."""
        assert Nothing.flatten().is_none()

    def test_flatten_non_nested_raises(self) -> None:
        """Test that flatten on Some(non-Option) raises TypeError."""
        with pytest.raises(TypeError, match="flatten called on non-nested Option"):
            Some(42).flatten()


class TestInspect:
    """Test the inspect method."""

    def test_inspect_some(self) -> None:
        """Test inspect is called on Some and returns self."""
        called: list[int] = []
        result = Some(42).inspect(lambda x: called.append(x))
        assert called == [42]
        assert result.unwrap() == 42

    def test_inspect_nothing(self) -> None:
        """Test inspect is not called on Nothing."""
        called: list[int] = []
        result = Nothing.inspect(lambda x: called.append(x))
        assert called == []
        assert result.is_none()


class TestIteration:
    """Test iteration over Option."""

    def test_iter_some(self) -> None:
        """Test iterating over Some yields the value once."""
        assert list(Some(42)) == [42]

    def test_iter_nothing(self) -> None:
        """Test iterating over Nothing yields nothing."""
        assert list(Nothing) == []

    def test_comprehension_mixed(self) -> None:
        """Test filtering a mixed list via iteration."""
        opts: list[Option[int]] = [Some(1), Nothing, Some(2), Nothing, Some(3)]
        values = [v for o in opts for v in o]
        assert values == [1, 2, 3]

    def test_sum_via_iteration(self) -> None:
        """Test summing values via iteration."""
        opts: list[Option[int]] = [Some(10), Nothing, Some(20), Nothing, Some(5)]
        total = sum(v for o in opts for v in o)
        assert total == 35


class TestOkOr:
    """Test conversion from Option to Result."""

    def test_ok_or_some(self) -> None:
        """Test ok_or on Some produces Ok."""
        from polars_result import Ok

        result = Some(42).ok_or("missing")
        assert isinstance(result, Ok)
        assert result.unwrap() == 42

    def test_ok_or_nothing(self) -> None:
        """Test ok_or on Nothing produces Err."""
        from polars_result import Err

        result = Nothing.ok_or("missing")
        assert isinstance(result, Err)
        assert result.unwrap_err() == "missing"

    def test_ok_or_with_exception(self) -> None:
        """Test ok_or with a structured exception as the error value."""
        from polars_result import Err
        from polars_result.exceptions import ValidationError

        err = ValidationError("user_id is required", field="user_id")
        result = Nothing.ok_or(err)
        assert isinstance(result, Err)
        unwrapped = result.unwrap_err()
        assert isinstance(unwrapped, ValidationError)
        assert unwrapped.field == "user_id"


class TestPatternMatching:
    """Test pattern matching on Option variants."""

    def test_match_some(self) -> None:
        """Test matching on Some extracts the value."""
        opt: Option[int] = Some(42)
        match opt:
            case Some(v):
                assert v == 42
            case _:
                pytest.fail("Expected Some branch")

    def test_match_nothing(self) -> None:
        """Test matching on Nothing hits the nothing branch."""
        opt: Option[int] = Nothing
        match opt:
            case _ if opt.is_none():
                assert True
            case _:
                pytest.fail("Expected Nothing branch")

    def test_match_in_pipeline(self) -> None:
        """Test using Option in a realistic match pipeline."""

        def lookup(key: str) -> Option[int]:
            data = {"a": 1, "b": 2}
            return Some(data[key]) if key in data else Nothing

        results: list[int | None] = []
        for key in ["a", "c", "b"]:
            match lookup(key):
                case Some(v):
                    results.append(v)
                case _:
                    results.append(None)

        assert results == [1, None, 2]


class TestChaining:
    """Test chaining patterns with Option."""

    def test_chained_and_then(self) -> None:
        """Test multiple and_then calls chain correctly."""
        result = Some(10).and_then(lambda x: Some(x * 2)).and_then(lambda x: Some(x + 1))
        assert result.unwrap() == 21

    def test_chain_short_circuits_on_nothing(self) -> None:
        """Test chaining short-circuits when Nothing is produced."""
        called: list[int] = []

        def should_not_run(x: int) -> Option[int]:
            called.append(x)
            return Some(x)

        result: Option[int] = Some(10).and_then(lambda _: Nothing).and_then(should_not_run)
        assert result.is_none()
        assert called == []

    def test_recovery_with_or_else(self) -> None:
        """Test chaining or_else for fallback logic."""
        result = Nothing.or_else(lambda: Nothing).or_else(lambda: Some(42))
        assert result.unwrap() == 42

    def test_option_to_result_pipeline(self) -> None:
        """Test converting Option to Result and continuing the chain."""
        from polars_result import Ok
        from polars_result.exceptions import ValidationError

        result = (
            Some("  hello  ")
            .map(str.strip)
            .filter(lambda s: len(s) > 0)
            .ok_or(ValidationError("value is empty", field="name"))
            .map(str.upper)
        )
        assert isinstance(result, Ok)
        assert result.unwrap() == "HELLO"
