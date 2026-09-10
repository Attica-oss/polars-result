"""catch(), resultify() delegation, __bool__, and exception chaining."""

import polars as pl
import pytest
from polars.exceptions import ComputeError

from polars_result import (
    Err,
    Nothing,
    Ok,
    PipelineError,
    PolarsResultError,
    ResultSchemaError,
    Some,
    ValidationError,
    catch,
    resultify,
)


class TestCatch:
    def test_success_wraps_in_ok(self) -> None:
        assert catch(lambda: 21 * 2) == Ok(42)

    def test_none_is_ok_none(self) -> None:
        assert catch(lambda: None) == Ok(None)

    def test_existing_result_passes_through(self) -> None:
        assert catch(lambda: Ok(1)) == Ok(1)
        assert catch(lambda: Err("boom")) == Err("boom")

    def test_generic_exception_becomes_pipeline_error_with_cause(self) -> None:
        r = catch(lambda: 1 // 0)
        assert r.is_err()
        err = r.unwrap_err()
        assert isinstance(err, PipelineError)
        assert isinstance(err.cause, ZeroDivisionError)
        assert err.__cause__ is err.cause
        assert err.__suppress_context__ is True

    def test_context_prefixes_message(self) -> None:
        r = catch(lambda: int("bad"), context="parse count")
        assert str(r.unwrap_err()).startswith("parse count: ")

    def test_error_type_override(self) -> None:
        r = catch(lambda: int("bad"), error_type=ValidationError)
        assert type(r.unwrap_err()) is ValidationError

    def test_unmatched_exception_propagates(self) -> None:
        with pytest.raises(KeyError):
            catch(lambda: {}["missing"], catch_types=ValueError)

    def test_polars_error_maps_to_structured_subclass(self) -> None:
        # selecting a missing column raises a Polars error
        r = catch(
            lambda: pl.DataFrame({"a": [1]}).select("nope"),
            context="select",
        )
        err = r.unwrap_err()
        assert isinstance(err, ResultSchemaError)  # via from_polars mapping
        assert isinstance(err.cause, pl.exceptions.PolarsError)


class TestFromPolarsSubclasses:
    def test_direct_type_maps(self) -> None:
        err = PolarsResultError.from_polars(ComputeError("overflow"), "totals")
        assert type(err) is PipelineError
        assert isinstance(err.cause, ComputeError)
        assert "overflow" in str(err.cause)

    def test_subclass_of_mapped_type_still_maps(self) -> None:
        # created dynamically so the static checker doesn't wrestle with
        # Polars' dual ComputeError classes
        weird_cls = type("WeirdComputeError", (ComputeError,), {})
        err = PolarsResultError.from_polars(weird_cls("x"), "op")
        assert type(err) is PipelineError  # matched via isinstance, not exact type


class TestResultify:
    def test_bare(self) -> None:
        @resultify
        def add(a: int, b: int) -> int:
            return a + b

        assert add(2, 3) == Ok(5)

    def test_bare_catches(self) -> None:
        @resultify
        def boom() -> int:
            raise RuntimeError("nope")

        r = boom()
        assert isinstance(r.unwrap_err(), PipelineError)
        assert isinstance(r.unwrap_err().cause, RuntimeError)

    def test_parameterized_catch_types_and_error_type(self) -> None:
        @resultify(catch_types=ValueError, error_type=ValidationError)
        def parse(s: str) -> int:
            return int(s)

        assert parse("10") == Ok(10)
        assert type(parse("x").unwrap_err()) is ValidationError
        with pytest.raises(TypeError):
            parse(None)  # type: ignore[arg-type]  # not a ValueError -> propagates

    def test_none_return_is_ok_none(self) -> None:
        @resultify
        def sideeffect() -> None:
            return None

        assert sideeffect() == Ok(None)

    def test_passes_through_result_return(self) -> None:
        @resultify
        def already() -> "Ok[int]":
            return Ok(99)

        assert already() == Ok(99)

    def test_preserves_metadata(self) -> None:
        @resultify
        def documented() -> int:
            """A docstring."""
            return 1

        assert documented.__name__ == "documented"
        assert documented.__doc__ == "A docstring."


class TestBool:
    def test_result_truthiness(self) -> None:
        assert bool(Ok(0)) is True
        assert bool(Ok(None)) is True
        assert bool(Err("e")) is False

    def test_option_truthiness(self) -> None:
        assert bool(Some(0)) is True
        assert bool(Nothing) is False

    def test_if_result(self) -> None:
        seen = []
        for r in (Ok(1), Err("e")):
            if r:
                seen.append("ok")
            else:
                seen.append("err")
        assert seen == ["ok", "err"]
