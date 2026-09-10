"""Result / Option are frozen, hashable value types, plus narrowing guards."""

import dataclasses

import pytest

from polars_result import (
    Err,
    Nothing,
    Ok,
    Some,
    is_err,
    is_none,
    is_ok,
    is_some,
)


class TestHashable:
    def test_results_go_in_sets(self) -> None:
        assert {Ok(1), Ok(1), Err("e"), Err("e")} == {Ok(1), Err("e")}

    def test_options_go_in_sets(self) -> None:
        assert {Some(1), Some(1), Nothing, Nothing} == {Some(1), Nothing}

    def test_results_are_dict_keys(self) -> None:
        table = {Ok(1): "a", Err("x"): "b"}
        assert table[Ok(1)] == "a"
        assert table[Err("x")] == "b"

    def test_unhashable_payload_still_raises(self) -> None:
        with pytest.raises(TypeError):
            hash(Ok([1, 2, 3]))


class TestFrozen:
    def test_ok_some_fields_are_read_only(self) -> None:
        with pytest.raises(dataclasses.FrozenInstanceError):
            Ok(1).value = 99  # type: ignore[misc]
        with pytest.raises(dataclasses.FrozenInstanceError):
            Some(1).value = 99  # type: ignore[misc]

    def test_err_field_is_read_only(self) -> None:
        with pytest.raises(dataclasses.FrozenInstanceError):
            Err("e").error = "x"  # type: ignore[misc]


class TestReprQuoting:
    def test_ok_err_quote_str_payloads(self) -> None:
        assert repr(Ok("hi")) == "Ok('hi')"
        assert repr(Err("boom")) == "Err('boom')"

    def test_ok_err_plain_for_non_str(self) -> None:
        assert repr(Ok(42)) == "Ok(42)"
        assert repr(Err(7)) == "Err(7)"


class TestGuards:
    def test_is_ok_is_err(self) -> None:
        assert is_ok(Ok(1)) and not is_err(Ok(1))
        assert is_err(Err("e")) and not is_ok(Err("e"))

    def test_is_some_is_none(self) -> None:
        assert is_some(Some(1)) and not is_none(Some(1))
        assert is_none(Nothing) and not is_some(Nothing)

    def test_guard_narrows_for_access(self) -> None:
        r = Ok(10) if True else Err("e")
        if is_ok(r):
            assert r.value == 10  # attribute is Ok-only
        else:  # pragma: no cover
            pytest.fail("expected Ok")
