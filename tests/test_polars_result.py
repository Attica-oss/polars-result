"""Tests for Polars Result operations."""

import stat
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import starlette
from polars.exceptions import PolarsError

from polars_result import (
    # Err,
    Ok,
    PolarsResult,
    catch,
    collect,
    from_dict,
    polars_catch,
    read_csv,
    read_json,
    read_parquet,
    scan_csv,
    scan_parquet,
)


class TestCatch:
    """Test the catch function for converting exceptions to Results."""

    def test_catch_success(self) -> None:
        """Test catch returns Ok on success."""
        result = catch(lambda: 42)
        assert result.is_ok()
        assert result.unwrap() == 42

    def test_catch_failure(self) -> None:
        """Test catch returns Err on exception."""
        result = catch(lambda: 1 / 0)
        assert result.is_err()
        assert isinstance(result.unwrap_err(), ZeroDivisionError)

    def test_catch_with_specific_error_type(self) -> None:
        """Test catch with specific error type catches only that type."""
        result = catch(lambda: int("bad"), ValueError)
        assert result.is_err()
        assert isinstance(result.unwrap_err(), ValueError)

    def test_catch_does_not_catch_other_exceptions(self) -> None:
        """Test catch with specific error type doesn't catch other exceptions."""
        with pytest.raises(ZeroDivisionError):
            catch(lambda: 1 / 0, ValueError)

    def test_catch_with_none_return(self) -> None:
        """Test catch handles None return value."""
        result = catch(lambda: None)
        assert result.is_ok()
        assert result.unwrap() is None


class TestPolarsCatch:
    """Test the polars_catch function."""

    def test_polars_catch_success(self) -> None:
        """Test polars_catch returns Ok on success."""
        result = polars_catch(lambda: pl.DataFrame({"a": [1, 2, 3]}), "test operation")
        assert result.is_ok()
        assert isinstance(result.unwrap(), pl.DataFrame)

    def test_polars_catch_wraps_error(self) -> None:
        """Test polars_catch wraps exceptions as PolarsError."""
        result = polars_catch(lambda: pl.DataFrame({"a": []})[0, "nonexistent"], "test operation")
        assert result.is_err()
        error = result.unwrap_err()
        assert isinstance(error, PolarsError)
        assert "test operation" in str(error)


class TestReadOperations:
    """Test read operations."""

    @patch("polars.read_csv")
    def test_read_csv_success(self, mock_read: MagicMock) -> None:
        """Test successful CSV read."""
        expected_df = pl.DataFrame({"a": [1, 2, 3]})
        mock_read.return_value = expected_df

        result = PolarsResult.read_csv("test.csv")
        assert result.is_ok()
        assert result.unwrap().equals(expected_df)
        mock_read.assert_called_once_with("test.csv")

    @patch("polars.read_csv")
    def test_read_csv_failure(self, mock_read: MagicMock) -> None:
        """Test CSV read failure."""
        mock_read.side_effect = FileNotFoundError("File not found")

        result = PolarsResult.read_csv("nonexistent.csv")
        assert result.is_err()
        error = result.unwrap_err()
        assert isinstance(error, PolarsError)
        assert "Failed to read CSV" in str(error)

    @patch("polars.read_csv")
    def test_read_csv_with_kwargs(self, mock_read: MagicMock) -> None:
        """Test CSV read with additional kwargs."""
        expected_df = pl.DataFrame({"a": [1, 2, 3]})
        mock_read.return_value = expected_df

        result = PolarsResult.read_csv("test.csv", separator=";", has_header=False)
        assert result.is_ok()
        mock_read.assert_called_once_with("test.csv", separator=";", has_header=False)

    @patch("polars.read_parquet")
    def test_read_parquet_success(self, mock_read: MagicMock) -> None:
        """Test successful Parquet read."""
        expected_df = pl.DataFrame({"a": [1, 2, 3]})
        mock_read.return_value = expected_df

        result = PolarsResult.read_parquet("test.parquet")
        assert result.is_ok()
        assert result.unwrap().equals(expected_df)

    @patch("polars.read_parquet")
    def test_read_parquet_with_path_object(self, mock_read: MagicMock) -> None:
        """Test Parquet read with Path object."""
        expected_df = pl.DataFrame({"a": [1, 2, 3]})
        mock_read.return_value = expected_df

        path = Path("test.parquet")
        result = PolarsResult.read_parquet(path)
        assert result.is_ok()

    @patch("polars.read_json")
    def test_read_json_success(self, mock_read: MagicMock) -> None:
        """Test successful JSON read."""
        expected_df = pl.DataFrame({"a": [1, 2, 3]})
        mock_read.return_value = expected_df

        result = PolarsResult.read_json("test.json")
        assert result.is_ok()
        assert result.unwrap().equals(expected_df)

    @patch("polars.read_excel")
    def test_read_excel_success(self, mock_read: MagicMock) -> None:
        """Test successful Excel read."""
        expected_df = pl.DataFrame({"a": [1, 2, 3]})
        mock_read.return_value = expected_df

        result = PolarsResult.read_excel("test.xlsx")
        assert result.is_ok()
        assert result.unwrap().equals(expected_df)


class TestScanOperations:
    """Test scan operations."""

    @patch("polars.scan_csv")
    def test_scan_csv_success(self, mock_scan: MagicMock) -> None:
        """Test successful CSV scan."""
        mock_lf = MagicMock(spec=pl.LazyFrame)
        mock_scan.return_value = mock_lf

        result = PolarsResult.scan_csv("test.csv")
        assert result.is_ok()
        assert result.unwrap() is mock_lf

    @patch("polars.scan_csv")
    def test_scan_csv_failure(self, mock_scan: MagicMock) -> None:
        """Test CSV scan failure."""
        mock_scan.side_effect = FileNotFoundError("File not found")

        result = PolarsResult.scan_csv("nonexistent.csv")
        assert result.is_err()
        assert "Failed to scan CSV" in str(result.unwrap_err())

    @patch("polars.scan_parquet")
    def test_scan_parquet_success(self, mock_scan: MagicMock) -> None:
        """Test successful Parquet scan."""
        mock_lf = MagicMock(spec=pl.LazyFrame)
        mock_scan.return_value = mock_lf

        result = PolarsResult.scan_parquet("test.parquet")
        assert result.is_ok()
        assert result.unwrap() is mock_lf


class TestDataFrameConstruction:
    """Test DataFrame construction methods."""

    def test_from_dict_success(self) -> None:
        """Test successful DataFrame creation from dict."""
        data = {"a": [1, 2, 3], "b": [4, 5, 6]}
        result = PolarsResult.from_dict(data)
        assert result.is_ok()
        df = result.unwrap()
        assert df.columns == ["a", "b"]
        assert len(df) == 3

    def test_from_dict_failure(self) -> None:
        """Test DataFrame creation from dict with mismatched lengths."""
        data = {"a": [1, 2, 3], "b": [4, 5]}  # Mismatched lengths
        result = PolarsResult.from_dict(data)
        assert result.is_err()
        assert "Failed to create DataFrame from dict" in str(result.unwrap_err())

    def test_from_records_success(self) -> None:
        """Test successful DataFrame creation from records."""
        data = [{"a": 1, "b": 4}, {"a": 2, "b": 5}, {"a": 3, "b": 6}]
        result = PolarsResult.from_records(data)
        assert result.is_ok()
        df = result.unwrap()
        assert len(df) == 3

    @patch("polars.from_arrow")
    def test_from_arrow_success(self, mock_from_arrow: MagicMock) -> None:
        """Test successful DataFrame creation from Arrow."""
        expected_df = pl.DataFrame({"a": [1, 2, 3]})
        mock_from_arrow.return_value = expected_df

        mock_table = MagicMock()
        result = PolarsResult.from_arrow(mock_table)
        assert result.is_ok()

    @patch("polars.from_pandas")
    def test_from_pandas_success(self, mock_from_pandas: MagicMock) -> None:
        """Test successful DataFrame creation from Pandas."""
        expected_df = pl.DataFrame({"a": [1, 2, 3]})
        mock_from_pandas.return_value = expected_df

        mock_df = MagicMock()
        result = PolarsResult.from_pandas(mock_df)
        assert result.is_ok()


class TestLazyFrameOperations:
    """Test LazyFrame operations."""

    def test_collect_success(self) -> None:
        """Test successful LazyFrame collection."""
        lf = pl.LazyFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        result = PolarsResult.collect(lf)
        assert result.is_ok()
        df = result.unwrap()
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 3

    def test_collect_failure(self) -> None:
        """Test LazyFrame collection failure."""
        lf = pl.LazyFrame({"a": [1, 2, 3]}).select(pl.col("nonexistent"))
        result = PolarsResult.collect(lf)
        assert result.is_err()
        assert "LazyFrame collection failed" in str(result.unwrap_err())


class TestWriteOperations:
    """Test write operations."""

    @patch.object(pl.DataFrame, "write_csv")
    def test_write_csv_success(self, mock_write: MagicMock) -> None:
        """Test successful CSV write."""
        mock_write.return_value = None
        df = pl.DataFrame({"a": [1, 2, 3]})
        result = PolarsResult.write_csv(df, "output.csv")
        assert result.is_ok()
        assert result.unwrap() is None
        mock_write.assert_called_once_with("output.csv")

    @patch.object(pl.DataFrame, "write_csv")
    def test_write_csv_failure(self, mock_write: MagicMock) -> None:
        """Test CSV write failure."""
        mock_write.side_effect = PermissionError("Permission denied")
        df = pl.DataFrame({"a": [1, 2, 3]})
        result = PolarsResult.write_csv(df, "/forbidden/output.csv")
        assert result.is_err()
        assert "Failed to write CSV" in str(result.unwrap_err())

    @patch.object(pl.DataFrame, "write_parquet")
    def test_write_parquet_success(self, mock_write: MagicMock) -> None:
        """Test successful Parquet write."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        result = PolarsResult.write_parquet(df, "output.parquet")
        assert result.is_ok()
        mock_write.assert_called_once_with("output.parquet")

    @patch.object(pl.DataFrame, "write_json")
    def test_write_json_success(self, mock_write: MagicMock) -> None:
        """Test successful JSON write."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        result = PolarsResult.write_json(df, "output.json")
        assert result.is_ok()
        mock_write.assert_called_once_with("output.json")


class TestDataFrameOperations:
    """Test DataFrame transformation operations."""

    def test_select_success(self) -> None:
        """Test successful select operation."""
        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
        result = PolarsResult.select(df, "a", "b")
        assert result.is_ok()
        selected = result.unwrap()
        assert selected.columns == ["a", "b"]

    def test_select_failure(self) -> None:
        """Test select operation with nonexistent column."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        result = PolarsResult.select(df, "nonexistent")
        assert result.is_err()
        assert "Select operation failed" in str(result.unwrap_err())

    def test_filter_success(self) -> None:
        """Test successful filter operation."""
        df = pl.DataFrame({"a": [1, 2, 3, 4, 5]})
        result = PolarsResult.filter(df, pl.col("a") > 3)
        assert result.is_ok()
        filtered = result.unwrap()
        assert len(filtered) == 2

    def test_filter_failure(self) -> None:
        """Test filter operation with invalid predicate."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        result = PolarsResult.filter(df, pl.col("nonexistent") > 0)
        assert result.is_err()
        assert "Filter operation failed" in str(result.unwrap_err())

    def test_with_columns_success(self) -> None:
        """Test successful with_columns operation."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        result = PolarsResult.with_columns(df, b=pl.col("a") * 2)
        assert result.is_ok()
        modified = result.unwrap()
        assert "b" in modified.columns

    def test_with_columns_failure(self) -> None:
        """Test with_columns operation with invalid expression."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        result = PolarsResult.with_columns(df, b=pl.col("nonexistent"))
        assert result.is_err()
        assert "with_columns operation failed" in str(result.unwrap_err())

    def test_join_success(self) -> None:
        """Test successful join operation."""
        df1 = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        df2 = pl.DataFrame({"a": [1, 2, 3], "c": [7, 8, 9]})
        result = PolarsResult.join(df1, df2, on="a")
        assert result.is_ok()
        joined = result.unwrap()
        assert "c" in joined.columns

    def test_join_failure(self) -> None:
        """Test join operation with nonexistent column."""
        df1 = pl.DataFrame({"a": [1, 2, 3]})
        df2 = pl.DataFrame({"b": [1, 2, 3]})
        result = PolarsResult.join(df1, df2, on="nonexistent")
        assert result.is_err()
        assert "Join operation failed" in str(result.unwrap_err())

    def test_group_by_success(self) -> None:
        """Test successful group_by operation."""
        df = pl.DataFrame({"a": [1, 1, 2, 2], "b": [3, 4, 5, 6]})
        result = PolarsResult.group_by(df, "a")
        assert result.is_ok()
        # group_by returns a GroupBy object, not a DataFrame
        assert result.unwrap() is not None

    @staticmethod
    def test_group_by_failure() -> None:
        """Test group_by operation with nonexistent column."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        result = PolarsResult.group_by(df, "nonexistent")
        assert result.is_err()
        assert "group_by operation failed" in str(result.unwrap_err())


class TestConvenienceAliases:
    """Test that convenience aliases work correctly."""

    @patch("polars.read_csv")
    def test_read_csv_alias(self, mock_read: MagicMock) -> None:
        """Test read_csv convenience alias."""
        expected_df = pl.DataFrame({"a": [1, 2, 3]})
        mock_read.return_value = expected_df

        result = read_csv("test.csv")
        assert result.is_ok()

    @patch("polars.read_parquet")
    def test_read_parquet_alias(self, mock_read: MagicMock) -> None:
        """Test read_parquet convenience alias."""
        expected_df = pl.DataFrame({"a": [1, 2, 3]})
        mock_read.return_value = expected_df

        result = read_parquet("test.parquet")
        assert result.is_ok()

    @patch("polars.read_json")
    def test_read_json_alias(self, mock_read: MagicMock) -> None:
        """Test read_json convenience alias."""
        expected_df = pl.DataFrame({"a": [1, 2, 3]})
        mock_read.return_value = expected_df

        result = read_json("test.json")
        assert result.is_ok()

    @patch("polars.scan_csv")
    def test_scan_csv_alias(self, mock_scan: MagicMock) -> None:
        """Test scan_csv convenience alias."""
        mock_lf = MagicMock(spec=pl.LazyFrame)
        mock_scan.return_value = mock_lf

        result = scan_csv("test.csv")
        assert result.is_ok()

    @patch("polars.scan_parquet")
    def test_scan_parquet_alias(self, mock_scan: MagicMock) -> None:
        """Test scan_parquet convenience alias."""
        mock_lf = MagicMock(spec=pl.LazyFrame)
        mock_scan.return_value = mock_lf

        result = scan_parquet("test.parquet")
        assert result.is_ok()

    def test_from_dict_alias(self) -> None:
        """Test from_dict convenience alias."""
        data = {"a": [1, 2, 3]}
        result = from_dict(data)
        assert result.is_ok()

    def test_collect_alias(self) -> None:
        """Test collect convenience alias."""
        lf = pl.LazyFrame({"a": [1, 2, 3]})
        result = collect(lf)
        assert result.is_ok()


class TestChaining:
    """Test chaining Polars operations with Result."""

    @patch("polars.read_csv")
    def test_successful_pipeline(self, mock_read: MagicMock) -> None:
        """Test successful pipeline of operations."""
        df = pl.DataFrame({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]})
        mock_read.return_value = df

        result = (
            read_csv("input.csv")
            .and_then(lambda df: PolarsResult.filter(df, pl.col("a") > 2))
            .and_then(lambda df: PolarsResult.select(df, "a"))
            .map(lambda df: len(df))
        )

        assert result.is_ok()
        assert result.unwrap() == 3

    @patch("polars.read_csv")
    def test_pipeline_short_circuits(self, mock_read: MagicMock) -> None:
        """Test that pipeline short-circuits on error."""
        mock_read.side_effect = FileNotFoundError("File not found")

        call_count = 0

        def should_not_run(df: pl.DataFrame) -> pl.DataFrame:
            nonlocal call_count
            call_count += 1
            return df

        result = (
            read_csv("nonexistent.csv")
            .and_then(lambda df: PolarsResult.filter(df, pl.col("a") > 0))
            .map(should_not_run)
        )

        assert result.is_err()
        assert call_count == 0

    @patch("polars.read_csv")
    def test_error_recovery(self, mock_read: MagicMock) -> None:
        """Test error recovery with or_else."""
        mock_read.side_effect = FileNotFoundError("File not found")
        backup_df = pl.DataFrame({"a": [1, 2, 3]})

        result = read_csv("primary.csv").or_else(lambda _: Ok(backup_df))

        assert result.is_ok()
        assert result.unwrap().equals(backup_df)
