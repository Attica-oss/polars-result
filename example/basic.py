"""Basic example of using the PolarsResult library"""

import polars as pl

from polars_result import print_dataframe
from polars_result.exceptions import PolarsResultError
from polars_result.result import Ok, Result


def basic() -> Result[pl.DataFrame, PolarsResultError]:
    """Basic example of using the PolarsResult library"""
    df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    return Ok(df)


if __name__ == "__main__":
    data = basic()
    print_dataframe(data.unwrap())
