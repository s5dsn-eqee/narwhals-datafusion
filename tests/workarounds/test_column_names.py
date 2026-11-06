"""Tests for the `Column names` workarounds in the datafusion-workarounds skill."""

from __future__ import annotations

import re

import pyarrow as pa
import pytest
from datafusion import SessionContext, col
from narwhals.exceptions import ColumnNotFoundError

import narwhals as nw

ENGINE_CHANGED = "engine behaviour changed: re-check this datafusion-workarounds entry"


def native(data: dict):
    return SessionContext().from_arrow(pa.table(data))


def collect(lf: nw.LazyFrame) -> dict:
    return lf.collect(backend="pyarrow").to_dict(as_series=False)


def test_col_matches_names_exactly() -> None:
    """`utils.col`: `datafusion.col` lower-cases and splits on dots; the plugin quotes."""
    df = native({"a": [1], "A": [2], "A.b": [3]})
    assert df.select(col("A")).to_pydict() == {"a": [1]}, ENGINE_CHANGED
    with pytest.raises(Exception, match=r"No field named a\.b\."):
        df.select(col("A.b")).collect()
    assert collect(nw.from_native(df).select("a", "A", "A.b")) == {"a": [1], "A": [2], "A.b": [3]}


def test_name_string_apis_get_quoted_names() -> None:
    """`utils.quote`: `unnest_columns` and `join` parse name strings; `explode` and `join` quote."""
    lists = native({"i": [1, 2], "A": [[1, 2], [3]]})
    left = native({"A": [1, 2], "v": [10, 20]})
    right = native({"A": [2, 3], "w": [200, 300]})
    with pytest.raises(Exception, match=r"No field named a\."):
        lists.unnest_columns("A").collect()
    with pytest.raises(Exception, match=r"No field named a\."):
        left.join(right, on="A").collect()
    exploded = nw.from_native(lists).explode("A").sort("i", "A")
    assert collect(exploded) == {"i": [1, 1, 2], "A": [1, 2, 3]}
    joined = nw.from_native(left).join(nw.from_native(right), on="A")
    assert collect(joined) == {"A": [2], "v": [20], "w": [200]}


@pytest.mark.parametrize(
    ("name", "identifier"),
    [("nope", "nope"), ("Nope", '"Nope"'), ('q"y', '"q""y"'), ("N.b", '"N.b"')],
)
def test_missing_column_error_names_column_as_given(name: str, identifier: str) -> None:
    """`catch_datafusion_exception`: engine errors quote the name; the plugin unquotes it."""
    df = native({"a": [1]})
    with pytest.raises(Exception, match=f"No field named {re.escape(identifier)}\\."):
        df.select(col(identifier)).collect()
    with pytest.raises(ColumnNotFoundError, match=re.escape(str([name]))):
        nw.from_native(df).select(nw.col(name))
