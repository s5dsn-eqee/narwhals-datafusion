"""Tests for the `Frames` workarounds in the datafusion-workarounds skill."""

from __future__ import annotations

import io

import pyarrow as pa
import pytest
from datafusion import SessionContext, col, lit
from datafusion import functions as F
from datafusion.expr import Window
from narwhals.exceptions import DuplicateError

import narwhals as nw

ENGINE_CHANGED = "engine behaviour changed: re-check this datafusion-workarounds entry"


def native(data: dict):
    return SessionContext().from_arrow(pa.table(data))


def collect(lf: nw.LazyFrame) -> dict:
    return lf.collect(backend="pyarrow").to_dict(as_series=False)


def ascending(*names: str) -> list:
    return [col(name).sort(ascending=True, nulls_first=False) for name in names]


def test_explode_keeps_empty_and_null_lists() -> None:
    """`explode`: `unnest_columns` drops empty lists; the plugin turns them into nulls first."""
    df = native({"i": [1, 2, 3], "l": [[1, 2], [], None]})
    engine = df.unnest_columns("l", preserve_nulls=True).sort(*ascending("i", "l")).to_pydict()
    assert engine == {"i": [1, 1, 3], "l": [1, 2, None]}, ENGINE_CHANGED
    result = nw.from_native(df).explode("l").sort("i", "l", nulls_last=True)
    assert collect(result) == {"i": [1, 1, 2, 3], "l": [1, 2, None, None]}


def test_explode_multiple_columns_raises() -> None:
    """`explode`: `unnest_columns` pads mismatched lists with nulls; several columns raise."""
    df = native({"i": [1, 2], "a": [[1, 2], [3]], "b": [[10], [30]]})
    engine = df.unnest_columns("a", "b").sort(*ascending("i", "a")).to_pydict()
    assert engine == {"i": [1, 1, 2], "a": [1, 2, 3], "b": [10, None, 30]}, ENGINE_CHANGED
    with pytest.raises(NotImplementedError, match="multiple columns"):
        nw.from_native(df).explode(["a", "b"])


@pytest.mark.parametrize(
    ("values", "dtype", "expected"),
    [
        (["x"], nw.String, ["10", "x"]),
        ([1.5], nw.Float64, [10.0, 1.5]),
        ([True], nw.Int64, [10, 1]),
    ],
)
def test_unpivot_casts_values_to_supertype(values: list, dtype: type, expected: list) -> None:
    """`unpivot`: `union` reports the first input's type; values are cast to a supertype."""
    union = native({"v": [10]}).union(native({"v": ["x"]}))
    assert union.schema().field("v").type == pa.int64(), ENGINE_CHANGED
    assert union.to_arrow_table().schema.field("v").type == pa.string(), ENGINE_CHANGED
    result = nw.from_native(native({"a": [10], "b": values})).unpivot(on=["a", "b"])
    result = result.sort("variable")
    assert result.collect_schema()["value"] == dtype
    assert collect(result)["value"] == expected


def test_concat_diagonal_matches_columns_by_name() -> None:
    """`concat`: `union` is positional; `union_by_name` matches names and fills nulls."""
    first = native({"x": [1], "y": [2]})
    second = native({"z": [6], "x": [5]})
    engine = first.union(second).sort(*ascending("x")).to_pydict()
    assert engine == {"x": [1, 6], "y": [2, 5]}, ENGINE_CHANGED
    frames = [nw.from_native(first), nw.from_native(second)]
    result = nw.concat(frames, how="diagonal").sort("x")
    assert collect(result) == {"x": [1, 5], "y": [2, None], "z": [None, 6]}


def test_concat_returns_every_row_in_some_order() -> None:
    """`union` output order: nondeterministic, so only the rows are asserted, not their order."""
    frames = [nw.from_native(native({"x": [i]})) for i in range(8)]
    assert sorted(collect(nw.concat(frames))["x"]) == list(range(8))


def test_rename_into_a_taken_name_raises_duplicate() -> None:
    """`rename`: a taken name is an engine planning error; the plugin raises `DuplicateError`."""
    df = native({"a": [1], "b": [2]})
    with pytest.raises(Exception, match="would be ambiguous"):
        df.select(col("a").alias("b"), col("b"))
    with pytest.raises(DuplicateError):
        nw.from_native(df).rename({"a": "b"})


def test_top_k_plans_a_top_k_sort() -> None:
    """`top_k`: a `row_number() <= k` filter sorts every row; `sort` + `limit` plans a TopK."""
    df = native({"x": [3, 1, None, 2]})
    order = [col("x").sort(ascending=False, nulls_first=False)]
    ranked = df.select(col("x"), F.row_number().over(Window(order_by=order)).alias("r"))
    plan = ranked.filter(col("r") <= lit(2)).execution_plan().display_indent()
    assert "TopK" not in plan, ENGINE_CHANGED
    result = nw.from_native(df).top_k(2, by="x")
    assert "TopK(fetch=2)" in result.to_native().execution_plan().display_indent()
    assert sorted(collect(result)["x"]) == [2, 3]


def test_sink_parquet_rejects_file_objects(tmp_path, monkeypatch) -> None:
    """`sink_parquet`: `write_parquet` turns a buffer into a filename; the plugin raises."""
    monkeypatch.chdir(tmp_path)
    df = native({"x": [1]})
    buffer = io.BytesIO()
    df.write_parquet(buffer)
    assert buffer.getvalue() == b"", ENGINE_CHANGED
    assert [path.name for path in tmp_path.iterdir()] == [str(buffer)], ENGINE_CHANGED
    with pytest.raises(NotImplementedError, match="file-like"):
        nw.from_native(df).sink_parquet(io.BytesIO())
    assert len(list(tmp_path.iterdir())) == 1


def test_with_row_index_is_int64() -> None:
    """`with_row_index`: `row_number` minus an Int64 literal is Decimal; the plugin casts first."""
    df = native({"c": [3.0, 1.0]})
    row_number = F.row_number().over(Window(order_by=ascending("c")))
    engine = df.select((row_number - lit(1)).alias("i")).schema().field("i").type
    assert engine == pa.decimal128(20, 0), ENGINE_CHANGED
    result = nw.from_native(df).with_row_index("i", order_by="c").sort("i")
    assert result.collect_schema()["i"] == nw.Int64
    assert collect(result) == {"i": [0, 1], "c": [1.0, 3.0]}
