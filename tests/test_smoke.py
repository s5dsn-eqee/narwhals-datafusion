from __future__ import annotations

import datetime as dt

import narwhals as nw
import pyarrow as pa
import pytest
from datafusion import SessionContext

DATA = {
    "a": [1, 2, None, 4],
    "b": ["x", "y", "z", "x"],
    "c": [1.5, 2.5, 3.5, 0.5],
}


def df_native():
    return SessionContext().from_arrow(pa.table(DATA))


def to_dict(lf: nw.LazyFrame) -> dict:
    return lf.collect(backend="pyarrow").to_dict(as_series=False)


def test_from_native_roundtrip() -> None:
    lf = nw.from_native(df_native())
    assert isinstance(lf, nw.LazyFrame)
    assert lf.columns == ["a", "b", "c"]
    assert to_dict(lf) == DATA


def test_schema() -> None:
    lf = nw.from_native(df_native())
    schema = lf.collect_schema()
    assert schema["a"] == nw.Int64
    assert schema["b"] == nw.String
    assert schema["c"] == nw.Float64


def test_select_exprs() -> None:
    lf = nw.from_native(df_native())
    result = to_dict(lf.select((nw.col("a") + 1).alias("a1"), nw.col("c") * 2))
    assert result == {"a1": [2, 3, None, 5], "c": [3.0, 5.0, 7.0, 1.0]}


def test_filter_and_sort() -> None:
    lf = nw.from_native(df_native())
    result = to_dict(lf.filter(nw.col("a") > 1).sort("a", descending=True))
    assert result["a"] == [4, 2]


def test_aggregation() -> None:
    lf = nw.from_native(df_native())
    result = to_dict(
        lf.select(nw.col("a").sum(), nw.col("c").mean().alias("m"), nw.col("b").n_unique())
    )
    assert result == {"a": [7], "m": [2.0], "b": [3]}


def test_group_by() -> None:
    lf = nw.from_native(df_native())
    result = to_dict(
        lf.group_by("b").agg(nw.col("a").sum(), nw.col("c").max().alias("cmax")).sort("b")
    )
    assert result == {"b": ["x", "y", "z"], "a": [5, 2, 0], "cmax": [1.5, 2.5, 3.5]}


def test_with_columns_broadcast_agg() -> None:
    lf = nw.from_native(df_native())
    result = to_dict(lf.with_columns(total=nw.col("a").sum()).sort("c"))
    assert result["total"] == [7, 7, 7, 7]


def test_over_partition() -> None:
    lf = nw.from_native(df_native())
    result = to_dict(lf.with_columns(part_sum=nw.col("a").sum().over("b")).sort("c"))
    assert result["part_sum"] == [5, 5, 2, 0]


def test_cum_sum_over_order() -> None:
    lf = nw.from_native(df_native())
    result = to_dict(lf.with_columns(cs=nw.col("a").cum_sum().over(order_by="c")).sort("c"))
    assert result["cs"] == [4, 5, 7, None]


def test_when_then_otherwise() -> None:
    lf = nw.from_native(df_native())
    result = to_dict(
        lf.select(
            nw.when(nw.col("a") > 1).then(nw.lit("big")).otherwise(nw.lit("small")).alias("w")
        )
    )
    assert result == {"w": ["small", "big", "small", "big"]}


def test_join_suffix() -> None:
    left = nw.from_native(df_native())
    right = nw.from_native(
        SessionContext().from_arrow(pa.table({"b": ["x", "y"], "c": [10.0, 20.0]}))
    )
    result = to_dict(left.join(right, on="b", how="inner").sort("a"))
    assert result == {
        "a": [1, 2, 4],
        "b": ["x", "y", "x"],
        "c": [1.5, 2.5, 0.5],
        "c_right": [10.0, 20.0, 10.0],
    }


def test_join_full() -> None:
    left = nw.from_native(SessionContext().from_arrow(pa.table({"k": [1, 2], "v": [10, 20]})))
    right = nw.from_native(SessionContext().from_arrow(pa.table({"k": [2, 3], "w": [200, 300]})))
    result = to_dict(left.join(right, on="k", how="full").sort("v", nulls_last=True))
    assert result == {
        "k": [1, 2, None],
        "v": [10, 20, None],
        "k_right": [None, 2, 3],
        "w": [None, 200, 300],
    }


def test_cross_join() -> None:
    left = nw.from_native(SessionContext().from_arrow(pa.table({"x": [1, 2]})))
    right = nw.from_native(SessionContext().from_arrow(pa.table({"y": [3, 4]})))
    result = to_dict(left.join(right, how="cross").sort("x", "y"))
    assert result == {"x": [1, 1, 2, 2], "y": [3, 4, 3, 4]}


def test_semi_anti_join() -> None:
    left = nw.from_native(df_native())
    right = nw.from_native(SessionContext().from_arrow(pa.table({"b": ["x"]})))
    semi = to_dict(left.join(right, on="b", how="semi").sort("c"))
    assert semi["b"] == ["x", "x"]
    anti = to_dict(left.join(right, on="b", how="anti").sort("c"))
    assert anti["b"] == ["y", "z"]


def test_unique() -> None:
    lf = nw.from_native(df_native())
    result = to_dict(lf.unique(subset=["b"], keep="any").sort("b"))
    assert result["b"] == ["x", "y", "z"]


def test_unique_keep_none_with_order_by() -> None:
    # Regression: the group-size count window must not inherit `order_by`,
    # which would turn it into a running count and keep one row per group.
    native = SessionContext().from_arrow(pa.table({"g": ["a", "a", "b"], "i": [1, 2, 3]}))
    lf = nw.from_native(native)
    result = to_dict(lf.unique(subset=["g"], keep="none", order_by="i").sort("i"))
    assert result == {"g": ["b"], "i": [3]}


def test_sink_parquet(tmp_path) -> None:
    import io

    lf = nw.from_native(df_native())
    target = tmp_path / "out.parquet"
    lf.sink_parquet(target)
    assert to_dict(nw.scan_parquet(target, backend="pyarrow").sort("c"))["b"] == [
        "x",
        "x",
        "y",
        "z",
    ]
    # Regression: a buffer must raise instead of writing to a repr-named file.
    with pytest.raises(NotImplementedError, match="file-like"):
        lf.sink_parquet(io.BytesIO())


def test_mean_horizontal_all_null_row() -> None:
    # Regression: an all-null row must yield null, not NaN (0.0/0).
    native = SessionContext().from_arrow(pa.table({"x": [1.0, None], "y": [2.0, None]}))
    lf = nw.from_native(native)
    result = to_dict(lf.select(m=nw.mean_horizontal("x", "y")))
    assert result == {"m": [1.5, None]}


def test_str_namespace() -> None:
    lf = nw.from_native(df_native())
    result = to_dict(
        lf.select(
            up=nw.col("b").str.to_uppercase(),
            padded=nw.col("b").str.pad_start(3, "_"),
        )
    )
    assert result == {"up": ["X", "Y", "Z", "X"], "padded": ["__x", "__y", "__z", "__x"]}


def test_dt_namespace() -> None:
    native = SessionContext().from_arrow(
        pa.table({"t": pa.array([dt.datetime(2024, 3, 5, 10, 30, 20)], type=pa.timestamp("us"))})
    )
    lf = nw.from_native(native)
    result = to_dict(
        lf.select(
            y=nw.col("t").dt.year(),
            m=nw.col("t").dt.month(),
            wd=nw.col("t").dt.weekday(),
            s=nw.col("t").dt.to_string("%Y-%m-%d"),
        )
    )
    assert result == {"y": [2024], "m": [3], "wd": [2], "s": ["2024-03-05"]}


def test_horizontal_fns() -> None:
    lf = nw.from_native(df_native())
    result = to_dict(
        lf.select(
            nw.col("c"),
            mx=nw.max_horizontal("a", "c"),
            sm=nw.sum_horizontal("a", "c"),
            mean=nw.mean_horizontal("a", "c"),
        ).sort("c")
    )
    # rows ordered by c: (4, 0.5), (1, 1.5), (2, 2.5), (None, 3.5)
    assert result["mx"] == [4.0, 1.5, 2.5, 3.5]
    assert result["sm"] == [4.5, 2.5, 4.5, 3.5]
    assert result["mean"] == [2.25, 1.25, 2.25, 3.5]


def test_concat_vertical_and_diagonal() -> None:
    a = nw.from_native(SessionContext().from_arrow(pa.table({"x": [1], "y": [2]})))
    b = nw.from_native(SessionContext().from_arrow(pa.table({"y": [3], "x": [4]})))
    vertical = to_dict(nw.concat([a, b], how="vertical").sort("x"))
    assert vertical == {"x": [1, 4], "y": [2, 3]}
    c = nw.from_native(SessionContext().from_arrow(pa.table({"x": [5], "z": [6]})))
    diagonal = to_dict(nw.concat([a, c], how="diagonal").sort("x"))
    assert diagonal == {"x": [1, 5], "y": [2, None], "z": [None, 6]}


def test_explode() -> None:
    native = SessionContext().from_arrow(pa.table({"i": [1, 2, 3], "l": [[1, 2], [], None]}))
    lf = nw.from_native(native)
    result = to_dict(lf.explode("l").sort("i", "l", nulls_last=True))
    assert result == {"i": [1, 1, 2, 3], "l": [1, 2, None, None]}


def test_unpivot() -> None:
    native = SessionContext().from_arrow(pa.table({"id": [1], "a": [10], "b": [20]}))
    lf = nw.from_native(native)
    result = to_dict(lf.unpivot(on=["a", "b"], index=["id"]).sort("variable"))
    assert result == {"id": [1, 1], "variable": ["a", "b"], "value": [10, 20]}


def test_with_row_index() -> None:
    lf = nw.from_native(df_native())
    result = to_dict(lf.with_row_index("idx", order_by="c").sort("idx"))
    assert result["idx"] == [0, 1, 2, 3]


def test_fill_null_strategy() -> None:
    native = SessionContext().from_arrow(pa.table({"i": [1, 2, 3, 4], "v": [1.0, None, None, 4.0]}))
    lf = nw.from_native(native)
    result = to_dict(
        lf.with_columns(f=nw.col("v").fill_null(strategy="forward").over(order_by="i")).sort("i")
    )
    assert result["f"] == [1.0, 1.0, 1.0, 4.0]


def test_replace_strict() -> None:
    lf = nw.from_native(df_native())
    result = to_dict(
        lf.select(
            nw.col("b").replace_strict(
                ["x", "y", "z"], [1, 2, 3], default=None, return_dtype=nw.Int64
            )
        )
    )
    assert result == {"b": [1, 2, 3, 1]}


def test_struct_and_list_namespaces() -> None:
    lf = nw.from_native(df_native())
    result = to_dict(
        lf.select(s=nw.struct(nw.col("a"), nw.col("b"))).select(nw.col("s").struct.field("a"))
    )
    assert result == {"a": [1, 2, None, 4]}

    native = SessionContext().from_arrow(pa.table({"l": [[3, 1], [5]]}))
    result = to_dict(
        nw.from_native(native)
        .select(
            ln=nw.col("l").list.len(),
            mx=nw.col("l").list.max(),
        )
        .sort("ln")
    )
    assert result == {"ln": [1, 2], "mx": [5, 3]}


def test_arrow_pycapsule_handoff() -> None:
    # A narwhals eager frame can hop into DataFusion zero-copy via Arrow.
    pytest.importorskip("polars")
    import polars as pl

    df = nw.from_native(pl.DataFrame({"a": [1, 2]}))
    native = SessionContext().from_arrow(df.to_native())
    lf = nw.from_native(native)
    assert to_dict(lf.select(nw.col("a").sum())) == {"a": [3]}
