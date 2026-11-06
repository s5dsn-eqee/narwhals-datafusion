"""Tests for the `Windows` workarounds in the datafusion-workarounds skill."""

from __future__ import annotations

import pyarrow as pa
import pytest
from datafusion import SessionContext, col, lit
from datafusion import functions as F
from datafusion.expr import Window, WindowFrame

import narwhals as nw

ENGINE_CHANGED = "engine behaviour changed: re-check this datafusion-workarounds entry"

DATA = {"g": [1, 1, 1, 2, 2], "i": [0, 1, 2, 3, 4], "x": [None, 3, 1, 5, None]}


def native(data: dict = DATA):
    return SessionContext().from_arrow(pa.table(data))


def collect(lf: nw.LazyFrame) -> dict:
    return lf.collect(backend="pyarrow").to_dict(as_series=False)


def by_i(df, expr) -> list:
    return df.select(col("i"), expr.alias("r")).sort(col("i").sort()).to_pydict()["r"]


def test_order_inside_windowed_aggregate_moves_to_window() -> None:
    """Order inside a windowed aggregate raises; `first`/`last` order the window instead."""
    df = native()
    first_ordered = F.first_value(col("x"), order_by=[col("i").sort()])
    with pytest.raises(Exception, match="Aggregate order_by is not supported"):
        by_i(df, first_ordered.over(Window(partition_by=[col("g")])))
    lf = nw.from_native(df)
    result = collect(
        lf.with_columns(
            f=nw.col("x").first().over("g", order_by="i"),
            l=nw.col("x").last().over("g", order_by="i"),
        )
        .sort("i")
        .select("f", "l")
    )
    assert result == {"f": [None, None, None, 5, 5], "l": [1, 1, 1, None, None]}


def test_ordered_window_frame_is_cumulative() -> None:
    """With `order_by` the default frame is cumulative; whole-partition counts drop the order."""
    df = native()
    ordered = Window(partition_by=[col("g")], order_by=[col("i").sort()])
    assert by_i(df, F.count(col("i")).over(ordered)) == [1, 2, 3, 1, 2], ENGINE_CHANGED
    lf = nw.from_native(native({"k": [1, 1, 2], "i": [0, 1, 2]}))
    assert collect(lf.unique(["k"], keep="none", order_by="i")) == {"k": [2], "i": [2]}


def test_bounded_first_last_value_frames_raise() -> None:
    """Bounded frames need `retract_batch`: `fill_null` with `limit` raises, unlimited works."""
    df = native()
    bounded = Window(
        partition_by=[col("g")],
        order_by=[col("i").sort()],
        window_frame=WindowFrame("rows", 1, 0),
    )
    with pytest.raises(Exception, match="retract_batch"):
        by_i(df, F.last_value(col("x")).over(bounded))
    lf = nw.from_native(df)
    with pytest.raises(NotImplementedError, match="`fill_null` with a `limit`"):
        lf.with_columns(nw.col("x").fill_null(strategy="forward", limit=1).over(order_by="i"))
    filled = lf.with_columns(nw.col("x").fill_null(strategy="forward").over(order_by="i"))
    assert collect(filled.sort("i").select("x")) == {"x": [None, 3, 1, 5, 5]}


def test_only_aggregates_go_inside_over() -> None:
    """Only aggregate and window functions in `over`: window each aggregate, combine outside."""
    df = native()
    with pytest.raises(Exception, match="Using BinaryExpr with `over` is not allowed"):
        by_i(df, (F.sum(col("x")) + lit(1)).over(Window(partition_by=[col("g")])))
    lf = nw.from_native(df)
    result = collect(
        lf.with_columns(
            s=(nw.col("x").sum() + 1).over("g"),
            m=nw.mean_horizontal(nw.col("x").sum(), nw.col("x").max()).over("g"),
        )
        .sort("i")
        .select("s", "m")
    )
    assert result == {"s": [5, 5, 5, 6, 6], "m": [3.5, 3.5, 3.5, 5.0, 5.0]}


def test_n_unique_over_relies_on_windowed_distinct() -> None:
    """`n_unique` over a window: `DISTINCT` inside a windowed `count` is honoured since 55."""
    df = native()
    distinct = F.count(col("x"), distinct=True).over(Window(partition_by=[col("g")]))
    assert by_i(df, distinct) == [2, 2, 2, 1, 1], ENGINE_CHANGED
    result = collect(
        nw.from_native(df).with_columns(n=nw.col("x").n_unique().over("g")).sort("i").select("n")
    )
    assert result == {"n": [3, 3, 3, 2, 2]}


def test_sort_null_placement_is_explicit() -> None:
    """Sort keys pass both flags: `Expr.sort()` defaults to nulls last since datafusion 55."""
    df = native()
    engine = df.sort(col("x").sort()).to_pydict()["x"]
    assert engine == [1, 3, 5, None, None], ENGINE_CHANGED
    lf = nw.from_native(df)
    assert collect(lf.sort("x").select("x")) == {"x": [None, None, 1, 3, 5]}
    assert collect(lf.sort("x", nulls_last=True).select("x")) == {"x": [1, 3, 5, None, None]}
