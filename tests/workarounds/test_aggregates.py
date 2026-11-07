"""Tests for the `Aggregates` workarounds in the datafusion-workarounds skill."""

from __future__ import annotations

import importlib.util
import math

import pyarrow as pa
import pytest
from datafusion import SessionContext, col, lit, udaf
from datafusion import functions as F
from narwhals.exceptions import InvalidOperationError

import narwhals as nw

ENGINE_CHANGED = "engine behaviour changed: re-check this datafusion-workarounds entry"

needs_extra = pytest.mark.skipif(
    importlib.util.find_spec("datafusion_extra_functions") is None,
    reason="needs the `extra-functions` extra",
)


def native(data: dict | pa.Table):
    table = data if isinstance(data, pa.Table) else pa.table(data)
    return SessionContext().from_arrow(table)


def collect(lf: nw.LazyFrame) -> dict:
    return lf.collect(backend="pyarrow").to_dict(as_series=False)


def extra(name: str):
    from datafusion_extra_functions import udaf_by_name

    return udaf(udaf_by_name(name))


def test_n_unique_counts_null() -> None:
    """`n_unique`: `count_distinct` skips nulls; `max`/`sum` over no rows are null."""
    df = native({"x": [1, None, 1]})
    engine = df.aggregate([], [F.count(col("x"), distinct=True).alias("n")]).to_pydict()
    assert engine == {"n": [1]}, ENGINE_CHANGED
    assert collect(nw.from_native(df).select(nw.col("x").n_unique())) == {"x": [2]}
    empty = native({"x": pa.array([], pa.int64())})
    aggregates = [F.max(col("x")).alias("m"), F.sum(col("x")).alias("s")]
    assert empty.aggregate([], aggregates).to_pydict() == {"m": [None], "s": [None]}, ENGINE_CHANGED
    result = nw.from_native(empty).select(u=nw.col("x").n_unique(), n=nw.col("x").null_count())
    assert collect(result) == {"u": [0], "n": [0]}


def test_literal_only_aggregate_keeps_its_node() -> None:
    """Literal-only `aggregate`: the optimizer drops the node; `count_star` keeps it."""
    df = native({"x": [1, 2]})
    with pytest.raises(Exception, match="simplify_expressions"):
        df.aggregate([], [lit(1).alias("one")]).collect()
    result = collect(nw.from_native(df).select(nw.lit(1).alias("one"), nw.len()))
    assert result == {"one": [1], "len": [2]}


@needs_extra
def test_skew_undoes_bias_adjustment() -> None:
    """Extra aggregates: the crate's `skewness` is bias-adjusted; `skew` undoes it."""
    data = {"x": [1.0, 2.0, 4.0, 8.0]}
    df = native(data)
    engine = df.aggregate([], [extra("skewness")(col("x")).alias("s")]).to_pydict()["s"][0]
    g1 = collect(nw.from_native(df).select(nw.col("x").skew()))["x"][0]
    n = len(data["x"])
    assert engine == pytest.approx(g1 * math.sqrt(n * (n - 1)) / (n - 2)), ENGINE_CHANGED
    assert g1 == pytest.approx(0.6568077345, rel=1e-6)


@needs_extra
def test_mode_rejects_unsupported_types_while_planning() -> None:
    """`mode` input types: the crate fails at execution; the plugin raises while planning."""
    df = native(pa.table({"s": pa.array(["a", "a", "b"], pa.large_string())}))
    with pytest.raises(Exception, match="Unsupported data type: LargeUtf8"):
        df.aggregate([], [extra("mode")(col("s"))]).collect()
    with pytest.raises(NotImplementedError, match="`mode` on `large_string`"):
        nw.from_native(df).select(nw.col("s").mode(keep="any"))


@needs_extra
def test_mode_does_not_count_nulls() -> None:
    """`mode` and nulls: the crate skips nulls and rejects the one-element-list trick."""
    df = native({"x": [None, None, 1]})
    engine = df.aggregate([], [extra("mode")(col("x")).alias("m")]).to_pydict()
    assert engine == {"m": [1]}, ENGINE_CHANGED
    with pytest.raises(Exception, match="Unsupported data type: List"):
        df.aggregate([], [extra("mode")(F.make_array(col("x")))]).collect()
    assert collect(nw.from_native(df).select(nw.col("x").mode(keep="any"))) == {"x": [1]}


def test_median_rejects_non_numeric() -> None:
    """`median`, `quantile`: engine `median` has numeric signatures only."""
    df = native({"s": ["a", "b"]})
    with pytest.raises(Exception, match="'median' failed to match"):
        df.aggregate([], [F.median(col("s"))]).collect()
    with pytest.raises(InvalidOperationError, match="`median` operation not supported"):
        collect(nw.from_native(df).select(nw.col("s").median()))


def test_quantile_is_linear_percentile_cont() -> None:
    """`median`, `quantile`: `percentile_cont` is exact, linear, NaN last."""
    df = native({"x": [1.0, 3.0, 4.0, float("nan")]})
    engine = df.aggregate([], [F.percentile_cont(col("x"), 0.25).alias("q")]).to_pydict()
    assert engine == {"q": [2.5]}, ENGINE_CHANGED
    lf = nw.from_native(df)
    assert collect(lf.select(nw.col("x").quantile(0.25, "linear"))) == {"x": [2.5]}
    with pytest.raises(NotImplementedError, match="interpolation='nearest'"):
        lf.select(nw.col("x").quantile(0.25, "nearest"))


def test_no_product_aggregate() -> None:
    """`product`: no aggregate, so `cum_prod` is declared unsupported."""
    assert not hasattr(F, "product"), ENGINE_CHANGED
    with pytest.raises(NotImplementedError, match="cum_prod"):
        nw.from_native(native({"x": [1, 2]})).select(nw.col("x").cum_prod())


def test_list_sum_is_float_only() -> None:
    """List aggregates: `array_sum` is Float64 and null when empty; `list.sum` is float-only."""
    big = 2**53 + 1
    ints = native(pa.table({"l": pa.array([[big, 1]], pa.list_(pa.int64()))}))
    engine = ints.select(F.array_sum(col("l")).alias("s")).to_arrow_table().column("s")
    assert engine.type == pa.float64(), ENGINE_CHANGED
    assert engine.to_pylist() != [big + 1], ENGINE_CHANGED
    with pytest.raises(NotImplementedError, match="`list.sum` on `list<item: int64>`"):
        collect(nw.from_native(ints).select(nw.col("l").list.sum()))

    floats = native(pa.table({"l": pa.array([[1.5, 2.5], [], None], pa.list_(pa.float64()))}))
    engine = floats.select(F.array_sum(col("l")).alias("s")).to_pydict()
    assert engine == {"s": [4.0, None, None]}, ENGINE_CHANGED
    assert collect(nw.from_native(floats).select(nw.col("l").list.sum())) == {"l": [4.0, 0.0, None]}


def test_list_mean_and_median() -> None:
    """List aggregates: `array_avg` matches `list.mean`; no `array_median`."""
    df = native(pa.table({"l": pa.array([[1, 2, 3], [], None], pa.list_(pa.int64()))}))
    engine = df.select(F.array_avg(col("l")).alias("m")).to_pydict()
    assert engine == {"m": [2.0, None, None]}
    assert not hasattr(F, "array_median"), ENGINE_CHANGED
    lf = nw.from_native(df)
    assert collect(lf.select(nw.col("l").list.mean())) == {"l": [2.0, None, None]}
    with pytest.raises(NotImplementedError, match="median"):
        lf.select(nw.col("l").list.median())
