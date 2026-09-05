from __future__ import annotations

import importlib.util

import pyarrow as pa
import pytest
from datafusion import SessionContext

import narwhals as nw
from narwhals_datafusion import extra_functions

needs_ffi = pytest.mark.skipif(
    importlib.util.find_spec("datafusion_extra_functions_ffi") is None,
    reason="needs the `extra-functions` extra",
)


def lf(data: dict) -> nw.LazyFrame:
    return nw.from_native(SessionContext().from_arrow(pa.table(data)))


def to_dict(frame: nw.LazyFrame) -> dict:
    return frame.collect(backend="pyarrow").to_dict(as_series=False)


@pytest.mark.parametrize(
    "expr",
    [nw.col("a").mode(keep="any"), nw.col("a").skew(), nw.col("a").kurtosis()],
    ids=["mode", "skew", "kurtosis"],
)
def test_without_extra_points_at_it(monkeypatch: pytest.MonkeyPatch, expr: nw.Expr) -> None:
    monkeypatch.setattr(extra_functions, "extra_udaf", lambda _: None)  # bare install
    with pytest.raises(NotImplementedError, match=r"narwhals-datafusion\[extra-functions\]"):
        lf({"a": [1.0, 2.0, 4.0]}).select(expr)


@needs_ffi
def test_ffi_module_lists_functions() -> None:
    import datafusion_extra_functions_ffi as ffi

    names = set(ffi.list_functions())
    assert {"mode", "skewness"} <= names


@needs_ffi
def test_mode() -> None:
    result = to_dict(lf({"a": [1, 1, 2, 2, 2, None]}).select(nw.col("a").mode(keep="any")))
    assert result == {"a": [2]}


@needs_ffi
@pytest.mark.xfail(
    reason=(
        "upstream datafusion-extra-functions bug: mode's `state_fields` declares "
        "scalar columns but the accumulator serializes list state, so any "
        "multi-partition group-by fails schema validation (works with "
        "target_partitions=1)"
    ),
    strict=True,
)
def test_mode_group_by() -> None:
    data = {"g": ["x", "x", "x", "y", "y"], "a": [1, 1, 2, 3, 3]}
    result = to_dict(lf(data).group_by("g").agg(nw.col("a").mode(keep="any")).sort("g"))
    assert result == {"g": ["x", "y"], "a": [1, 3]}


@needs_ffi
def test_skew() -> None:
    result = to_dict(lf({"a": [1.0, 2.0, 4.0, 8.0]}).select(nw.col("a").skew()))
    assert result["a"][0] == pytest.approx(0.6568077345, rel=1e-6)


@needs_ffi
def test_skew_edge_cases() -> None:
    assert to_dict(lf({"a": [1.0, 2.0]}).select(nw.col("a").skew())) == {"a": [0.0]}
    result = to_dict(lf({"a": [None]}).select(nw.col("a").cast(nw.Float64).skew()))
    assert result == {"a": [None]}


@needs_ffi
def test_kurtosis() -> None:
    result = to_dict(lf({"a": [1.0, 2.0, 4.0, 8.0]}).select(nw.col("a").kurtosis()))
    assert result["a"][0] == pytest.approx(-1.0990, rel=1e-3)


@needs_ffi
def test_skew_over_partition() -> None:
    data = {"g": ["x"] * 4 + ["y"] * 4, "a": [1.0, 2.0, 4.0, 8.0, 1.0, 1.0, 1.0, 5.0]}
    result = to_dict(
        lf(data).with_columns(s=nw.col("a").skew().over("g")).sort("g", "a").select("s")
    )
    assert result["s"][0] == pytest.approx(0.6568077345, rel=1e-6)
    assert result["s"][4] == pytest.approx(1.1547005, rel=1e-5)
