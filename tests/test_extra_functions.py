from __future__ import annotations

import importlib.util
import re

import pyarrow as pa
import pytest
from datafusion import SessionContext

import narwhals as nw
from narwhals_datafusion import extra_functions

needs_extra = pytest.mark.skipif(
    importlib.util.find_spec("datafusion_extra_functions") is None,
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


@needs_extra
def test_extra_module_lists_functions() -> None:
    import datafusion_extra_functions as extra

    names = set(extra.list_functions())
    assert {"mode", "skewness", "kurtosis_pop"} <= names


@needs_extra
def test_mode() -> None:
    result = to_dict(lf({"a": [1, 1, 2, 2, 2, None]}).select(nw.col("a").mode(keep="any")))
    assert result == {"a": [2]}


@needs_extra
def test_mode_group_by() -> None:
    data = {"g": ["x", "x", "x", "y", "y"], "a": [1, 1, 2, 3, 3]}
    result = to_dict(lf(data).group_by("g").agg(nw.col("a").mode(keep="any")).sort("g"))
    assert result == {"g": ["x", "y"], "a": [1, 3]}


@needs_extra
@pytest.mark.parametrize(
    "values",
    [
        pa.array(["x", "x", "y"], type=pa.string()),
        pa.array(["x", "x", "y"], type=pa.string_view()),
        pa.array([1.5, 1.5, 2.0], type=pa.float32()),
        pa.array([1, 1, 2], type=pa.uint8()),
    ],
    ids=["utf8", "utf8_view", "float32", "uint8"],
)
def test_mode_supported_dtypes(values: pa.Array) -> None:
    result = to_dict(lf({"a": values}).select(nw.col("a").mode(keep="any")))
    assert result == {"a": [values[0].as_py()]}


@needs_extra
@pytest.mark.parametrize(
    "values",
    [
        pa.array(["x", "x", "y"], type=pa.large_string()),
        pa.array([True, True, False]),
        pa.array([1, 1, 2], type=pa.decimal128(10, 2)),
    ],
    ids=["large_utf8", "bool", "decimal"],
)
@pytest.mark.parametrize("over", [False, True], ids=["agg", "over"])
def test_mode_unsupported_dtype_raises(values: pa.Array, *, over: bool) -> None:
    mode = nw.col("a").mode(keep="any")
    frame = lf({"g": [1, 1, 2], "a": values})
    message = rf"`mode` on `{re.escape(str(values.type))}`.*DataFusion"
    with pytest.raises(NotImplementedError, match=message):
        if over:
            to_dict(frame.with_columns(m=mode.over("g")))
        else:
            to_dict(frame.select(mode))


@needs_extra
def test_skew() -> None:
    result = to_dict(lf({"a": [1.0, 2.0, 4.0, 8.0]}).select(nw.col("a").skew()))
    assert result["a"][0] == pytest.approx(0.6568077345, rel=1e-6)


@needs_extra
def test_skew_edge_cases() -> None:
    assert to_dict(lf({"a": [1.0, 2.0]}).select(nw.col("a").skew())) == {"a": [0.0]}
    result = to_dict(lf({"a": [None]}).select(nw.col("a").cast(nw.Float64).skew()))
    assert result == {"a": [None]}


@needs_extra
def test_kurtosis() -> None:
    result = to_dict(lf({"a": [1.0, 2.0, 4.0, 8.0]}).select(nw.col("a").kurtosis()))
    assert result["a"][0] == pytest.approx(-1.0990, rel=1e-3)


@needs_extra
def test_skew_over_partition() -> None:
    data = {"g": ["x"] * 4 + ["y"] * 4, "a": [1.0, 2.0, 4.0, 8.0, 1.0, 1.0, 1.0, 5.0]}
    result = to_dict(
        lf(data).with_columns(s=nw.col("a").skew().over("g")).sort("g", "a").select("s")
    )
    assert result["s"][0] == pytest.approx(0.6568077345, rel=1e-6)
    assert result["s"][4] == pytest.approx(1.1547005, rel=1e-5)
