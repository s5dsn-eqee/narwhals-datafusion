"""Tests for the `Joins` workarounds in the datafusion-workarounds skill."""

from __future__ import annotations

import pyarrow as pa
import pytest
from datafusion import SessionContext, col

import narwhals as nw

ENGINE_CHANGED = "engine behaviour changed: re-check this datafusion-workarounds entry"


def native(data: dict):
    return SessionContext().from_arrow(pa.table(data))


def collect(lf: nw.LazyFrame) -> dict:
    return lf.collect(backend="pyarrow").to_dict(as_series=False)


@pytest.mark.parametrize(
    ("how", "expected"),
    [
        ("inner", {"k": [1, 2], "v": [10, 20], "v_right": [30, 40]}),
        ("left", {"k": [1, 2], "v": [10, 20], "v_right": [30, 40]}),
        ("full", {"k": [1, 2], "v": [10, 20], "k_right": [1, 2], "v_right": [30, 40]}),
    ],
)
def test_join_suffixes_names_on_both_sides(how: str, expected: dict) -> None:
    """`dataframe.join`: a shared name is an ambiguous reference; right columns are renamed."""
    left = native({"k": [1, 2], "v": [10, 20]})
    right = native({"k": [1, 2], "v": [30, 40]})
    with pytest.raises(Exception, match="Ambiguous reference to unqualified field k"):
        left.join_on(right, col("k") == col("k")).collect()
    result = nw.from_native(left).join(nw.from_native(right), on="k", how=how).sort("k")
    assert collect(result) == expected


def test_cross_join_has_no_native_how() -> None:
    """Cross join: `join` has no `how="cross"`; `join_on` without a predicate is one."""
    left = native({"k": [1, 2]})
    right = native({"k": [3, 4]})
    with pytest.raises(Exception, match="join type cross does not exist"):
        left.join(right, on="k", how="cross")
    result = nw.from_native(left).join(nw.from_native(right), how="cross").sort("k", "k_right")
    assert collect(result) == {"k": [1, 1, 2, 2], "k_right": [3, 4, 3, 4]}


def test_join_asof_is_declared_unsupported() -> None:
    """`join_asof`: the engine has no as-of join; the plugin declares it `not_implemented()`."""
    ctx = SessionContext()
    left = ctx.from_arrow(pa.table({"t": [1, 5], "a": [1, 2]}), name="l")
    right = ctx.from_arrow(pa.table({"t": [2, 4], "b": [10, 20]}), name="r")
    with pytest.raises(Exception, match="Unsupported JOIN operator AsOf"):
        ctx.sql("SELECT * FROM l ASOF JOIN r MATCH_CONDITION (l.t >= r.t)")
    with pytest.raises(NotImplementedError, match="join_asof"):
        nw.from_native(left).join_asof(nw.from_native(right), on="t")
