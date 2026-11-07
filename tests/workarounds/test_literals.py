"""Tests for the `Literals and operators` workarounds in the datafusion-workarounds skill."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pyarrow as pa
import pytest
from datafusion import Expr, SessionContext, col, lit
from datafusion import functions as F
from narwhals.exceptions import InvalidOperationError

import narwhals as nw

ENGINE_CHANGED = "engine behaviour changed: re-check this datafusion-workarounds entry"


def native(data: dict):
    return SessionContext().from_arrow(pa.table(data))


def collect(lf: nw.LazyFrame) -> dict:
    return lf.collect(backend="pyarrow").to_dict(as_series=False)


def engine_divide(df) -> list:
    return df.select((col("a") / col("b")).alias("r")).to_pydict()["r"]


@pytest.mark.parametrize(
    ("engine", "pinned", "expr", "expected"),
    [
        pytest.param(lambda _: hasattr(Expr, "__neg__"), False, -nw.col("a"), [-7, 7], id="neg"),
        pytest.param(
            lambda _: hasattr(Expr, "__pow__"),
            False,
            nw.col("a") ** nw.col("b"),
            [49, 49],
            id="pow",
        ),
        pytest.param(engine_divide, [3, -3], nw.col("a") / nw.col("b"), [3.5, -3.5], id="truediv"),
        pytest.param(engine_divide, [3, -3], nw.col("a") // nw.col("b"), [3, -4], id="floordiv"),
    ],
)
def test_arithmetic_follows_python(engine, pinned, expr: nw.Expr, expected: list) -> None:
    """`__neg__`, `__pow__`, `__truediv__`, `_floordiv`: no `-x` or `**`, integer `/` truncates."""
    df = native({"a": [7, -7], "b": [2, 2]})
    assert engine(df) == pinned, ENGINE_CHANGED
    assert collect(nw.from_native(df).select(r=expr)) == {"r": expected}


def test_neg_keeps_the_input_type() -> None:
    """`__neg__`: `-1 * x` widens int8 and rejects durations; a typed `0 - x` wraps unsigned."""
    second = dt.timedelta(seconds=1)
    df = native(
        {
            "i": pa.array([1, -2], pa.int8()),
            "d": pa.array([second, second], pa.duration("us")),
            "u": pa.array([1, 2], pa.uint8()),
        }
    )
    widened = df.select((lit(-1) * col("i")).alias("r")).schema().field("r").type
    assert widened == pa.int64(), ENGINE_CHANGED
    with pytest.raises(Exception, match=r"Cannot coerce arithmetic expression Int64 \* Duration"):
        df.select(lit(-1) * col("d"))
    wrapped = df.select((lit(0).cast(pa.uint8()) - col("u")).alias("r")).to_pydict()
    assert wrapped == {"r": [255, 254]}, ENGINE_CHANGED
    lf = nw.from_native(df)
    result = lf.select(-nw.col("i"), -nw.col("d"))
    assert result.collect_schema() == {"i": nw.Int8(), "d": nw.Duration("us")}
    assert collect(result) == {"i": [-1, 2], "d": [-second, -second]}
    with pytest.raises(InvalidOperationError, match="unsigned"):
        lf.select(-nw.col("u"))


def test_mod_takes_divisor_sign_and_nulls_zero() -> None:
    """`floor_mod`: `%` keeps the dividend's sign, raises on zero; `((a % b) + b) % b` wraps."""
    signs = native({"a": [-7, 7], "b": [3, -3]})
    zero = native({"a": [5], "b": [0]})
    narrow = native({"a": pa.array([99], pa.int8()), "b": pa.array([100], pa.int8())})
    a, b = col("a"), col("b")
    assert signs.select((a % b).alias("r")).to_pydict() == {"r": [-1, 1]}, ENGINE_CHANGED
    with pytest.raises(Exception, match="Divide by zero"):
        zero.select((a % b).alias("r")).collect()
    assert narrow.select((((a % b) + b) % b).alias("r")).to_pydict() == {"r": [-57]}, ENGINE_CHANGED
    mod = nw.col("a") % nw.col("b")
    assert collect(nw.from_native(signs).select(r=mod)) == {"r": [2, -2]}
    assert collect(nw.from_native(zero).select(r=mod)) == {"r": [None]}
    assert collect(nw.from_native(narrow).select(r=mod)) == {"r": [99]}


def test_list_contains_matches_null_and_coerces_decimal() -> None:
    """`list.contains`: `array_has` is null for a null item and rejects a narrower Decimal."""
    df = native(
        {
            "l": pa.array([[1, None], [2], [], None], pa.list_(pa.int64())),
            "d": pa.array(
                [[Decimal("1.50")], [Decimal("2.00")], [], None], pa.list_(pa.decimal128(38, 2))
            ),
        }
    )
    engine = df.select(F.array_has(col("l"), lit(None)).alias("r")).to_pydict()
    assert engine == {"r": [None, None, None, None]}, ENGINE_CHANGED
    with pytest.raises(Exception, match="Failed to coerce arguments"):
        df.select(F.array_has(col("d"), lit(Decimal("1.5")))).collect()
    result = nw.from_native(df).select(
        n=nw.col("l").list.contains(None), d=nw.col("d").list.contains(Decimal("1.5"))
    )
    assert collect(result) == {"n": [True, False, False, None], "d": [True, False, False, None]}


def test_scalar_arguments_need_lit() -> None:
    """Scalar arguments: some functions reject raw Python values; `lag` takes only a raw int."""
    df = native({"x": [None, 2, 3]})
    with pytest.raises(AttributeError):
        df.select(F.coalesce(col("x"), 0)).collect()
    with pytest.raises(TypeError, match="cannot be interpreted as an integer"):
        df.select(F.lag(col("x"), lit(1))).collect()
    lf = nw.from_native(df)
    assert collect(lf.select(nw.col("x").fill_null(0))) == {"x": [0, 2, 3]}
    shifted = lf.with_columns(s=nw.col("x").shift(1).over(order_by="x")).sort("x", nulls_last=True)
    assert collect(shifted.select("s")) == {"s": [None, 2, None]}
