"""The ``narwhals.stable.v1`` surface a consumer such as pandera drives."""

from __future__ import annotations

import datetime as dt

import narwhals.stable.v1 as nw_v1
import pyarrow as pa
import pytest
from datafusion import SessionContext

import narwhals as nw

DATA = {
    "a": [1, 2, None, 4],
    "b": ["x", "y", "z", "x"],
    "c": [1.5, 2.5, float("nan"), 0.5],
}


def df_native():
    return SessionContext().from_arrow(pa.table(DATA))


def to_dict(lf: nw_v1.LazyFrame) -> dict:
    return lf.collect().to_dict(as_series=False)


def test_from_native_and_back() -> None:
    lf = nw_v1.from_native(df_native(), eager_or_interchange_only=False)
    assert isinstance(lf, nw_v1.LazyFrame)
    assert lf.lazy() is lf
    assert lf.implementation is nw_v1.Implementation.UNKNOWN
    assert nw_v1.dependencies.is_narwhals_lazyframe(lf)
    assert nw_v1.get_native_namespace(lf).__name__ == "datafusion"
    assert type(nw_v1.to_native(lf, pass_through=True)).__module__.startswith("datafusion")
    with pytest.raises(TypeError, match="eager_only"):
        nw_v1.from_native(df_native(), eager_only=True)


def test_collect_is_v1_dataframe() -> None:
    df = nw_v1.from_native(df_native()).collect()
    assert isinstance(df, nw_v1.DataFrame)
    assert df.implementation is nw_v1.Implementation.PYARROW
    assert df.columns == ["a", "b", "c"]


def test_schema_uses_v1_dtypes() -> None:
    native = SessionContext().from_arrow(
        pa.table({"t": pa.array([dt.datetime(2024, 3, 5)], type=pa.timestamp("us"))})
    )
    schema = nw_v1.from_native(native).collect_schema()
    assert schema.names() == ["t"]
    assert isinstance(schema["t"], nw_v1.Datetime)
    assert schema["t"] == nw_v1.Datetime("us")
    casted = nw_v1.from_native(native).select(nw_v1.col("t").cast(nw_v1.Datetime("ms")))
    assert casted.collect_schema()["t"] == nw_v1.Datetime("ms")


def test_validation_shaped_query() -> None:
    lf = nw_v1.from_native(df_native())
    checks = lf.select(
        nulls=nw_v1.col("a").is_null().any(),
        nans=nw_v1.col("c").is_nan().any(),
        between=nw_v1.col("c").is_between(0, 2, closed="both").fill_null(False),
        allowed=nw_v1.col("b").is_in(["x", "y"]),
        prefix=nw_v1.col("b").str.starts_with("x"),
        ln=nw_v1.col("b").str.len_chars(),
        n=nw_v1.len(),
    ).sort("between", "allowed")
    assert to_dict(checks) == {
        "nulls": [True] * 4,
        "nans": [True] * 4,
        "between": [False, False, True, True],
        "allowed": [False, True, True, True],
        "prefix": [False, False, True, True],
        "ln": [1, 1, 1, 1],
        "n": [4] * 4,
    }
    failures = lf.filter(~nw_v1.all_horizontal(nw_v1.col("a") > 1, nw_v1.col("b") != "z"))
    assert to_dict(failures.select("b").sort("b")) == {"b": ["x", "z"]}


def test_group_by_and_concat() -> None:
    lf = nw_v1.from_native(df_native())
    counts = lf.group_by(nw_v1.col("b")).agg(nw_v1.len().alias("n")).sort("b")
    assert to_dict(counts) == {"b": ["x", "y", "z"], "n": [2, 1, 1]}
    both = nw_v1.concat([counts, counts.drop("n").with_columns(n=nw_v1.lit(0))]).sort("b", "n")
    assert both.collect().shape == (6, 2)
    assert to_dict(both.head(2)) == {"b": ["x", "x"], "n": [0, 2]}


def test_narwhalify_returns_native() -> None:
    @nw_v1.narwhalify
    def rename(frame):
        return frame.rename({"a": "z"})

    result = rename(df_native())
    assert result.schema().names == ["z", "b", "c"]


def test_with_row_index_needs_order_by() -> None:
    lf = nw_v1.from_native(df_native())
    with pytest.raises(TypeError, match="order_by"):
        lf.with_row_index("idx")


def test_failure_case_labels() -> None:
    """``_build_lazy_failure_case``: one label column per failing row plus constant columns."""
    lf = nw_v1.from_native(df_native()).filter(nw_v1.col("a") > 1)
    parts = [
        nw_v1.concat_str(nw_v1.lit(f"{c}="), nw_v1.col(c).cast(nw_v1.String)) for c in ("a", "b")
    ]
    enriched = lf.select(nw_v1.concat_str(*parts, separator=", ").alias("failure_case"))
    enriched = enriched.with_columns(
        nw_v1.lit("DataFrameSchema").alias("schema_context"),
        nw_v1.lit(None).alias("column"),
        nw_v1.lit("greater_than").alias("check"),
    ).sort("failure_case")
    assert to_dict(enriched) == {
        "failure_case": ["a=2, b=y", "a=4, b=x"],
        "schema_context": ["DataFrameSchema"] * 2,
        "column": [None] * 2,
        "check": ["greater_than"] * 2,
    }


def test_duplicate_rows_query() -> None:
    """``unique`` check: group, count, keep counts above one, drop the counter."""
    lf = nw_v1.from_native(df_native())
    grouped = lf.select(["b"]).group_by(nw_v1.col("b")).agg(nw_v1.len().alias("_count"))
    dups = grouped.filter(nw_v1.col("_count") > 1).drop("_count")
    native = nw_v1.to_native(dups.collect())
    assert len(native) == 1
    assert to_dict(dups) == {"b": ["x"]}


def test_nullability_pipeline() -> None:
    """``check_nullable``: null-or-NaN indicator, scalar ``any``, then the failing rows."""
    lf = nw_v1.from_native(df_native())
    key = "check_output"
    null_expr = nw_v1.col("c").is_null() | nw_v1.col("c").is_nan()
    combined = lf.with_columns(null_expr.alias(key))
    has_nulls = combined.select(nw_v1.col(key).any()).collect()
    assert bool(has_nulls[key][0]) is True
    failures = combined.filter(nw_v1.col(key)).select("c")
    assert to_dict(failures)["c"][0] != to_dict(failures)["c"][0]  # NaN


def test_ignore_na_pipeline() -> None:
    """Check output with ``ignore_na``: null counts as pass, else null is a failure."""
    lf = nw_v1.from_native(df_native())
    key = "check_output"
    checked = lf.with_columns((nw_v1.col("a") > 1).alias(key))
    na_pass = nw_v1.col(key) | nw_v1.col(key).is_null() | nw_v1.col("a").is_null()
    passing = checked.with_columns(na_pass.alias(key)).filter(~nw_v1.col(key)).select("a")
    assert to_dict(passing) == {"a": [1]}
    strict = checked.with_columns(nw_v1.col(key).fill_null(False))
    failing = strict.filter(~nw_v1.col(key)).select("a").sort("a", nulls_last=True)
    assert to_dict(failing) == {"a": [1, None]}


def test_nullable_int32_literal() -> None:
    """``_lit_nullable_int32`` and ``add_missing_columns``: typed literals, alone and alongside."""
    lf = nw_v1.from_native(df_native())
    alone = lf.select(nw_v1.lit(None).cast(nw_v1.Int32).alias("missing"))
    assert alone.collect_schema()["missing"] == nw_v1.Int32
    assert to_dict(alone) == {"missing": [None]}
    added = lf.with_columns(
        nw_v1.lit(5).cast(nw_v1.Int32).alias("filled"),
        nw_v1.col("a").fill_null(0).alias("a"),
    )
    assert added.collect_schema()["filled"] == nw_v1.Int32
    assert to_dict(added.sort("a"))["a"] == [0, 1, 2, 4]


def test_concat_failure_cases_unique() -> None:
    """``_concat_failure_cases`` on lazy items: ``nw.concat`` then ``unique``."""
    lf = nw_v1.from_native(df_native()).select("b")
    items = [lf.filter(nw_v1.col("b") == "x"), lf.filter(nw_v1.col("b") != "z")]
    combined = nw_v1.concat(items).unique().sort("b")
    assert isinstance(combined, nw_v1.LazyFrame)
    assert to_dict(combined) == {"b": ["x", "y"]}


def test_main_and_v1_dtypes_compare_equal() -> None:
    schema = nw_v1.from_native(df_native()).collect_schema()
    assert schema["a"] == nw.Int64
    assert schema["a"] == nw_v1.Int64
