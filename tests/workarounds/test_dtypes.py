"""Tests for the `Dtypes and time` workarounds in the datafusion-workarounds skill."""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest
from datafusion import SessionContext, col, lit
from datafusion import functions as F
from narwhals._arrow.utils import narwhals_to_native_dtype as arrow_narwhals_to_native_dtype
from narwhals._utils import Version
from narwhals.exceptions import InvalidOperationError

import narwhals as nw

ENGINE_CHANGED = "engine behaviour changed: re-check this datafusion-workarounds entry"

SUNDAY = dt.datetime(2024, 3, 3, 10, 30, 20, 123456)


def native(data: dict):
    return SessionContext().from_arrow(pa.table(data))


def collect(lf: nw.LazyFrame) -> dict:
    return lf.collect(backend="pyarrow").to_dict(as_series=False)


def test_enum_cast_names_the_backend() -> None:
    """`cast`: narwhals' Arrow mapping names PyArrow for `Enum`; the plugin names DataFusion."""
    with pytest.raises(NotImplementedError, match="not supported for PyArrow"):
        arrow_narwhals_to_native_dtype(nw.Enum(["a"]), Version.MAIN)
    lf = nw.from_native(native({"s": ["a"]}))
    with pytest.raises(NotImplementedError, match="DataFusion backend"):
        lf.select(nw.col("s").cast(nw.Enum(["a"])))


def test_is_nan_on_strings_raises_invalid_operation() -> None:
    """`is_nan`: `isnan` rejects strings while planning; the plugin raises narwhals' error."""
    df = native({"s": ["x"]})
    with pytest.raises(Exception, match="Function 'isnan' requires Numeric"):
        df.select(F.isnan(col("s")))
    with pytest.raises(InvalidOperationError, match="`is_nan`"):
        nw.from_native(df).select(nw.col("s").is_nan())


@pytest.mark.parametrize(
    ("part", "engine_value", "method", "expected"),
    [
        ("millisecond", 20123, "millisecond", 123),
        ("microsecond", 20123456, "microsecond", 123456),
        ("nanosecond", 20123456000, "nanosecond", 123456000),
        ("dow", 0, "weekday", 7),
    ],
)
def test_date_parts_follow_polars(part: str, engine_value: int, method: str, expected: int) -> None:
    """`dt` parts: `date_part` sub-second parts include whole seconds; `dow` is 0=Sunday."""
    df = native({"t": pa.array([SUNDAY], pa.timestamp("us"))})
    engine = df.select(F.date_part(part, col("t")).alias("p")).to_pydict()
    assert engine == {"p": [engine_value]}, ENGINE_CHANGED
    result = nw.from_native(df).select(getattr(nw.col("t").dt, method)())
    assert collect(result) == {"t": [expected]}


@pytest.mark.parametrize(
    ("every", "expected"),
    [
        ("15m", dt.datetime(2023, 5, 5, 7)),
        ("2h", dt.datetime(2023, 5, 5, 6)),
        ("3d", dt.datetime(2023, 5, 5)),
        ("2mo", dt.datetime(2023, 5, 1)),
        ("2q", dt.datetime(2023, 1, 1)),
    ],
)
def test_truncate_by_multiple_anchors_at_epoch(every: str, expected: dt.datetime) -> None:
    """`dt.truncate`: `date_trunc` takes no multiple; the plugin uses `date_bin` from the epoch."""
    df = native({"t": pa.array([dt.datetime(2023, 5, 5, 7, 13, 42)], pa.timestamp("us"))})
    with pytest.raises(Exception, match="Unsupported date_trunc granularity"):
        df.select(F.date_trunc("15 minute", col("t"))).collect()
    result = nw.from_native(df).select(nw.col("t").dt.truncate(every))
    assert collect(result) == {"t": [expected]}


def test_truncate_keeps_input_type() -> None:
    """`dt.truncate`: `date_bin` returns ns for a `us` input, `date_trunc` for a Date; cast back."""
    df = native({"t": pa.array([dt.datetime(2023, 5, 5, 7, 13)], pa.timestamp("us"))})
    two_hours = lit(pa.scalar((0, 0, 2 * 3600 * 10**9), type=pa.month_day_nano_interval()))
    us_origin = lit(pa.scalar(0, type=pa.timestamp("us")))
    binned = df.select(F.date_bin(two_hours, col("t"), us_origin).alias("t"))
    assert binned.schema().field("t").type == pa.timestamp("ns"), ENGINE_CHANGED
    dates = native({"d": pa.array([dt.date(2023, 5, 6)], pa.date32())})
    truncated = dates.select(F.date_trunc("month", col("d")).alias("d"))
    assert truncated.schema().field("d").type == pa.timestamp("ns"), ENGINE_CHANGED
    result = nw.from_native(df).select(nw.col("t").dt.truncate("2h"))
    assert result.collect_schema()["t"] == nw.Datetime("us")
    result = nw.from_native(dates).select(nw.col("d").dt.truncate("1mo"))
    assert collect(result) == {"d": [dt.date(2023, 5, 1)]}


def test_truncate_origin_is_not_ns() -> None:
    """`dt.truncate`: with an ns origin `date_bin` keeps a `us` input but nulls it past 2262."""
    df = native({"t": pa.array([dt.datetime(2500, 5, 5, 7, 13)], pa.timestamp("us"))})
    two_hours = lit(pa.scalar((0, 0, 2 * 3600 * 10**9), type=pa.month_day_nano_interval()))
    ns_origin = lit(pa.scalar(0, type=pa.timestamp("ns")))
    engine = df.select(F.date_bin(two_hours, col("t"), ns_origin).alias("t")).to_pydict()
    assert engine == {"t": [None]}, ENGINE_CHANGED
    with pytest.raises(Exception, match="exceeds the representable i64 range"):
        collect(nw.from_native(df).select(nw.col("t").dt.truncate("2h")))


def test_truncate_by_multiple_bins_wall_time() -> None:
    """`dt.truncate`: `date_bin` bins an aware timestamp in UTC; the plugin bins the wall time."""
    kolkata = ZoneInfo("Asia/Kolkata")
    local = dt.datetime(2023, 5, 5, 7, 13, tzinfo=kolkata)
    df = native({"t": pa.array([local], pa.timestamp("us", tz="Asia/Kolkata"))})
    two_hours = lit(pa.scalar((0, 0, 2 * 3600 * 10**9), type=pa.month_day_nano_interval()))
    origin = lit(pa.scalar(0, type=pa.timestamp("us")))
    engine = df.select(F.date_bin(two_hours, col("t"), origin).alias("t")).to_pydict()
    assert engine == {"t": [dt.datetime(2023, 5, 5, 5, 30, tzinfo=kolkata)]}, ENGINE_CHANGED
    result = nw.from_native(df).select(nw.col("t").dt.truncate("2h"))
    assert collect(result) == {"t": [dt.datetime(2023, 5, 5, 6, tzinfo=kolkata)]}


def test_convert_time_zone_reads_naive_as_utc() -> None:
    """`convert_time_zone`: a cast from naive keeps the wall time; the plugin attaches UTC first."""
    berlin = ZoneInfo("Europe/Berlin")
    df = native({"t": pa.array([dt.datetime(2024, 3, 5, 10, 30)], pa.timestamp("us"))})
    engine = df.select(col("t").cast(pa.timestamp("us", tz="Europe/Berlin")).alias("t"))
    assert engine.to_pydict() == {"t": [dt.datetime(2024, 3, 5, 10, 30, tzinfo=berlin)]}, (
        ENGINE_CHANGED
    )
    result = nw.from_native(df).select(nw.col("t").dt.convert_time_zone("Europe/Berlin"))
    assert collect(result) == {"t": [dt.datetime(2024, 3, 5, 11, 30, tzinfo=berlin)]}


def test_to_datetime_offset_format_is_utc_aware() -> None:
    """`str.to_datetime`: a `%z` format parses to UTC but returns naive; the plugin adds UTC."""
    df = native({"s": ["2024-03-05 10:00+0200", None]})
    parsed = df.select(F.to_timestamp(col("s"), lit("%Y-%m-%d %H:%M%z")).alias("t"))
    assert parsed.schema().field("t").type == pa.timestamp("ns"), ENGINE_CHANGED
    assert parsed.to_pydict() == {"t": [dt.datetime(2024, 3, 5, 8), None]}, ENGINE_CHANGED
    result = nw.from_native(df).select(nw.col("s").str.to_datetime("%Y-%m-%d %H:%M%z"))
    assert result.collect_schema()["s"] == nw.Datetime("us", "UTC")
    assert collect(result) == {"s": [dt.datetime(2024, 3, 5, 8, tzinfo=dt.timezone.utc), None]}


@pytest.mark.parametrize(
    ("fmt", "value", "unit"),
    [
        ("%Y-%m-%d %H:%M:%S", "2024-03-05 10:00:00", "us"),
        ("%Y-%m-%d %H:%M:%S%.3f", "2024-03-05 10:00:00.123", "ms"),
        ("%Y-%m-%d %H:%M:%S%.9f", "2024-03-05 10:00:00.123456789", "ns"),
    ],
)
def test_to_datetime_unit_follows_the_format(fmt: str, value: str, unit: str) -> None:
    """`str.to_datetime`: `to_timestamp` is always ns; polars takes `%.3f`/`%.9f`, else us."""
    df = native({"s": [value]})
    parsed = df.select(F.to_timestamp(col("s"), lit(fmt)).alias("t"))
    assert parsed.schema().field("t").type == pa.timestamp("ns"), ENGINE_CHANGED
    result = nw.from_native(df).select(nw.col("s").str.to_datetime(fmt))
    assert result.collect_schema()["s"] == nw.Datetime(unit)


def test_to_datetime_without_format_raises() -> None:
    """`str.to_datetime`: `to_timestamp` infers no format polars would; the plugin requires one."""
    df = native({"s": ["2024/03/05 10:00:00"]})
    with pytest.raises(Exception, match="Error parsing timestamp"):
        df.select(F.to_timestamp(col("s"))).collect()
    with pytest.raises(NotImplementedError, match="specify `format`"):
        nw.from_native(df).select(nw.col("s").str.to_datetime())
