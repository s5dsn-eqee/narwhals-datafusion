"""`dt` time zones and `truncate`, `str.to_time`, `str.strip_chars`, checked against polars."""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest
from datafusion import SessionContext

import narwhals as nw

BERLIN = ZoneInfo("Europe/Berlin")
KOLKATA = ZoneInfo("Asia/Kolkata")
TOKYO = ZoneInfo("Asia/Tokyo")
UTC = ZoneInfo("UTC")


def frame(values: list, type_: pa.DataType) -> nw.LazyFrame:
    return nw.from_native(SessionContext().from_arrow(pa.table({"t": pa.array(values, type_)})))


def collect(lf: nw.LazyFrame) -> list:
    return lf.collect(backend="pyarrow")["t"].to_list()


def test_convert_time_zone_reads_naive_as_utc() -> None:
    lf = frame([dt.datetime(2024, 3, 5, 10, 30), None], pa.timestamp("us"))
    result = lf.select(nw.col("t").dt.convert_time_zone("Europe/Berlin"))
    assert result.collect_schema()["t"] == nw.Datetime("us", "Europe/Berlin")
    assert collect(result) == [dt.datetime(2024, 3, 5, 11, 30, tzinfo=BERLIN), None]


def test_convert_time_zone_keeps_instant_and_unit() -> None:
    lf = frame(
        [dt.datetime(2024, 3, 5, 11, 30, tzinfo=BERLIN), None], pa.timestamp("ns", "Europe/Berlin")
    )
    result = lf.select(nw.col("t").dt.convert_time_zone("Asia/Tokyo"))
    assert result.collect_schema()["t"] == nw.Datetime("ns", "Asia/Tokyo")
    assert collect(result) == [dt.datetime(2024, 3, 5, 19, 30, tzinfo=TOKYO), None]


@pytest.mark.parametrize(
    ("time_zone", "tzinfo"), [(None, None), ("UTC", UTC), ("Asia/Tokyo", TOKYO)]
)
def test_replace_time_zone_keeps_wall_time(time_zone: str | None, tzinfo: ZoneInfo | None) -> None:
    lf = frame(
        [dt.datetime(2024, 3, 5, 11, 30, tzinfo=BERLIN), None], pa.timestamp("ns", "Europe/Berlin")
    )
    result = lf.select(nw.col("t").dt.replace_time_zone(time_zone))
    assert result.collect_schema()["t"] == nw.Datetime("ns", time_zone)
    assert collect(result) == [dt.datetime(2024, 3, 5, 11, 30, tzinfo=tzinfo), None]


@pytest.mark.parametrize("time_zone", [None, "UTC", "Asia/Tokyo"])
def test_replace_time_zone_on_naive_attaches_zone(time_zone: str | None) -> None:
    lf = frame([dt.datetime(2024, 3, 5, 10, 30), None], pa.timestamp("us"))
    result = lf.select(nw.col("t").dt.replace_time_zone(time_zone))
    assert result.collect_schema()["t"] == nw.Datetime("us", time_zone)
    tzinfo = ZoneInfo(time_zone) if time_zone else None
    assert collect(result) == [dt.datetime(2024, 3, 5, 10, 30, tzinfo=tzinfo), None]


@pytest.mark.parametrize(
    "wall_time", [dt.datetime(2024, 3, 31, 2, 30), dt.datetime(2024, 10, 27, 2, 30)]
)
def test_replace_time_zone_raises_on_skipped_or_repeated_wall_time(wall_time: dt.datetime) -> None:
    lf = frame([wall_time], pa.timestamp("us"))
    with pytest.raises(Exception, match="Cast error"):
        collect(lf.select(nw.col("t").dt.replace_time_zone("Europe/Berlin")))


@pytest.mark.parametrize("unit", ["ms", "us", "ns"])
@pytest.mark.parametrize(
    ("every", "expected"),
    [
        ("1h", dt.datetime(2023, 5, 5, 7)),
        ("2h", dt.datetime(2023, 5, 5, 6)),
        ("3d", dt.datetime(2023, 5, 5)),
        ("2mo", dt.datetime(2023, 5, 1)),
    ],
)
def test_truncate_keeps_time_unit(unit: str, every: str, expected: dt.datetime) -> None:
    lf = frame([dt.datetime(2023, 5, 5, 7, 13, 42), None], pa.timestamp(unit))
    result = lf.select(nw.col("t").dt.truncate(every))
    assert result.collect_schema()["t"] == nw.Datetime(unit)
    assert collect(result) == [expected, None]


@pytest.mark.parametrize(
    ("every", "expected"),
    [
        ("1y", dt.date(2023, 1, 1)),
        ("1mo", dt.date(2023, 5, 1)),
        ("1d", dt.date(2023, 5, 6)),
        ("1h", dt.date(2023, 5, 6)),
        ("2q", dt.date(2023, 1, 1)),
        ("2mo", dt.date(2023, 5, 1)),
        ("3d", dt.date(2023, 5, 5)),
        ("2h", dt.date(2023, 5, 6)),
    ],
)
def test_truncate_keeps_date(every: str, expected: dt.date) -> None:
    lf = frame([dt.date(2023, 5, 6), None], pa.date32())
    result = lf.select(nw.col("t").dt.truncate(every))
    assert result.collect_schema()["t"] == nw.Date
    assert collect(result) == [expected, None]


@pytest.mark.parametrize(
    ("every", "expected"),
    [
        ("1d", [dt.datetime(2023, 5, 5), dt.datetime(2023, 5, 1)]),
        ("1mo", [dt.datetime(2023, 5, 1), dt.datetime(2023, 5, 1)]),
        ("15m", [dt.datetime(2023, 5, 5, 7), dt.datetime(2023, 5, 1, 3)]),
        ("2h", [dt.datetime(2023, 5, 5, 6), dt.datetime(2023, 5, 1, 2)]),
        ("3d", [dt.datetime(2023, 5, 5), dt.datetime(2023, 4, 29)]),
        ("2mo", [dt.datetime(2023, 5, 1), dt.datetime(2023, 5, 1)]),
    ],
)
def test_truncate_bins_aware_input_in_local_time(every: str, expected: list[dt.datetime]) -> None:
    local = [dt.datetime(2023, 5, 5, 7, 13), dt.datetime(2023, 5, 1, 3)]
    lf = frame(
        [*(t.replace(tzinfo=KOLKATA) for t in local), None], pa.timestamp("us", "Asia/Kolkata")
    )
    result = lf.select(nw.col("t").dt.truncate(every))
    assert result.collect_schema()["t"] == nw.Datetime("us", "Asia/Kolkata")
    assert collect(result) == [*(t.replace(tzinfo=KOLKATA) for t in expected), None]


@pytest.mark.parametrize(
    ("values", "format", "expected"),
    [
        (["12.34.56", None], "%H.%M.%S", dt.time(12, 34, 56)),
        (["01:30 PM", None], "%I:%M %p", dt.time(13, 30)),
        (["12:34:56.123456", None], "%H:%M:%S%.f", dt.time(12, 34, 56, 123456)),
    ],
)
def test_to_time_with_format_keeps_nulls(values: list, format: str, expected: dt.time) -> None:
    result = frame(values, pa.string()).select(nw.col("t").str.to_time(format))
    assert result.collect_schema()["t"] == nw.Time
    assert collect(result) == [expected, None]


def test_to_time_rejects_value_not_matching_format() -> None:
    lf = frame(["12:34"], pa.string())
    with pytest.raises(Exception, match="12:34"):
        collect(lf.select(nw.col("t").str.to_time("%H:%M:%S")))


ODD = "\t x-^]\\ "
STRINGS = [ODD, "xyxbarxy", "ééaé", "[a]", "", None]


@pytest.mark.parametrize(
    ("characters", "both", "start", "end"),
    [
        ("", STRINGS, STRINGS, STRINGS),
        (
            None,
            ["x-^]\\", *STRINGS[1:]],
            ["x-^]\\ ", *STRINGS[1:]],
            ["\t x-^]\\", *STRINGS[1:]],
        ),
        (
            "-^]\\ \t",
            ["x", "xyxbarxy", "ééaé", "[a", "", None],
            ["x-^]\\ ", *STRINGS[1:]],
            ["\t x", "xyxbarxy", "ééaé", "[a", "", None],
        ),
        (
            "xy",
            [ODD, "bar", "ééaé", "[a]", "", None],
            [ODD, "barxy", "ééaé", "[a]", "", None],
            [ODD, "xyxbar", "ééaé", "[a]", "", None],
        ),
        (
            "é[]",
            [ODD, "xyxbarxy", "a", "a", "", None],
            [ODD, "xyxbarxy", "aé", "a]", "", None],
            [ODD, "xyxbarxy", "ééa", "[a", "", None],
        ),
    ],
)
def test_strip_chars_strips_a_character_set(
    characters: str | None, both: list, start: list, end: list
) -> None:
    lf = frame(STRINGS, pa.string())
    result = lf.select(
        both=nw.col("t").str.strip_chars(characters),
        start=nw.col("t").str.strip_chars_start(characters),
        end=nw.col("t").str.strip_chars_end(characters),
    )
    expected = {"both": both, "start": start, "end": end}
    assert result.collect(backend="pyarrow").to_dict(as_series=False) == expected
