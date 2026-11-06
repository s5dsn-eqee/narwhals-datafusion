from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import functions as F
from narwhals._constants import (
    MS_PER_SECOND,
    NS_PER_MICROSECOND,
    NS_PER_MILLISECOND,
    NS_PER_MINUTE,
    NS_PER_SECOND,
    SECONDS_PER_MINUTE,
    US_PER_SECOND,
)
from narwhals._duration import Interval
from narwhals._sql.expr_dt import SQLExprDateTimeNamesSpace
from narwhals._utils import not_implemented

from narwhals_datafusion.utils import lit

if TYPE_CHECKING:
    from collections.abc import Callable

    from datafusion.expr import Expr

    from narwhals_datafusion.expr import DataFusionExpr

# narwhals interval unit -> `date_trunc` precision
UNITS_DICT = {
    "y": "year",
    "q": "quarter",
    "mo": "month",
    "d": "day",
    "h": "hour",
    "m": "minute",
    "s": "second",
    "ms": "millisecond",
    "us": "microsecond",
}

# month-based units, for `date_bin` strides
MONTHS_PER_UNIT = {"y": 12, "q": 3, "mo": 1}

# sub-day units in nanoseconds, for `date_bin` strides
NS_PER_UNIT = {
    "h": NS_PER_MINUTE * SECONDS_PER_MINUTE,
    "m": NS_PER_MINUTE,
    "s": NS_PER_SECOND,
    "ms": NS_PER_MILLISECOND,
    "us": NS_PER_MICROSECOND,
    "ns": 1,
}


class DataFusionExprDateTimeNamespace(SQLExprDateTimeNamesSpace["DataFusionExpr"]):
    """``Expr.dt``; most methods come from ``SQLExprDateTimeNamesSpace``."""

    def millisecond(self) -> DataFusionExpr:
        # `date_part('millisecond')` includes whole seconds
        return self.compliant._with_elementwise(
            lambda expr: (
                F.date_part("millisecond", expr) - F.date_part("second", expr) * lit(MS_PER_SECOND)
            )
        )

    def microsecond(self) -> DataFusionExpr:
        return self.compliant._with_elementwise(
            lambda expr: (
                F.date_part("microsecond", expr) - F.date_part("second", expr) * lit(US_PER_SECOND)
            )
        )

    def nanosecond(self) -> DataFusionExpr:
        return self.compliant._with_elementwise(
            lambda expr: (
                F.date_part("nanosecond", expr) - F.date_part("second", expr) * lit(NS_PER_SECOND)
            )
        )

    def to_string(self, format: str) -> DataFusionExpr:
        return self.compliant._with_elementwise(lambda expr: F.to_char(expr, lit(format)))

    def weekday(self) -> DataFusionExpr:
        # `dow` is 0=Sunday..6=Saturday; narwhals weekday is 1=Monday..7=Sunday
        return self.compliant._with_elementwise(
            lambda expr: ((F.date_part("dow", expr) + lit(6)) % lit(7)) + lit(1)
        )

    def date(self) -> DataFusionExpr:
        return self.compliant._with_elementwise(lambda expr: expr.cast(pa.date32()))

    def truncate(self, every: str) -> DataFusionExpr:
        """Floor to a multiple of a unit, ``every`` such as ``"15m"``, anchored at the epoch."""
        interval = Interval.parse(every)
        # a stride is (months, days, nanoseconds), the parts of an Arrow interval
        match (interval.multiple, interval.unit):
            case (1, unit) if unit in UNITS_DICT:
                precision = UNITS_DICT[unit]
                return self._floor(lambda expr: F.date_trunc(precision, expr), wall_time=False)
            case (multiple, unit) if unit in MONTHS_PER_UNIT:
                stride = (multiple * MONTHS_PER_UNIT[unit], 0, 0)
            case (multiple, "d"):
                stride = (0, multiple, 0)
            case (multiple, unit) if unit in NS_PER_UNIT:
                stride = (0, 0, multiple * NS_PER_UNIT[unit])
            case _:  # pragma: no cover
                msg = f"Truncating by {every!r} is not supported for the DataFusion backend."
                raise NotImplementedError(msg)
        # multiples: `date_bin` anchored at the epoch, polars semantics
        stride_lit = lit(pa.scalar(stride, type=pa.month_day_nano_interval()))
        # an ns origin keeps the input unit but nulls values past 2262; this one raises
        origin = lit(pa.scalar(dt.datetime(1970, 1, 1), type=pa.timestamp("us")))
        return self._floor(lambda expr: F.date_bin(stride_lit, expr, origin), wall_time=True)

    def _floor(self, floor: Callable[[Expr], Expr], *, wall_time: bool) -> DataFusionExpr:
        """Apply ``floor`` keeping the input type; ``wall_time`` floors an aware input locally."""

        def func(expr: Expr, dtype: pa.DataType) -> Expr:
            if wall_time and pa.types.is_timestamp(dtype) and dtype.tz is not None:
                # `date_bin` bins an aware timestamp in UTC: bin the wall time, cast re-attaches
                expr = F.to_local_time(expr)
            # `date_trunc` and `date_bin` return Timestamp(ns) for a Date, `date_bin` for any unit
            return floor(expr).cast(dtype)

        return self.compliant._with_native_dtype(func, elementwise=True)

    def replace_time_zone(self, time_zone: str | None) -> DataFusionExpr:
        """Attach, change or drop the zone, keeping the wall time."""
        if time_zone is None:
            return self.compliant._with_elementwise(F.to_local_time)

        def func(expr: Expr, dtype: pa.DataType) -> Expr:
            return F.to_local_time(expr).cast(pa.timestamp(dtype.unit, time_zone))

        return self.compliant._with_native_dtype(func, elementwise=True)

    def convert_time_zone(self, time_zone: str) -> DataFusionExpr:
        """Change the zone, keeping the instant; a naive input is read as UTC."""

        def func(expr: Expr, dtype: pa.DataType) -> Expr:
            # a cast from naive keeps the wall time: attach UTC first
            utc = expr.cast(pa.timestamp(dtype.unit, "UTC"))
            return utc.cast(pa.timestamp(dtype.unit, time_zone))

        return self.compliant._with_native_dtype(func, elementwise=True)

    offset_by = not_implemented()
    timestamp = not_implemented()
    total_microseconds = not_implemented()
    total_milliseconds = not_implemented()
    total_minutes = not_implemented()
    total_nanoseconds = not_implemented()
    total_seconds = not_implemented()
