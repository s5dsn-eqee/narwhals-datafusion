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
                return self.compliant._with_elementwise(lambda expr: F.date_trunc(precision, expr))
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
        origin = lit(pa.scalar(dt.datetime(1970, 1, 1), type=pa.timestamp("us")))
        return self.compliant._with_elementwise(lambda expr: F.date_bin(stride_lit, expr, origin))

    def replace_time_zone(self, time_zone: str | None) -> DataFusionExpr:
        """Attach or drop a zone, keeping the wall time; only ``None`` and ``"UTC"``."""
        if time_zone is None:
            return self.compliant._with_elementwise(lambda expr: expr.cast(pa.timestamp("us")))
        if time_zone == "UTC":
            # wall time equals UTC time: attaching the zone is a plain cast
            return self.compliant._with_elementwise(
                lambda expr: expr.cast(pa.timestamp("us")).cast(pa.timestamp("us", tz="UTC"))
            )
        msg = "`replace_time_zone` with a non-UTC time zone is not supported for DataFusion."
        raise NotImplementedError(msg)

    def convert_time_zone(self, time_zone: str) -> DataFusionExpr:
        """Change the zone, keeping the instant."""
        return self.compliant._with_elementwise(
            lambda expr: expr.cast(pa.timestamp("us", tz=time_zone))
        )

    offset_by = not_implemented()
    timestamp = not_implemented()
    total_microseconds = not_implemented()
    total_milliseconds = not_implemented()
    total_minutes = not_implemented()
    total_nanoseconds = not_implemented()
    total_seconds = not_implemented()
