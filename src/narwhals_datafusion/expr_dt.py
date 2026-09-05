from __future__ import annotations

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

NS_PER_UNIT = {
    "h": NS_PER_MINUTE * SECONDS_PER_MINUTE,
    "m": NS_PER_MINUTE,
    "s": NS_PER_SECOND,
    "ms": NS_PER_MILLISECOND,
    "us": NS_PER_MICROSECOND,
    "ns": 1,
}


class DataFusionExprDateTimeNamespace(SQLExprDateTimeNamesSpace["DataFusionExpr"]):
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
        import datetime as dt

        interval = Interval.parse(every)
        multiple, unit = interval.multiple, interval.unit
        if multiple == 1 and unit in UNITS_DICT:
            precision = UNITS_DICT[unit]
            return self.compliant._with_elementwise(lambda expr: F.date_trunc(precision, expr))
        # multiples: `date_bin` anchored at the epoch, polars semantics
        months, days, nanos = 0, 0, 0
        if unit == "y":
            # narwhals' `Interval.parse` rejects year multiples other than 1 today
            months = 12 * multiple
        elif unit == "q":
            months = 3 * multiple
        elif unit == "mo":
            months = multiple
        elif unit == "d":
            days = multiple
        elif unit in NS_PER_UNIT:
            nanos = multiple * NS_PER_UNIT[unit]
        else:  # pragma: no cover
            msg = f"Truncating by {every!r} is not supported for the DataFusion backend."
            raise NotImplementedError(msg)
        stride = lit(pa.scalar((months, days, nanos), type=pa.month_day_nano_interval()))
        origin = lit(pa.scalar(dt.datetime(1970, 1, 1), type=pa.timestamp("us")))
        return self.compliant._with_elementwise(lambda expr: F.date_bin(stride, expr, origin))

    def replace_time_zone(self, time_zone: str | None) -> DataFusionExpr:
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
