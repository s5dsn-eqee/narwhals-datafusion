from __future__ import annotations

import re
import string
from typing import TYPE_CHECKING

from datafusion import functions as F
from narwhals._sql.expr_str import SQLExprStringNamespace
from narwhals._utils import not_implemented

from narwhals_datafusion.utils import lit

if TYPE_CHECKING:
    from datafusion.expr import Expr

    from narwhals_datafusion.expr import DataFusionExpr


def _char_class(characters: str) -> str:
    return "".join(re.escape(char) for char in characters)


class DataFusionExprStringNamespace(SQLExprStringNamespace["DataFusionExpr"]):
    def strip_chars(self, characters: str | None) -> DataFusionExpr:
        # `btrim` takes one argument: character-set trims go through a regex
        chars = _char_class(string.whitespace if characters is None else characters)
        pattern = f"^[{chars}]+|[{chars}]+$"
        return self.compliant._with_elementwise(
            lambda expr: F.regexp_replace(expr, lit(pattern), lit(""), lit("g"))
        )

    def strip_chars_start(self, characters: str) -> DataFusionExpr:
        pattern = f"^[{_char_class(characters)}]+"
        return self.compliant._with_elementwise(
            lambda expr: F.regexp_replace(expr, lit(pattern), lit(""), lit("g"))
        )

    def strip_chars_end(self, characters: str) -> DataFusionExpr:
        pattern = f"[{_char_class(characters)}]+$"
        return self.compliant._with_elementwise(
            lambda expr: F.regexp_replace(expr, lit(pattern), lit(""), lit("g"))
        )

    def replace_all(self, value: DataFusionExpr, pattern: str, *, literal: bool) -> DataFusionExpr:
        def func(expr: Expr, value: Expr) -> Expr:
            if literal:
                return F.replace(expr, lit(pattern), value)
            return F.regexp_replace(expr, lit(pattern), value, lit("g"))

        return self.compliant._with_elementwise(func, expression_args={"value": value})

    def to_datetime(self, format: str | None) -> DataFusionExpr:
        if format is None:
            msg = "Cannot infer format with DataFusion, please specify `format` explicitly."
            raise NotImplementedError(msg)

        if "%z" in format or "%Z" in format:
            # `%z`/`%Z` parse to UTC but return a naive timestamp: re-attach the zone
            import pyarrow as pa

            return self.compliant._with_elementwise(
                lambda expr: F.to_timestamp(expr, lit(format)).cast(pa.timestamp("us", tz="UTC"))
            )
        return self.compliant._with_elementwise(lambda expr: F.to_timestamp(expr, lit(format)))

    def to_date(self, format: str | None) -> DataFusionExpr:
        if format is not None:
            return self.compliant._with_elementwise(lambda expr: F.to_date(expr, lit(format)))
        compliant_expr = self.compliant
        return compliant_expr.cast(compliant_expr._version.dtypes.Date())

    def to_time(self, format: str | None) -> DataFusionExpr:
        time_dtype = self.compliant._version.dtypes.Time()
        if format is None:
            # Arrow's cast parses "HH:MM:SS" strings directly
            return self.compliant.cast(time_dtype)

        # `to_timestamp` needs a date: parse against the epoch day and cast down.
        # `concat` turns a null into "1970-01-01 " (parse error), hence the null branch
        import pyarrow as pa

        def func(expr: Expr) -> Expr:
            parsed = F.to_timestamp(F.concat(lit("1970-01-01 "), expr), lit(f"%Y-%m-%d {format}"))
            null = lit(None).cast(pa.timestamp("ns"))
            return F.when(expr.is_null(), null).otherwise(parsed)

        return self.compliant._with_elementwise(func).cast(time_dtype)

    def to_titlecase(self) -> DataFusionExpr:
        return self.compliant._with_elementwise(F.initcap)

    replace = not_implemented()
