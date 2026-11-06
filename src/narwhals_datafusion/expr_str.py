from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import functions as F
from narwhals._sql.expr_str import SQLExprStringNamespace
from narwhals._utils import not_implemented

from narwhals_datafusion.utils import lit

if TYPE_CHECKING:
    from datafusion.expr import Expr

    from narwhals_datafusion.expr import DataFusionExpr


class DataFusionExprStringNamespace(SQLExprStringNamespace["DataFusionExpr"]):
    """``Expr.str``; most methods come from ``SQLExprStringNamespace``."""

    def strip_chars_start(self, characters: str) -> DataFusionExpr:
        return self.compliant._with_elementwise(lambda expr: F.ltrim(expr, lit(characters)))

    def strip_chars_end(self, characters: str) -> DataFusionExpr:
        return self.compliant._with_elementwise(lambda expr: F.rtrim(expr, lit(characters)))

    def replace_all(self, value: DataFusionExpr, pattern: str, *, literal: bool) -> DataFusionExpr:
        """Replace every match of ``pattern`` with ``value``; ``literal`` disables regex."""

        def func(expr: Expr, value: Expr) -> Expr:
            if literal:
                return F.replace(expr, lit(pattern), value)
            return F.regexp_replace(expr, lit(pattern), value, lit("g"))

        return self.compliant._with_elementwise(func, expression_args={"value": value})

    def to_datetime(self, format: str | None) -> DataFusionExpr:
        """Parse with ``format``; inferring one is not supported."""
        if format is None:
            msg = "Cannot infer format with DataFusion, please specify `format` explicitly."
            raise NotImplementedError(msg)

        if "%z" in format or "%Z" in format:
            # `%z`/`%Z` parse to UTC but return a naive timestamp: re-attach the zone
            return self.compliant._with_elementwise(
                lambda expr: F.to_timestamp(expr, lit(format)).cast(pa.timestamp("us", tz="UTC"))
            )
        return self.compliant._with_elementwise(lambda expr: F.to_timestamp(expr, lit(format)))

    def to_date(self, format: str | None) -> DataFusionExpr:
        """Parse with ``format``, or cast when none is given."""
        if format is not None:
            return self.compliant._with_elementwise(lambda expr: F.to_date(expr, lit(format)))
        compliant_expr = self.compliant
        return compliant_expr.cast(compliant_expr._version.dtypes.Date())

    def to_time(self, format: str | None) -> DataFusionExpr:
        """Parse with ``format``, or cast when none is given."""
        if format is None:
            # Arrow's cast parses "HH:MM:SS" strings directly
            return self.compliant.cast(self.compliant._version.dtypes.Time())
        return self.compliant._with_elementwise(lambda expr: F.to_time(expr, lit(format)))

    def to_titlecase(self) -> DataFusionExpr:
        return self.compliant._with_elementwise(F.initcap)

    replace = not_implemented()
