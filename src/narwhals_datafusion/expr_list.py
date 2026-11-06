from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import functions as F
from narwhals._compliant import LazyExprNamespace
from narwhals._compliant.any_namespace import ListNamespace
from narwhals._utils import not_implemented

from narwhals_datafusion.utils import lit, when

if TYPE_CHECKING:
    from datafusion.expr import Expr
    from narwhals.typing import NonNestedLiteral

    from narwhals_datafusion.expr import DataFusionExpr


class DataFusionExprListNamespace(
    LazyExprNamespace["DataFusionExpr"], ListNamespace["DataFusionExpr"]
):
    """``Expr.list``, one ``array_*`` function per method."""

    def len(self) -> DataFusionExpr:
        return self.compliant._with_elementwise(F.array_length)

    def unique(self, *, maintain_order: bool) -> DataFusionExpr:
        """Distinct elements; ``array_distinct`` gives no order, so ``maintain_order`` raises."""
        if maintain_order:
            msg = "`maintain_order=True` is not supported for the DataFusion backend."
            raise NotImplementedError(msg)
        return self.compliant._with_elementwise(F.array_distinct)

    def contains(self, item: NonNestedLiteral) -> DataFusionExpr:
        # `array_has` is null for a null item and does not coerce Decimal; the
        # list form plans to the same single projection, `make_array` folded:
        #   ProjectionExec: expr=[array_has_any(l@0, [2]) as r]
        #     DataSourceExec: partitions=1, partition_sizes=[1]
        return self.compliant._with_elementwise(
            lambda expr: F.array_has_any(expr, F.make_array(lit(item)))
        )

    def get(self, index: int) -> DataFusionExpr:
        """Element at zero-based ``index``; ``array_element`` counts from one."""
        return self.compliant._with_elementwise(lambda expr: F.array_element(expr, lit(index + 1)))

    def min(self) -> DataFusionExpr:
        return self.compliant._with_elementwise(F.array_min)

    def max(self) -> DataFusionExpr:
        return self.compliant._with_elementwise(F.array_max)

    def sort(self, *, descending: bool, nulls_last: bool) -> DataFusionExpr:
        return self.compliant._with_elementwise(
            lambda expr: F.array_sort(expr, descending=descending, null_first=not nulls_last)
        )

    def mean(self) -> DataFusionExpr:
        return self.compliant._with_elementwise(F.array_avg)

    def sum(self) -> DataFusionExpr:
        """Sum of the non-null elements, 0 for an empty list; float elements only."""

        # `array_sum` returns Float64 for every input and null for an empty list
        def func(expr: Expr, dtype: pa.DataType) -> Expr:
            value_type = getattr(dtype, "value_type", None)
            if value_type is None or not pa.types.is_floating(value_type):
                msg = f"`list.sum` on `{dtype}` is not supported for the DataFusion backend."
                raise NotImplementedError(msg)
            return when(expr.is_not_null(), F.coalesce(F.array_sum(expr), lit(0.0)))

        return self.compliant._with_native_dtype(func, elementwise=True)

    # no `array_median`
    median = not_implemented()
