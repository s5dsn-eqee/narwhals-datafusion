from __future__ import annotations

from typing import TYPE_CHECKING

from datafusion import functions as F
from narwhals._compliant import LazyExprNamespace
from narwhals._compliant.any_namespace import ListNamespace
from narwhals._utils import not_implemented

from narwhals_datafusion.utils import lit

if TYPE_CHECKING:
    from narwhals.typing import NonNestedLiteral

    from narwhals_datafusion.expr import DataFusionExpr


class DataFusionExprListNamespace(
    LazyExprNamespace["DataFusionExpr"], ListNamespace["DataFusionExpr"]
):
    def len(self) -> DataFusionExpr:
        return self.compliant._with_elementwise(F.array_length)

    def unique(self, *, maintain_order: bool) -> DataFusionExpr:
        if maintain_order:
            msg = "`maintain_order=True` is not supported for the DataFusion backend."
            raise NotImplementedError(msg)
        return self.compliant._with_elementwise(F.array_distinct)

    def contains(self, item: NonNestedLiteral) -> DataFusionExpr:
        return self.compliant._with_elementwise(lambda expr: F.array_has(expr, lit(item)))

    def get(self, index: int) -> DataFusionExpr:
        return self.compliant._with_elementwise(lambda expr: F.array_element(expr, lit(index + 1)))

    def min(self) -> DataFusionExpr:
        return self.compliant._with_elementwise(F.array_min)

    def max(self) -> DataFusionExpr:
        return self.compliant._with_elementwise(F.array_max)

    def sort(self, *, descending: bool, nulls_last: bool) -> DataFusionExpr:
        return self.compliant._with_elementwise(
            lambda expr: F.array_sort(expr, descending=descending, null_first=not nulls_last)
        )

    # no `array_sum`/`array_mean`/`array_median` in 54
    mean = not_implemented()
    median = not_implemented()
    sum = not_implemented()
