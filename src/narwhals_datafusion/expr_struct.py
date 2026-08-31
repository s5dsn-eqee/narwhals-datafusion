from __future__ import annotations

from typing import TYPE_CHECKING

from datafusion import functions as F
from narwhals._compliant import LazyExprNamespace
from narwhals._compliant.any_namespace import StructNamespace

if TYPE_CHECKING:
    from narwhals_datafusion.expr import DataFusionExpr


class DataFusionExprStructNamespace(
    LazyExprNamespace["DataFusionExpr"], StructNamespace["DataFusionExpr"]
):
    def field(self, name: str) -> DataFusionExpr:
        return self.compliant._with_elementwise(lambda expr: F.get_field(expr, name)).alias(name)
