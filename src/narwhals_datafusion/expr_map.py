from __future__ import annotations

from typing import TYPE_CHECKING

from datafusion import functions as F
from datafusion import lit
from narwhals._compliant import LazyExprNamespace
from narwhals._compliant.any_namespace import MapNamespace

if TYPE_CHECKING:
    from narwhals.typing import NonNestedLiteral

    from narwhals_datafusion.expr import DataFusionExpr


class DataFusionExprMapNamespace(LazyExprNamespace["DataFusionExpr"], MapNamespace["DataFusionExpr"]):
    """``Expr.map``."""

    def get(self, key: NonNestedLiteral) -> DataFusionExpr:
        """The value for ``key``; null where the map is null or lacks it."""
        return self.compliant._with_elementwise(lambda expr: F.map_extract(expr, lit(key))[0])
