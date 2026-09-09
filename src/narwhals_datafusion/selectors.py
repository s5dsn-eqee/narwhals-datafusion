"""Column selectors: the ``nw.selectors`` namespace and the selector expression."""

from __future__ import annotations

from typing import TYPE_CHECKING

from narwhals._compliant import CompliantSelector, LazySelectorNamespace

from narwhals_datafusion.expr import DataFusionExpr

if TYPE_CHECKING:
    from datafusion.expr import Expr  # noqa: F401

    from narwhals_datafusion.dataframe import DataFusionLazyFrame  # noqa: F401
    from narwhals_datafusion.expr import DataFusionWindowFunction


class DataFusionSelectorNamespace(LazySelectorNamespace["DataFusionLazyFrame", "Expr"]):
    """``nw.selectors`` for this backend; narwhals supplies every selector."""

    @property
    def _selector(self) -> type[DataFusionSelector]:
        return DataFusionSelector


class DataFusionSelector(  # type: ignore[misc]
    CompliantSelector["DataFusionLazyFrame", "Expr"], DataFusionExpr
):
    """A ``DataFusionExpr`` that also supports set operations (``|``, ``&``, ``-``, ``~``)."""

    _window_function: DataFusionWindowFunction | None = None

    def _to_expr(self) -> DataFusionExpr:
        """Drop the selector behaviour, keeping the selected columns."""
        return DataFusionExpr(
            self._call,
            self._window_function,
            evaluate_output_names=self._evaluate_output_names,
            alias_output_names=self._alias_output_names,
            version=self._version,
        )
