from __future__ import annotations

from typing import TYPE_CHECKING

from narwhals._sql.group_by import SQLGroupBy

from narwhals_datafusion.utils import col

if TYPE_CHECKING:
    from collections.abc import Sequence

    from datafusion.expr import Expr  # noqa: F401

    from narwhals_datafusion.dataframe import DataFusionLazyFrame
    from narwhals_datafusion.expr import DataFusionExpr


class DataFusionGroupBy(SQLGroupBy["DataFusionLazyFrame", "DataFusionExpr", "Expr"]):
    def __init__(
        self,
        df: DataFusionLazyFrame,
        keys: Sequence[DataFusionExpr] | Sequence[str],
        /,
        *,
        drop_null_keys: bool,
    ) -> None:
        frame, self._keys, self._output_key_names = self._parse_keys(df, keys=keys)
        self._compliant_frame = frame.drop_nulls(self._keys) if drop_null_keys else frame

    def agg(self, *exprs: DataFusionExpr) -> DataFusionLazyFrame:
        agg_columns = list(self._evaluate_exprs(exprs))
        native = self.compliant.native.aggregate([col(key) for key in self._keys], agg_columns)
        return self.compliant._with_native(native).rename(
            dict(zip(self._keys, self._output_key_names, strict=True))
        )
