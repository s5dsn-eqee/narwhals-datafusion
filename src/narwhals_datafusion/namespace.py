from __future__ import annotations

import operator
from functools import reduce
from itertools import chain
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from datafusion import functions as F
from narwhals._compliant.namespace import AlignDiagonal
from narwhals._expression_parsing import (
    combine_alias_output_names,
    combine_evaluate_output_names,
    evaluate_output_names_and_aliases,
)
from narwhals._sql.namespace import SQLNamespace
from narwhals._utils import Implementation, validate_separators

from narwhals_datafusion.dataframe import DataFusionLazyFrame
from narwhals_datafusion.expr import DataFusionExpr
from narwhals_datafusion.selectors import DataFusionSelectorNamespace
from narwhals_datafusion.utils import (
    BACKEND_VERSION,
    function,
    lit,
    narwhals_to_native_dtype,
    session_context,
    when,
    window_expression,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from datafusion.expr import Expr
    from narwhals._compliant.window import WindowInputs
    from narwhals._utils import Version
    from narwhals.typing import (
        ConcatMethod,
        CorrelationMethod,
        IntoDType,
        NormalizedPath,
        PythonLiteral,
    )


class DataFusionNamespace(
    SQLNamespace[DataFusionLazyFrame, DataFusionExpr, "DataFrame", "Expr"],
    AlignDiagonal[DataFusionLazyFrame, DataFusionExpr],
):
    _implementation: Implementation = Implementation.UNKNOWN

    def __init__(self, *, version: Version) -> None:
        self._version = version

    @property
    def _backend_version(self) -> tuple[int, ...]:
        return BACKEND_VERSION

    @property
    def selectors(self) -> DataFusionSelectorNamespace:
        return DataFusionSelectorNamespace.from_namespace(self)

    @property
    def _expr(self) -> type[DataFusionExpr]:
        return DataFusionExpr

    @property
    def _lazyframe(self) -> type[DataFusionLazyFrame]:
        return DataFusionLazyFrame

    def scan_csv(
        self, source: NormalizedPath, *, separator: str = ",", **kwds: Any
    ) -> DataFusionLazyFrame:
        validate_separators(separator, ("delimiter", "delim", "sep"), kwds)
        native = session_context().read_csv(source, delimiter=separator, **kwds)
        return self._lazyframe.from_native(native, context=self)

    def scan_parquet(self, source: NormalizedPath, **kwds: Any) -> DataFusionLazyFrame:
        native = session_context().read_parquet(source, **kwds)
        return self._lazyframe.from_native(native, context=self)

    def _function(self, name: str, *args: Expr | PythonLiteral) -> Expr:  # type: ignore[override]
        return function(name, *args)

    def _lit(self, value: Any) -> Expr:
        return lit(value)

    def _when(self, condition: Expr, value: Expr, otherwise: Expr | None = None) -> Expr:
        return when(condition, value, otherwise)

    def _coalesce(self, *exprs: Expr) -> Expr:
        return F.coalesce(*exprs)

    def concat(
        self, items: Iterable[DataFusionLazyFrame], *, how: ConcatMethod
    ) -> DataFusionLazyFrame:
        items = list(items)
        first = items[0]
        if how == "diagonal":
            items = list(self.align_diagonal(items))
            first = items[0]
        else:
            schema = first.schema
            if not all(item.schema == schema for item in items[1:]):
                msg = "inputs should all have the same schema"
                raise TypeError(msg)
            # `union` is positional: align column order across frames.
            columns = first.columns
            items = [item.simple_select(*columns) for item in items]
        native = reduce(lambda left, right: left.union(right), (item.native for item in items))
        return first._with_native(native)

    def concat_str(
        self, *exprs: DataFusionExpr, separator: str, ignore_nulls: bool
    ) -> DataFusionExpr:
        def func(df: DataFusionLazyFrame) -> list[Expr]:
            cols = [expr.cast(pa.string()) for expr in chain.from_iterable(e(df) for e in exprs)]
            if ignore_nulls:
                return [F.concat_ws(separator, *cols)]
            null_mask = reduce(operator.or_, (expr.is_null() for expr in cols))
            return [when(~null_mask, F.concat_ws(separator, *cols))]

        return self._expr(
            call=func,
            evaluate_output_names=combine_evaluate_output_names(*exprs),
            alias_output_names=combine_alias_output_names(*exprs),
            version=self._version,
        )

    def mean_horizontal(self, *exprs: DataFusionExpr) -> DataFusionExpr:
        def func(cols: Iterable[Expr]) -> Expr:
            cols = tuple(cols)
            total = reduce(operator.add, (F.coalesce(col, lit(0)) for col in cols))
            count = reduce(operator.add, (col.is_not_null().cast(pa.int64()) for col in cols))
            # An all-null row divides 0.0/0, which DataFusion evaluates to NaN;
            # narwhals wants null.
            return when(count == lit(0), lit(None), total.cast(pa.float64()) / count)

        return self._expr._from_elementwise_horizontal_op(func, *exprs)

    def lit(self, value: PythonLiteral, dtype: IntoDType | None) -> DataFusionExpr:
        def func(_df: DataFusionLazyFrame) -> list[Expr]:
            if dtype is not None:
                target = narwhals_to_native_dtype(dtype, self._version)
                return [lit(value).cast(target)]
            return [lit(value)]

        def window_func(df: DataFusionLazyFrame, _window_inputs: WindowInputs[Expr]) -> list[Expr]:
            return func(df)

        return self._expr(
            func,
            window_func,
            evaluate_output_names=lambda _df: ["literal"],
            alias_output_names=None,
            version=self._version,
        )

    def len(self) -> DataFusionExpr:
        def func(_df: DataFusionLazyFrame) -> list[Expr]:
            return [F.count_star()]

        return self._expr(
            call=func,
            evaluate_output_names=lambda _df: ["len"],
            alias_output_names=None,
            version=self._version,
        )

    def corr(
        self, a: DataFusionExpr, b: DataFusionExpr, *, method: CorrelationMethod
    ) -> DataFusionExpr:
        if method != "pearson":
            msg = "Only 'pearson' correlation is supported for the DataFusion backend."
            raise NotImplementedError(msg)

        def func(df: DataFusionLazyFrame) -> list[Expr]:
            a_ = df._evaluate_single_output_expr(a)
            b_ = df._evaluate_single_output_expr(b)
            return [F.corr(a_.cast(pa.float64()), b_.cast(pa.float64()))]

        return self._expr(
            call=func,
            evaluate_output_names=combine_evaluate_output_names(a, b),
            alias_output_names=combine_alias_output_names(a, b),
            version=self._version,
        )

    def cov(self, a: DataFusionExpr, b: DataFusionExpr, *, ddof: int) -> DataFusionExpr:
        def _cov(a_: Expr, b_: Expr, wrap: Callable[[Expr], Expr]) -> Expr:
            # `wrap` windows each aggregate individually in a window context:
            # datafusion only accepts aggregate/window functions in `over`, so
            # the compound expression can't be wrapped as a whole.
            if ddof == 0:
                return wrap(F.covar_pop(a_, b_))
            if ddof == 1:
                return wrap(F.covar_samp(a_, b_))
            is_valid = (a_.is_not_null() & b_.is_not_null()).cast(pa.int64())
            n_samples = wrap(F.sum(is_valid))
            denominator = n_samples - lit(ddof)
            rescaled = wrap(F.covar_samp(a_, b_)) * (
                (n_samples - lit(1)).cast(pa.float64()) / denominator
            )
            return when(denominator <= lit(0), lit(None), rescaled)

        def func(df: DataFusionLazyFrame) -> list[Expr]:
            a_ = df._evaluate_single_output_expr(a).cast(pa.float64())
            b_ = df._evaluate_single_output_expr(b).cast(pa.float64())
            return [_cov(a_, b_, lambda e: e)]

        def window_f(df: DataFusionLazyFrame, inputs: WindowInputs[Expr]) -> list[Expr]:
            a_ = df._evaluate_single_output_expr(a).cast(pa.float64())
            b_ = df._evaluate_single_output_expr(b).cast(pa.float64())
            return [_cov(a_, b_, lambda e: window_expression(e, inputs.partition_by))]

        return self._expr(
            call=func,
            window_function=window_f,
            evaluate_output_names=combine_evaluate_output_names(a, b),
            alias_output_names=combine_alias_output_names(a, b),
            version=self._version,
        )

    def struct(self, *exprs: DataFusionExpr) -> DataFusionExpr:
        def func(df: DataFusionLazyFrame) -> list[Expr]:
            name_pairs = [
                (alias, native_expr)
                for expr in exprs
                for native_expr, _, alias in zip(
                    expr(df), *evaluate_output_names_and_aliases(expr, df, []), strict=False
                )
            ]
            return [F.named_struct(name_pairs)]

        return self._expr(
            call=func,
            evaluate_output_names=combine_evaluate_output_names(*exprs),
            alias_output_names=combine_alias_output_names(*exprs),
            version=self._version,
        )

    def list(self, *exprs: DataFusionExpr) -> DataFusionExpr:
        def func(df: DataFusionLazyFrame) -> list[Expr]:
            cols = [native_expr for expr in exprs for native_expr in expr(df)]
            return [F.make_array(*cols)]

        return self._expr(
            call=func,
            evaluate_output_names=combine_evaluate_output_names(*exprs),
            alias_output_names=combine_alias_output_names(*exprs),
            version=self._version,
        )
