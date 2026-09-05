from __future__ import annotations

import operator
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa
from datafusion import functions as F
from datafusion.common import NullTreatment
from narwhals._sql.expr import SQLExpr
from narwhals._utils import (
    NO_DEFAULT,
    Implementation,
    Version,
    extend_bool,
    not_implemented,
)
from narwhals.exceptions import InvalidOperationError

from narwhals_datafusion.expr_dt import DataFusionExprDateTimeNamespace
from narwhals_datafusion.expr_list import DataFusionExprListNamespace
from narwhals_datafusion.expr_str import DataFusionExprStringNamespace
from narwhals_datafusion.expr_struct import DataFusionExprStructNamespace
from narwhals_datafusion.utils import (
    BACKEND_VERSION,
    col,
    lit,
    narwhals_to_native_dtype,
    sort_expr,
    when,
    window_expression,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from datafusion.expr import Expr
    from narwhals._compliant import WindowInputs
    from narwhals._compliant.typing import (
        AliasNames,
        EvalNames,
        EvalSeries,
        WindowFunction,
    )
    from narwhals._typing import NoDefault
    from narwhals._utils import _LimitedContext
    from narwhals.typing import FillNullStrategy, IntoDType
    from typing_extensions import Self

    from narwhals_datafusion.dataframe import DataFusionLazyFrame
    from narwhals_datafusion.namespace import DataFusionNamespace

    DataFusionWindowFunction = WindowFunction[DataFusionLazyFrame, Expr]
    DataFusionWindowInputs = WindowInputs[Expr]


class DataFusionExpr(SQLExpr["DataFusionLazyFrame", "Expr"]):
    _implementation = Implementation.UNKNOWN

    def __init__(
        self,
        call: EvalSeries[DataFusionLazyFrame, Expr],
        window_function: DataFusionWindowFunction | None = None,
        *,
        evaluate_output_names: EvalNames[DataFusionLazyFrame],
        alias_output_names: AliasNames | None,
        version: Version,
        implementation: Implementation = Implementation.UNKNOWN,
    ) -> None:
        self._call = call
        self._evaluate_output_names = evaluate_output_names
        self._alias_output_names = alias_output_names
        self._version = version
        self._window_function: DataFusionWindowFunction | None = window_function

    @property
    def _backend_version(self) -> tuple[int, ...]:
        return BACKEND_VERSION

    def __narwhals_namespace__(self) -> DataFusionNamespace:  # pragma: no cover
        from narwhals_datafusion.namespace import DataFusionNamespace

        return DataFusionNamespace(version=self._version)

    def _count_star(self) -> Expr:
        return F.count_star()

    def _window_expression(
        self,
        expr: Expr,
        partition_by: Sequence[str | Expr] = (),
        order_by: Sequence[str | Expr] = (),
        rows_start: int | None = None,
        rows_end: int | None = None,
        *,
        descending: Sequence[bool] | None = None,
        nulls_last: Sequence[bool] | None = None,
    ) -> Expr:
        return window_expression(
            expr,
            partition_by,
            order_by,
            rows_start,
            rows_end,
            descending=descending,
            nulls_last=nulls_last,
        )

    def _first(self, expr: Expr, *order_by: str) -> Expr:
        return F.first_value(expr, order_by=[sort_expr(by) for by in order_by])

    def _last(self, expr: Expr, *order_by: str) -> Expr:
        # `last_value` with an inner `order_by` is rewritten to first internally;
        # `first_value` over the reversed order is equivalent
        return F.first_value(
            expr,
            order_by=[sort_expr(by, descending=True, nulls_last=True) for by in order_by],
        )

    def _any_value(self, expr: Expr, *, ignore_nulls: bool) -> Expr:
        if ignore_nulls:
            return F.first_value(expr, null_treatment=NullTreatment.IGNORE_NULLS)
        return F.first_value(expr)

    # ORDER BY / IGNORE NULLS / DISTINCT declared inside an aggregate are dropped
    # when it runs as a window function: `first`/`last`/`any_value` put them on
    # the window instead
    def first(self, order_by: Sequence[str] = ()) -> Self:
        def f(expr: Expr) -> Expr:
            if not order_by:  # pragma: no cover
                msg = "Expected `order_by` to be specified"
                raise InvalidOperationError(msg)
            return self._first(expr, *order_by)

        def window_f(df: DataFusionLazyFrame, inputs: DataFusionWindowInputs) -> Sequence[Expr]:
            self._check_first_last_window_args(inputs.order_by, order_by)
            return [
                window_expression(
                    F.first_value(expr),
                    inputs.partition_by,
                    tuple(inputs.order_by or order_by),
                )
                for expr in self(df)
            ]

        return self._with_callable(f, window_f)

    def last(self, order_by: Sequence[str] = ()) -> Self:
        def f(expr: Expr) -> Expr:
            if not order_by:  # pragma: no cover
                msg = "Expected `order_by` to be specified"
                raise InvalidOperationError(msg)
            return self._last(expr, *order_by)

        def window_f(df: DataFusionLazyFrame, inputs: DataFusionWindowInputs) -> Sequence[Expr]:
            self._check_first_last_window_args(inputs.order_by, order_by)
            # ascending like `first`, whole partition as frame: mixed sort
            # directions across windows in one projection trip an optimizer assertion
            return [
                window_expression(
                    F.last_value(expr),
                    inputs.partition_by,
                    tuple(inputs.order_by or order_by),
                    frame_full=True,
                )
                for expr in self(df)
            ]

        return self._with_callable(f, window_f)

    @staticmethod
    def _check_first_last_window_args(
        window_order_by: Sequence[Any], order_by: Sequence[str]
    ) -> None:
        if order_by and window_order_by:  # pragma: no cover
            msg = "Can't specify both `order_by` in `over` and `first`/`last`."
            raise InvalidOperationError(msg)
        if not order_by and not window_order_by:  # pragma: no cover
            msg = "Must specify `order_by` either in `over` or `first`/`last`."
            raise InvalidOperationError(msg)

    def any_value(self, *, ignore_nulls: bool) -> Self:
        def f(expr: Expr) -> Expr:
            return self._any_value(expr, ignore_nulls=ignore_nulls)

        def window_f(df: DataFusionLazyFrame, inputs: DataFusionWindowInputs) -> Sequence[Expr]:
            return [
                window_expression(
                    F.first_value(expr),
                    inputs.partition_by,
                    ignore_nulls=ignore_nulls,
                )
                for expr in self(df)
            ]

        return self._with_callable(f, window_f)

    def broadcast(self) -> Self:
        return self.over([lit(1)], [])

    @classmethod
    def from_column_names(
        cls,
        evaluate_column_names: EvalNames[DataFusionLazyFrame],
        /,
        *,
        context: _LimitedContext,
    ) -> Self:
        def func(df: DataFusionLazyFrame) -> list[Expr]:
            return [col(name) for name in evaluate_column_names(df)]

        return cls(
            func,
            evaluate_output_names=evaluate_column_names,
            alias_output_names=None,
            version=context._version,
        )

    @classmethod
    def from_column_indices(cls, *column_indices: int, context: _LimitedContext) -> Self:
        def func(df: DataFusionLazyFrame) -> list[Expr]:
            columns = df.columns
            return [col(columns[i]) for i in column_indices]

        return cls(
            func,
            evaluate_output_names=cls._eval_names_indices(column_indices),
            alias_output_names=None,
            version=context._version,
        )

    @classmethod
    def _alias_native(cls, expr: Expr, name: str) -> Expr:
        return expr.alias(name)

    def __invert__(self) -> Self:
        invert = cast("Callable[..., Expr]", operator.invert)
        return self._with_elementwise(invert)

    def __neg__(self) -> Self:
        # `Expr` has no unary minus
        return self._with_elementwise(lambda expr: lit(-1) * expr)

    def __pow__(self, other: Self) -> Self:
        # `Expr` has no `**`
        return self._with_binary(lambda expr, other: F.pow(expr, other), other)

    def __rpow__(self, other: Self) -> Self:
        return self._with_binary(lambda expr, other: F.pow(other, expr), other).alias("literal")

    def __truediv__(self, other: Self) -> Self:
        # integer `/` truncates; narwhals `/` is true division
        return self._with_binary(
            lambda expr, other: expr.cast(pa.float64()).__truediv__(other), other
        )

    def __rtruediv__(self, other: Self) -> Self:
        return self._with_binary(
            lambda expr, other: other.cast(pa.float64()).__truediv__(expr), other
        ).alias("literal")

    def n_unique(self) -> Self:
        F_ = self._function
        one, zero = self._lit(1), self._lit(0)

        def func(expr: Expr) -> Expr:
            return F_(
                "add",
                F_("count_distinct", expr),
                F_("max", self._when(F_("isnull", expr), one, zero)),
            )

        def window_f(df: DataFusionLazyFrame, inputs: DataFusionWindowInputs) -> Sequence[Expr]:
            # DISTINCT inside a window aggregate is dropped: wrong result, not an error
            msg = "`n_unique` over a window is not supported for the DataFusion backend."
            raise NotImplementedError(msg)

        return self._with_callable(func, window_f)

    def len(self) -> Self:
        return self._with_callable(lambda _expr: F.count_star())

    def null_count(self) -> Self:
        return self._with_callable(lambda expr: F.sum(expr.is_null().cast(pa.int64())))

    def is_nan(self) -> Self:
        return self._with_elementwise(F.isnan)

    def is_finite(self) -> Self:
        def func(expr: Expr) -> Expr:
            return ~(F.isnan(expr) | (expr == lit(float("inf"))) | (expr == lit(float("-inf"))))

        return self._with_elementwise(func)

    def is_in(self, other: Sequence[Any]) -> Self:
        # SQL IN with a NULL in the list returns null for every non-match;
        # narwhals wants false, so drop Nones
        values = [lit(value) for value in other if value is not None]
        if values:
            return self._with_elementwise(lambda expr: F.in_list(expr, values, negated=False))
        return self._with_elementwise(
            lambda expr: when(expr.is_null(), lit(None).cast(pa.bool_()), lit(False))
        )

    def fill_null(
        self, value: Self | None, strategy: FillNullStrategy | None, limit: int | None
    ) -> Self:
        if strategy is not None:
            if limit is not None:
                # first/last_value over a bounded frame need `retract_batch`, not implemented
                msg = "`fill_null` with a `limit` is not supported for the DataFusion backend."
                raise NotImplementedError(msg)

            def _fill_with_strategy(
                df: DataFusionLazyFrame, inputs: DataFusionWindowInputs
            ) -> Sequence[Expr]:
                # last non-null up to the current row (cumulative frame); backward
                # fill is the same over the reversed order
                flags = extend_bool(strategy == "backward", len(inputs.order_by))
                return [
                    window_expression(
                        F.last_value(expr),
                        inputs.partition_by,
                        inputs.order_by,
                        rows_end=0,
                        descending=flags,
                        nulls_last=flags,
                        ignore_nulls=True,
                    )
                    for expr in self(df)
                ]

            return self._with_window_function(_fill_with_strategy)

        def _fill_constant(expr: Expr, value: Expr) -> Expr:
            return F.nvl(expr, value)

        assert value is not None
        return self._with_elementwise(_fill_constant, expression_args={"value": value})

    def cast(self, dtype: IntoDType) -> Self:
        def func(df: DataFusionLazyFrame) -> list[Expr]:
            native_dtype = narwhals_to_native_dtype(dtype, self._version)
            return [expr.cast(native_dtype) for expr in self(df)]

        def window_f(df: DataFusionLazyFrame, inputs: DataFusionWindowInputs) -> list[Expr]:
            native_dtype = narwhals_to_native_dtype(dtype, self._version)
            return [expr.cast(native_dtype) for expr in self.window_function(df, inputs)]

        return self.__class__(
            func,
            window_f,
            evaluate_output_names=self._evaluate_output_names,
            alias_output_names=self._alias_output_names,
            version=self._version,
        )

    def replace_strict(
        self,
        default: DataFusionExpr | NoDefault,
        old: Sequence[Any],
        new: Sequence[Any],
        *,
        return_dtype: IntoDType | None,
    ) -> Self:
        if default is NO_DEFAULT:
            msg = "`replace_strict` requires an explicit `default` for the DataFusion backend."
            raise ValueError(msg)

        def func(df: DataFusionLazyFrame) -> list[Expr]:
            default_col = df._evaluate_single_output_expr(default)

            results = []
            for expr in self(df):
                pairs = iter(zip(old, new, strict=True))
                first_old, first_new = next(pairs)
                builder = F.when(expr == lit(first_old), lit(first_new))
                for old_value, new_value in pairs:
                    builder = builder.when(expr == lit(old_value), lit(new_value))
                results.append(builder.otherwise(default_col))

            if return_dtype:
                native_dtype = narwhals_to_native_dtype(return_dtype, self._version)
                return [res.cast(native_dtype) for res in results]
            return results

        return self.__class__(
            func,
            None,
            evaluate_output_names=self._evaluate_output_names,
            alias_output_names=self._alias_output_names,
            version=self._version,
        )

    @property
    def str(self) -> DataFusionExprStringNamespace:
        return DataFusionExprStringNamespace(self)

    @property
    def dt(self) -> DataFusionExprDateTimeNamespace:
        return DataFusionExprDateTimeNamespace(self)

    @property
    def list(self) -> DataFusionExprListNamespace:
        return DataFusionExprListNamespace(self)

    @property
    def struct(self) -> DataFusionExprStructNamespace:
        return DataFusionExprStructNamespace(self)

    # no built-in mode/skewness/kurtosis in 54; the `extra-functions` shim provides them
    def _extra_udaf(self, function_name: str, operation: str) -> Any:
        from narwhals_datafusion.extra_functions import INSTALL_HINT, extra_udaf

        fn = extra_udaf(function_name)
        if fn is None:
            msg = f"`{operation}` is not available: {INSTALL_HINT}"
            raise NotImplementedError(msg)
        return fn

    def mode(self, *, keep: str) -> Self:
        if keep != "any":
            msg = (
                f"`Expr.mode(keep='{keep}')` is not implemented for the DataFusion backend.\n\n"
                "Hint: Use `nw.col(...).mode(keep='any')` instead."
            )
            raise NotImplementedError(msg)
        return self._with_callable(self._extra_udaf("mode", "mode"))

    def kurtosis(self) -> Self:
        return self._with_callable(self._extra_udaf("kurtosis_pop", "kurtosis"))

    def skew(self) -> Self:
        # the crate's `skewness` is bias-adjusted (G1); narwhals wants g1: undo
        # the adjustment and pin n=0/1/2, as the duckdb backend does
        fn = self._extra_udaf("skewness", "skew")
        W = self._window_expression

        def _correct(skewness: Expr, count: Expr) -> Expr:
            sample_skewness = skewness * (count - lit(2)) / F.sqrt(count * (count - lit(1)))
            return when(
                count == lit(0),
                lit(None),
                when(
                    count == lit(1),
                    lit(float("nan")),
                    when(count == lit(2), lit(0.0), sample_skewness),
                ),
            )

        def func(expr: Expr) -> Expr:
            return _correct(fn(expr), F.count(expr))

        def window_f(df: DataFusionLazyFrame, inputs: DataFusionWindowInputs) -> list[Expr]:
            return [
                _correct(
                    W(fn(expr), inputs.partition_by),
                    W(F.count(expr), inputs.partition_by),
                )
                for expr in self(df)
            ]

        return self._with_callable(func, window_f)

    # no exact quantile or `product` aggregate in 54
    cum_prod = not_implemented()
    quantile = not_implemented()
