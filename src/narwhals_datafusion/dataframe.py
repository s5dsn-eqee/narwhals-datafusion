from __future__ import annotations

from functools import reduce
from operator import and_
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from datafusion import functions as F
from narwhals._sql.dataframe import SQLLazyFrame
from narwhals._utils import (
    Implementation,
    Version,
    extend_bool,
    generate_temporary_column_name,
    not_implemented,
    parse_columns_to_drop,
)
from narwhals.exceptions import InvalidOperationError

from narwhals_datafusion.utils import (
    BACKEND_VERSION,
    catch_datafusion_exception,
    col,
    evaluate_exprs_and_aliases,
    lit,
    native_to_narwhals_dtype,
    sort_expr,
    window_expression,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping, Sequence
    from io import BytesIO
    from types import ModuleType

    import datafusion
    from datafusion.expr import Expr
    from narwhals._compliant.typing import CompliantDataFrameAny
    from narwhals._typing import _EagerAllowedImpl
    from narwhals._utils import _LimitedContext
    from narwhals.dataframe import LazyFrame
    from narwhals.dtypes import DType
    from narwhals.typing import JoinStrategy, UniqueKeepStrategy
    from typing_extensions import Self, TypeIs

    from narwhals_datafusion.expr import DataFusionExpr
    from narwhals_datafusion.group_by import DataFusionGroupBy
    from narwhals_datafusion.namespace import DataFusionNamespace


# `datafusion.DataFrame` has no `columns`, so it fails narwhals' `NativeLazyFrame`
# bound; dispatch goes through `is_native`, so the mismatch is type-only.
class DataFusionLazyFrame(
    SQLLazyFrame["DataFusionExpr", "datafusion.DataFrame", "LazyFrame[datafusion.DataFrame]"]  # pyright: ignore[reportInvalidTypeArguments]
):
    _implementation = Implementation.UNKNOWN

    def __init__(self, df: datafusion.DataFrame, *, version: Version) -> None:
        self._native_frame: datafusion.DataFrame = df
        self._version = version
        self._cached_native_schema: pa.Schema | None = None
        self._cached_columns: list[str] | None = None

    @property
    def _backend_version(self) -> tuple[int, ...]:
        return BACKEND_VERSION

    @staticmethod
    def _is_native(obj: datafusion.DataFrame | Any) -> TypeIs[datafusion.DataFrame]:
        import datafusion

        return isinstance(obj, datafusion.DataFrame)

    @classmethod
    def from_native(cls, data: datafusion.DataFrame, /, *, context: _LimitedContext) -> Self:
        return cls(data, version=context._version)

    def to_narwhals(self) -> LazyFrame[datafusion.DataFrame]:  # pyright: ignore[reportInvalidTypeArguments]
        return self._version.lazyframe(self, level="lazy")

    def __narwhals_lazyframe__(self) -> Self:
        return self

    def __native_namespace__(self) -> ModuleType:
        import datafusion

        return datafusion

    def __narwhals_namespace__(self) -> DataFusionNamespace:
        from narwhals_datafusion.namespace import DataFusionNamespace

        return DataFusionNamespace(version=self._version)

    def _iter_columns(self) -> Iterator[Expr]:
        for name in self.columns:
            yield col(name)

    @property
    def _native_schema(self) -> pa.Schema:
        if self._cached_native_schema is None:
            self._cached_native_schema = self.native.schema()
        return self._cached_native_schema

    @property
    def schema(self) -> dict[str, DType]:
        schema = self._native_schema
        return {field.name: native_to_narwhals_dtype(field.type, self._version) for field in schema}

    def collect_schema(self) -> dict[str, DType]:
        return self.schema

    @property
    def columns(self) -> list[str]:
        if self._cached_columns is None:
            self._cached_columns = list(self._native_schema.names)
        return self._cached_columns

    def _with_version(self, version: Version) -> Self:
        return self.__class__(self.native, version=version)

    def _with_native(self, df: datafusion.DataFrame) -> Self:
        return self.__class__(df, version=self._version)

    def collect(self, backend: _EagerAllowedImpl | None, **kwargs: Any) -> CompliantDataFrameAny:
        if backend is None or backend is Implementation.PYARROW:
            from narwhals._arrow.dataframe import ArrowDataFrame

            return ArrowDataFrame(
                self.native.to_arrow_table(),
                validate_backend_version=True,
                version=self._version,
                validate_column_names=True,
            )

        if backend is Implementation.PANDAS:
            from narwhals._pandas_like.dataframe import PandasLikeDataFrame

            return PandasLikeDataFrame(
                self.native.to_pandas(),
                implementation=Implementation.PANDAS,
                validate_backend_version=True,
                version=self._version,
                validate_column_names=True,
            )

        if backend is Implementation.POLARS:
            from narwhals._polars.dataframe import PolarsDataFrame

            return PolarsDataFrame(
                self.native.to_polars(),
                validate_backend_version=True,
                version=self._version,
            )

        msg = f"Unsupported `backend` value: {backend}"  # pragma: no cover
        raise ValueError(msg)  # pragma: no cover

    def head(self, n: int) -> Self:
        return self._with_native(self.native.limit(n))

    def simple_select(self, *column_names: str) -> Self:
        return self._with_native(self.native.select(*(col(c) for c in column_names)))

    def aggregate(self, *exprs: DataFusionExpr) -> Self:
        selection = [value.alias(name) for name, value in evaluate_exprs_and_aliases(self, *exprs)]
        try:
            return self._with_native(self.native.aggregate([], selection))
        except Exception as e:
            raise catch_datafusion_exception(e, self) from None

    def select(self, *exprs: DataFusionExpr) -> Self:
        selection = [value.alias(name) for name, value in evaluate_exprs_and_aliases(self, *exprs)]
        try:
            return self._with_native(self.native.select(*selection))
        except Exception as e:
            raise catch_datafusion_exception(e, self) from None

    def with_columns(self, *exprs: DataFusionExpr) -> Self:
        new_columns_map = dict(evaluate_exprs_and_aliases(self, *exprs))
        result = [
            new_columns_map.pop(name).alias(name) if name in new_columns_map else col(name)
            for name in self.columns
        ]
        result.extend(value.alias(name) for name, value in new_columns_map.items())
        try:
            return self._with_native(self.native.select(*result))
        except Exception as e:
            raise catch_datafusion_exception(e, self) from None

    def _filter(self, predicate: DataFusionExpr) -> Self:
        mask = predicate(self)[0]
        try:
            return self._with_native(self.native.filter(mask))
        except Exception as e:
            raise catch_datafusion_exception(e, self) from None

    def drop(self, columns: Sequence[str], *, strict: bool) -> Self:
        columns_to_drop = parse_columns_to_drop(self, columns, strict=strict)
        selection = (name for name in self.columns if name not in columns_to_drop)
        return self.simple_select(*selection)

    def drop_nulls(self, subset: Sequence[str] | None) -> Self:
        subset_ = subset if subset is not None else self.columns
        if not subset_:
            return self
        keep_condition = reduce(and_, (col(name).is_not_null() for name in subset_))
        return self._with_native(self.native.filter(keep_condition))

    def rename(self, mapping: Mapping[str, str]) -> Self:
        selection = (
            col(name).alias(mapping[name]) if name in mapping else col(name)
            for name in self.columns
        )
        return self._with_native(self.native.select(*selection))

    def group_by(
        self, keys: Sequence[str] | Sequence[DataFusionExpr], *, drop_null_keys: bool
    ) -> DataFusionGroupBy:
        from narwhals_datafusion.group_by import DataFusionGroupBy

        return DataFusionGroupBy(self, keys, drop_null_keys=drop_null_keys)

    def sort(self, *by: str, descending: bool | Sequence[bool], nulls_last: bool) -> Self:
        descending_flags = extend_bool(descending, len(by))
        keys = [
            sort_expr(name, descending=desc, nulls_last=nulls_last)
            for name, desc in zip(by, descending_flags, strict=True)
        ]
        return self._with_native(self.native.sort(*keys))

    def top_k(self, k: int, *, by: Iterable[str], reverse: bool | Sequence[bool]) -> Self:
        by = list(by)
        if isinstance(reverse, bool):
            descending = extend_bool(not reverse, len(by))
        else:
            descending = tuple(not rev for rev in reverse)
        tmp_name = generate_temporary_column_name(8, self.columns, prefix="top_k_")
        rank = window_expression(
            F.row_number(),
            order_by=by,
            descending=descending,
            nulls_last=extend_bool(True, len(by)),
        )
        with_rank = self.native.select(*(col(name) for name in self.columns), rank.alias(tmp_name))
        return self._with_native(with_rank.filter(col(tmp_name) <= lit(k))).drop(
            [tmp_name], strict=False
        )

    def unique(
        self,
        subset: Sequence[str] | None,
        *,
        keep: UniqueKeepStrategy,
        order_by: Sequence[str] | None,
    ) -> Self:
        subset_ = subset or self.columns
        if error := self._check_columns_exist(subset_):
            raise error
        tmp_name = generate_temporary_column_name(8, self.columns, prefix="row_index_")
        flags = extend_bool(True, len(order_by)) if order_by and keep == "last" else None
        if keep == "none":
            # no `order_by`: with ORDER BY the frame is cumulative and this
            # becomes a running count
            expr = window_expression(F.count_star(), subset_)
        else:
            expr = window_expression(
                F.row_number(),
                subset_,
                order_by or (),
                descending=flags,
                nulls_last=flags,
            )
        with_marker = self.native.select(
            *(col(name) for name in self.columns), expr.alias(tmp_name)
        )
        return self._with_native(with_marker.filter(col(tmp_name) == lit(1))).drop(
            [tmp_name], strict=False
        )

    def join(
        self,
        other: Self,
        *,
        how: JoinStrategy,
        left_on: Sequence[str] | None,
        right_on: Sequence[str] | None,
        suffix: str,
    ) -> Self:
        left_columns = self.columns
        right_columns = other.columns

        if how in ("semi", "anti"):  # tuple, so pyright narrows the literal
            assert left_on is not None
            assert right_on is not None
            joined = self.native.join(
                other.native,
                left_on=list(left_on),
                right_on=list(right_on),
                how=how,
            )
            # semi/anti joins keep left columns only
            return self._with_native(joined.select(*(col(c) for c in left_columns)))

        # shared column names make the joined schema ambiguous: rename every
        # right-hand column to a temporary name, re-select with narwhals' suffix rules
        tmp_names = {
            name: generate_temporary_column_name(
                8, [*left_columns, *right_columns], prefix=f"join_{i}_"
            )
            for i, name in enumerate(right_columns)
        }
        rhs = other.native.select(*(col(name).alias(tmp_names[name]) for name in right_columns))

        if how == "cross":
            joined = self.native.join_on(rhs, lit(True), how="inner")
        else:
            assert left_on is not None
            assert right_on is not None
            joined = self.native.join(
                rhs,
                left_on=list(left_on),
                right_on=[tmp_names[name] for name in right_on],
                how=how,
                coalesce_duplicate_keys=False,
            )

        selection = [col(name) for name in left_columns]
        for name in right_columns:
            in_left = name in left_columns
            renamed = col(tmp_names[name])
            if how == "full":
                if in_left:
                    selection.append(renamed.alias(f"{name}{suffix}"))
                else:
                    selection.append(renamed.alias(name))
            elif right_on is not None and name in right_on:
                continue  # key columns are kept from the left side only
            elif in_left:
                selection.append(renamed.alias(f"{name}{suffix}"))
            else:
                selection.append(renamed.alias(name))

        try:
            return self._with_native(joined.select(*selection))
        except Exception as e:
            raise catch_datafusion_exception(e, self) from None

    def explode(self, columns: Sequence[str]) -> Self:
        dtypes = self._version.dtypes
        schema = self.collect_schema()
        for name in columns:
            dtype = schema[name]
            if dtype != dtypes.List:
                msg = f"`explode` operation not supported for dtype `{dtype}`, expected List type"
                raise InvalidOperationError(msg)

        if len(columns) != 1:
            msg = (
                "Exploding on multiple columns is not supported with the DataFusion backend since "
                "we cannot guarantee that the exploded columns have matching element counts."
            )
            raise NotImplementedError(msg)

        name = columns[0]
        original_columns = self.columns
        inner_type = self._native_schema.field(name).type.value_type

        # `unnest_columns` drops empty lists and keeps nulls only via
        # `preserve_nulls`: route both through a literal-null branch and union
        not_null_condition = col(name).is_not_null() & (F.array_length(col(name)) > lit(0))
        non_null_rel = (
            self.native.filter(not_null_condition)
            .unnest_columns(name)
            .select(*(col(c) for c in original_columns))
        )
        null_rel = self.native.filter(~not_null_condition).select(
            *(
                lit(None).cast(inner_type).alias(c) if c == name else col(c)
                for c in original_columns
            )
        )
        return self._with_native(non_null_rel.union(null_rel))

    def unpivot(
        self,
        on: Sequence[str] | None,
        index: Sequence[str] | None,
        variable_name: str,
        value_name: str,
    ) -> Self:
        index_ = [] if index is None else list(index)
        on_ = [c for c in self.columns if c not in index_] if on is None else list(on)
        if not on_:
            # nothing to melt: index columns plus an empty variable/value pair
            empty = self.native.select(
                *(col(c) for c in index_),
                lit(None).cast(pa.string()).alias(variable_name),
                lit(None).alias(value_name),
            )
            return self._with_native(empty.filter(lit(False)))

        # `union` needs matching schemas: promote mixed numeric value columns first
        schema = self._native_schema
        value_types = {schema.field(name).type for name in on_}
        target: pa.DataType | None = None
        if len(value_types) > 1:
            if all(pa.types.is_integer(tp) for tp in value_types):
                target = pa.int64()
            elif all(pa.types.is_integer(tp) or pa.types.is_floating(tp) for tp in value_types):
                target = pa.float64()

        def value_expr(name: str) -> Expr:
            expr = col(name)
            return expr.cast(target) if target is not None else expr

        # no native unpivot: one projection per value column, unioned
        frames = [
            self.native.select(
                *(col(c) for c in index_),
                lit(name).alias(variable_name),
                value_expr(name).alias(value_name),
            )
            for name in on_
        ]
        native = reduce(lambda left, right: left.union(right), frames)
        return self._with_native(native)

    def with_row_index(self, name: str, order_by: Sequence[str]) -> Self:
        if not order_by:
            msg = "Must pass `order_by` to `with_row_index` for the DataFusion backend"
            raise TypeError(msg)
        expr = (window_expression(F.row_number(), order_by=order_by) - lit(1)).alias(name)
        return self._with_native(self.native.select(expr, *(col(c) for c in self.columns)))

    def sink_parquet(self, file: str | Path | BytesIO) -> None:
        if not isinstance(file, (str, Path)):
            # `write_parquet` takes a path; `str(buffer)` would write a file named after the repr
            msg = (
                "`sink_parquet` to a file-like object is not supported for the "
                "DataFusion backend; pass a file path instead."
            )
            raise NotImplementedError(msg)
        self.native.write_parquet(str(file))

    join_asof = not_implemented()
