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
    quote,
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
    """A narwhals lazy frame over ``datafusion.DataFrame``; every verb returns a new plan."""

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

    # schema and columns are cached: the native frame is immutable
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
        """Execute the plan into an eager frame: pyarrow by default, pandas or polars."""
        match backend:
            case None | Implementation.PYARROW:
                from narwhals._arrow.dataframe import ArrowDataFrame

                return ArrowDataFrame(
                    self.native.to_arrow_table(),
                    validate_backend_version=True,
                    version=self._version,
                    validate_column_names=True,
                )
            case Implementation.PANDAS:
                from narwhals._pandas_like.dataframe import PandasLikeDataFrame

                return PandasLikeDataFrame(
                    self.native.to_pandas(),
                    implementation=Implementation.PANDAS,
                    validate_backend_version=True,
                    version=self._version,
                    validate_column_names=True,
                )
            case Implementation.POLARS:
                from narwhals._polars.dataframe import PolarsDataFrame

                return PolarsDataFrame(
                    self.native.to_polars(),
                    validate_backend_version=True,
                    version=self._version,
                )
            case _:  # pragma: no cover
                msg = f"Unsupported `backend` value: {backend}"
                raise ValueError(msg)

    def head(self, n: int) -> Self:
        return self._with_native(self.native.limit(n))

    def simple_select(self, *column_names: str) -> Self:
        return self._with_native(self.native.select(*(col(c) for c in column_names)))

    def aggregate(self, *exprs: DataFusionExpr) -> Self:
        """Reduce the whole frame to one row of aggregates."""
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
        """Add or replace columns in place; new ones go last."""
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
        """The ``k`` rows with the largest ``by`` values; ``reverse`` takes the smallest."""
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
        """One row per distinct ``subset``, chosen by ``keep`` and ``order_by``.

        ``keep="none"`` drops every group of two or more rows instead.
        """
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
        """Join on ``left_on``/``right_on``; right columns that collide get ``suffix``."""
        left_columns, right_columns = self.columns, other.columns

        # shared column names make the joined schema ambiguous: rename every
        # right-hand column to a temporary name, re-select with narwhals' suffix rules
        all_columns = [*left_columns, *right_columns]
        tmp_names = {
            name: generate_temporary_column_name(8, all_columns, prefix=f"join_{i}_")
            for i, name in enumerate(right_columns)
        }
        rhs = other.native.select(*(col(name).alias(tmp) for name, tmp in tmp_names.items()))
        keys = zip(left_on or (), right_on or (), strict=True)
        on = [col(left) == col(tmp_names[right]) for left, right in keys] or [lit(True)]
        joined = self.native.join_on(rhs, *on, how="inner" if how == "cross" else how)
        if how in ("semi", "anti"):  # the engine keeps left columns only
            return self._with_native(joined)

        # right keys are dropped, the left copy stands for both; `full` keeps them
        dropped = set() if how == "full" else set(right_on or ())
        selection = [col(name) for name in left_columns] + [
            col(tmp).alias(f"{name}{suffix}" if name in left_columns else name)
            for name, tmp in tmp_names.items()
            if name not in dropped
        ]
        try:
            return self._with_native(joined.select(*selection))
        except Exception as e:
            raise catch_datafusion_exception(e, self) from None

    def explode(self, columns: Sequence[str]) -> Self:
        """One row per list element; an empty or null list gives one row with null."""
        if error := self._check_columns_exist(columns):
            raise error
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
        # `unnest_columns` drops empty lists; as nulls, `preserve_nulls` keeps them
        emptied = self.native.with_column(name, F.nullif(col(name), F.make_array()))
        return self._with_native(emptied.unnest_columns(quote(name), preserve_nulls=True))

    def unpivot(
        self,
        on: Sequence[str] | None,
        index: Sequence[str] | None,
        variable_name: str,
        value_name: str,
    ) -> Self:
        """Melt the ``on`` columns into ``variable_name``/``value_name`` rows, keeping ``index``."""
        index_ = list(index or ())
        on_ = [c for c in self.columns if c not in index_] if on is None else list(on)
        index_cols = [col(c) for c in index_]
        if not on_:
            # nothing to melt: the index columns plus an empty variable/value pair
            empty = self.native.select(
                *index_cols,
                lit(None).cast(pa.string()).alias(variable_name),
                lit(None).alias(value_name),
            )
            return self._with_native(empty.limit(0))

        # `union` coerces values but reports the first input's type: cast the
        # value columns to their polars supertype first
        types = {self._native_schema.field(name).type for name in on_}
        if len(types) == 1:
            target = None
        elif all(pa.types.is_integer(t) or pa.types.is_boolean(t) for t in types):
            target = pa.int64()
        elif all(pa.types.is_integer(t) or pa.types.is_floating(t) for t in types):
            target = pa.float64()
        else:
            target = pa.string()

        # no native unpivot: one projection per value column, unioned
        frames = [
            self.native.select(
                *index_cols,
                lit(name).alias(variable_name),
                (col(name) if target is None else col(name).cast(target)).alias(value_name),
            )
            for name in on_
        ]
        return self._with_native(reduce(lambda left, right: left.union(right), frames))

    def with_row_index(self, name: str, order_by: Sequence[str]) -> Self:
        """Prepend a zero-based row number in ``order_by`` order."""
        if not order_by:
            msg = "Must pass `order_by` to `with_row_index` for the DataFusion backend"
            raise TypeError(msg)
        # `row_number` is UInt64; minus an Int64 literal coerces to Decimal
        row_number = window_expression(F.row_number(), order_by=order_by).cast(pa.int64())
        expr = (row_number - lit(1)).alias(name)
        return self._with_native(self.native.select(expr, *(col(c) for c in self.columns)))

    def sink_parquet(self, file: str | Path | BytesIO) -> None:
        """Execute the plan and write the result to a Parquet file path."""
        if not isinstance(file, (str, Path)):
            # `write_parquet` takes a path; `str(buffer)` would write a file named after the repr
            msg = (
                "`sink_parquet` to a file-like object is not supported for the "
                "DataFusion backend; pass a file path instead."
            )
            raise NotImplementedError(msg)
        self.native.write_parquet(str(file))

    join_asof = not_implemented()
