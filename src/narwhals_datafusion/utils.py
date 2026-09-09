from __future__ import annotations

import operator
import re
from functools import lru_cache
from typing import TYPE_CHECKING, Any

import datafusion
import pyarrow as pa
from datafusion import functions as F
from datafusion import lit
from datafusion.expr import Expr, Window, WindowFrame
from narwhals._arrow.utils import (
    narwhals_to_native_dtype as _arrow_narwhals_to_native_dtype,
)
from narwhals._arrow.utils import (
    native_to_narwhals_dtype as _arrow_native_to_narwhals_dtype,
)
from narwhals._expression_parsing import evaluate_output_names_and_aliases
from narwhals._utils import extend_bool
from narwhals.exceptions import ColumnNotFoundError, DuplicateError

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from datafusion import SessionContext
    from datafusion.expr import SortExpr
    from narwhals._compliant.typing import CompliantLazyFrameAny
    from narwhals._utils import Version
    from narwhals.dtypes import DType
    from narwhals.typing import IntoDType

    from narwhals_datafusion.dataframe import DataFusionLazyFrame
    from narwhals_datafusion.expr import DataFusionExpr

__all__ = [
    "BACKEND_VERSION",
    "catch_datafusion_exception",
    "col",
    "evaluate_exprs_and_aliases",
    "function",
    "lit",
    "narwhals_to_native_dtype",
    "native_to_narwhals_dtype",
    "quote",
    "session_context",
    "sort_expr",
    "when",
    "window_expression",
]


def _parse_version(version: str) -> tuple[int, ...]:
    """Leading numeric components of ``version``, at most three."""
    parts = []
    for part in version.split(".")[:3]:
        digits = re.match(r"\d+", part)
        if digits is None:
            break
        parts.append(int(digits.group()))
    return tuple(parts)


BACKEND_VERSION: tuple[int, ...] = _parse_version(datafusion.__version__)


def quote(name: str) -> str:
    """Quote ``name`` for any DataFusion API that takes a column name."""
    # column names are parsed as SQL identifiers: lower-cased unless quoted
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def col(name: str) -> Expr:
    """Reference the column called exactly ``name``."""
    return datafusion.col(quote(name))


@lru_cache(maxsize=1)
def session_context() -> SessionContext:
    """Default ``SessionContext`` for ``scan_csv`` and ``scan_parquet``.

    User frames keep their own context; DataFusion combines frames from
    different contexts, so one shared default is safe.
    """
    from datafusion import SessionContext

    return SessionContext()


def native_to_narwhals_dtype(dtype: pa.DataType, version: Version) -> DType:
    """Map an Arrow type to a narwhals dtype.

    DataFusion schemas are Arrow schemas, so both directions reuse the
    Arrow backend's mapping.
    """
    return _arrow_native_to_narwhals_dtype(dtype, version)


def narwhals_to_native_dtype(dtype: IntoDType, version: Version) -> pa.DataType:
    """Map a narwhals dtype to an Arrow type."""
    return _arrow_narwhals_to_native_dtype(dtype, version)


def _ensure_expr(value: Any) -> Expr:
    """Wrap a Python value in ``lit``; pass an ``Expr`` through."""
    return value if isinstance(value, Expr) else lit(value)


def when(condition: Expr, value: Expr, otherwise: Expr | None = None) -> Expr:
    """``CASE WHEN condition THEN value``, with ``ELSE otherwise`` if given."""
    builder = F.when(condition, value)
    return builder.end() if otherwise is None else builder.otherwise(otherwise)


def _count(*args: Any) -> Expr:
    return F.count_star() if not args else F.count(_ensure_expr(args[0]))


def _divide(numerator: Any, denominator: Any) -> Expr:
    # integer `/` truncates; narwhals `divide` is true division
    return _ensure_expr(numerator).cast(pa.float64()) / _ensure_expr(denominator)


def _floordiv(left: Any, right: Any) -> Expr:
    left, right = _ensure_expr(left), _ensure_expr(right)
    # floor for any sign, staying in the input type: subtract the positive remainder first
    return (left - ((left % right) + right) % right) / right


def _substr(*args: Expr) -> Expr:
    return F.substring(*args) if len(args) == 3 else F.substr(*args)


def _lag(expr: Any, n: Any = 1) -> Expr:
    return F.lag(_ensure_expr(expr), n)


def _date_part(part: str) -> Callable[[Any], Expr]:
    def fn(expr: Any) -> Expr:
        return F.date_part(part, _ensure_expr(expr))

    return fn


# narwhals function names that `datafusion.functions` spells or behaves
# differently; the callables receive raw Python values
FUNCTION_REMAP: dict[str, Callable[..., Expr]] = {
    "add": lambda a, b: _ensure_expr(a) + _ensure_expr(b),
    "and": lambda a, b: _ensure_expr(a) & _ensure_expr(b),
    "count": _count,
    "count_distinct": lambda e: F.count(_ensure_expr(e), distinct=True),
    "day": _date_part("day"),
    "dayofyear": _date_part("doy"),
    "divide": _divide,
    "floordiv": _floordiv,
    "hour": _date_part("hour"),
    "isnotnull": lambda e: _ensure_expr(e).is_not_null(),
    "isnull": lambda e: _ensure_expr(e).is_null(),
    "lag": _lag,
    "length": lambda e: F.character_length(_ensure_expr(e)),
    "log": lambda e: F.ln(_ensure_expr(e)),
    "mean": lambda e: F.avg(_ensure_expr(e)),
    "minute": _date_part("minute"),
    "month": _date_part("month"),
    "multiply": operator.mul,
    "regexp_matches": lambda e, pattern: F.regexp_like(_ensure_expr(e), _ensure_expr(pattern)),
    "second": _date_part("second"),
    "str_split": lambda e, by: F.string_to_array(_ensure_expr(e), by),
    "substr": _substr,
    "subtract": operator.sub,
    "to_date": lambda e: F.to_date(_ensure_expr(e)),
    "year": _date_part("year"),
}


def function(name: str, *args: Any) -> Expr:
    """Call the DataFusion function behind narwhals' SQL function ``name``."""
    if remapped := FUNCTION_REMAP.get(name):
        return remapped(*args)
    native = getattr(F, name)
    return native(*(_ensure_expr(arg) for arg in args))


def sort_expr(
    into_expr: str | Expr, *, descending: bool = False, nulls_last: bool = False
) -> SortExpr:
    """Sort key from a column name or expression."""
    expr = col(into_expr) if isinstance(into_expr, str) else into_expr
    return expr.sort(ascending=not descending, nulls_first=not nulls_last)


def window_expression(
    expr: Expr,
    partition_by: Sequence[str | Expr] = (),
    order_by: Sequence[str | Expr] = (),
    rows_start: int | None = None,
    rows_end: int | None = None,
    *,
    descending: Sequence[bool] | None = None,
    nulls_last: Sequence[bool] | None = None,
    ignore_nulls: bool = False,
    frame_full: bool = False,
) -> Expr:
    """Evaluate ``expr`` over a window.

    ``rows_start``/``rows_end`` are narwhals row offsets; ``frame_full`` spans
    the whole partition instead. ``ignore_nulls`` is set on the window, where
    DataFusion honours it.
    """
    partition = [col(part) if isinstance(part, str) else part for part in partition_by] or None
    flags = extend_bool(False, len(order_by))
    descending = descending or flags
    nulls_last = nulls_last or flags
    order: list[SortExpr | Expr | str] | None = [
        sort_expr(by, descending=desc, nulls_last=nl)
        for by, desc, nl in zip(order_by, descending, nulls_last, strict=True)
    ] or None

    # narwhals bounds are offsets (negative = preceding); DataFusion takes magnitudes
    match (frame_full, rows_start, rows_end):
        case (True, _, _):
            frame = WindowFrame("rows", None, None)
        case (_, int(start), int(end)):
            frame = WindowFrame("rows", -start, end)
        case (_, None, int(end)):
            frame = WindowFrame("rows", None, end)
        case (_, int(start), None):
            frame = WindowFrame("rows", -start, None)
        case _:
            frame = None

    null_treatment = None
    if ignore_nulls:
        from datafusion.common import NullTreatment

        null_treatment = NullTreatment.IGNORE_NULLS
    return expr.over(
        Window(
            partition_by=partition,
            order_by=order,
            window_frame=frame,
            null_treatment=null_treatment,
        )
    )


def evaluate_exprs_and_aliases(
    df: DataFusionLazyFrame, /, *exprs: DataFusionExpr
) -> list[tuple[str, Expr]]:
    """Evaluate ``exprs`` against ``df`` as ``(output name, native expr)`` pairs.

    One narwhals expression can expand to several columns, e.g. ``nw.col("a", "b")``.
    """
    pairs: list[tuple[str, Expr]] = []
    for expr in exprs:
        _, aliases = evaluate_output_names_and_aliases(expr, df, [])
        pairs.extend(zip(aliases, expr(df), strict=True))
    return pairs


def catch_datafusion_exception(
    exception: Exception, frame: CompliantLazyFrameAny, /
) -> ColumnNotFoundError | DuplicateError | Exception:
    """Translate a DataFusion error into its narwhals exception, else return it as is."""
    message = str(exception)
    match re.search(r'No field named (?:"((?:[^"]|"")*)"|(\S+?))\.', message):
        case re.Match() as found:
            quoted, bare = found.groups()
            missing = bare if quoted is None else quoted.replace('""', '"')
            return ColumnNotFoundError.from_missing_and_available_column_names(
                [missing], frame.columns
            )
        case None if "Schema error: No field named" in message:  # pragma: no cover
            return ColumnNotFoundError.from_available_column_names(available_columns=frame.columns)
        case None if "Projections require unique expression names" in message:
            return DuplicateError(
                f"Expected unique column names, got duplicates in projection.\n\n{message}"
            )
        case _:
            return exception
