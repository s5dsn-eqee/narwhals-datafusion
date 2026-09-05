# narwhals-datafusion

[![CI](https://github.com/s5dsn-eqee/narwhals-datafusion/actions/workflows/ci.yml/badge.svg)](https://github.com/s5dsn-eqee/narwhals-datafusion/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/narwhals-datafusion.svg)](https://badge.fury.io/py/narwhals-datafusion)
[![PEP 740](https://img.shields.io/badge/PEP%20740-attested-3775A9?logo=pypi&logoColor=white)](https://pypi.org/project/narwhals-datafusion/#files)
[![Downloads](https://static.pepy.tech/badge/narwhals-datafusion/month)](https://pepy.tech/project/narwhals-datafusion)

[Apache DataFusion](https://datafusion.apache.org/python/) backend for
[Narwhals](https://github.com/narwhals-dev/narwhals), registered through the
`narwhals.plugins` entry point.

```sh
pip install narwhals-datafusion                    # core
pip install "narwhals-datafusion[extra-functions]" # + mode, skew, kurtosis
```

## Usage

```python
import narwhals as nw
import pyarrow as pa
from datafusion import SessionContext

ctx = SessionContext()
df = ctx.from_arrow(pa.table({"a": [1, 2, 3], "b": ["x", "y", "x"]}))

lf = nw.from_native(df)  # nw.LazyFrame on this backend
result = (
    lf.group_by("b")
    .agg(nw.col("a").sum())
    .sort("b")
    .collect(backend="pyarrow")
)
```

Everything is lazy until `.collect()`: expressions become `datafusion.Expr`,
frame verbs become `datafusion.DataFrame` methods, and DataFusion executes the
plan. Dtypes are pyarrow end-to-end.

## Compatibility

- Python 3.10 to 3.13.
- `datafusion>=54,<55`: capped at the tested major; a release moves the cap.
- `narwhals>=2.25`: floor only; tested on 2.25, the `narwhals/` submodule.
- `datafusion-extra-functions-ffi>=0.1` (extra): each shim minor pins its
  datafusion major.

## Architecture

A subclass of narwhals' SQL layer (`narwhals._sql`, `narwhals._compliant`),
the layer DuckDB, Ibis and Spark also use.

| Module | Class |
|---|---|
| `dataframe.py` | `DataFusionLazyFrame`: frame verbs over `datafusion.DataFrame` |
| `expr.py` | `DataFusionExpr`: the `SQLExpr` hooks, aggregates, windows, casts |
| `namespace.py` | `DataFusionNamespace`: `SQLNamespace` primitives, IO, horizontal functions |
| `group_by.py`, `selectors.py`, `expr_str/dt/list/struct.py` | supporting surface |
| `utils.py` | function-name remapping, window/sort builders, dtype bridge via `narwhals._arrow` |

## API coverage

narwhals 2.25 on datafusion 54. ⚠️ entries work with the caveat in parentheses.

| Namespace | ✅ Supported | ⚠️ Partial | ❌ Not supported |
|---|---|---|---|
| `Expr` | `abs` `alias` `all` `any` `any_value` `ceil` `clip` `cos` `count` `cum_count` `cum_max` `cum_min` `cum_sum` `diff` `exp` `fill_nan` `first` `floor` `is_between` `is_close` `is_duplicated` `is_finite` `is_first_distinct` `is_in` `is_last_distinct` `is_nan` `is_null` `is_unique` `last` `len` `log` `max` `mean` `median` `min` `null_count` `over` `pipe` `rank` `rolling_mean` `rolling_std` `rolling_sum` `rolling_var` `round` `shift` `sin` `sqrt` `std` `sum` `var` | `cast` (no `Enum`) · `fill_null` (no `strategy` + `limit`) · `kurtosis` (`[extra-functions]` extra) · `mode` (`[extra-functions]` extra, `keep="any"` only) · `n_unique` (not over windows) · `replace_strict` (explicit `default` required) · `skew` (`[extra-functions]` extra) | `cum_prod` `quantile` |
| `Expr.str` | `contains` `ends_with` `head` `len_chars` `pad_end` `pad_start` `replace_all` `slice` `split` `starts_with` `strip_chars` `strip_chars_end` `strip_chars_start` `tail` `to_lowercase` `to_time` `to_uppercase` `zfill` | `to_date`/`to_datetime` (explicit `format` required) · `to_titlecase` (no word breaks on digits) | `replace` (use `replace_all`) |
| `Expr.dt` | `convert_time_zone` `date` `day` `hour` `microsecond` `millisecond` `minute` `month` `nanosecond` `ordinal_day` `second` `to_string` `truncate` `weekday` `year` | `replace_time_zone` (`None`/`"UTC"` only) | `offset_by` `timestamp` `total_microseconds` `total_milliseconds` `total_minutes` `total_nanoseconds` `total_seconds` |
| `Expr.list` | `contains` `get` `len` `max` `min` `sort` | `unique` (`maintain_order=False` only) | `mean` `median` `sum` |
| `Expr.struct` | `field` | | |
| `LazyFrame` | `collect` `collect_schema` `drop` `drop_nulls` `filter` `group_by` `head` `join` `rename` `select` `sort` `top_k` `unique` `unpivot` `with_columns` `with_row_index` | `explode` (single column) · `sink_parquet` (file path only) | `join_asof` |

- `mode`, `skew` and `kurtosis` need the `extra-functions` extra. It installs
  [datafusion-extra-functions-ffi](https://github.com/s5dsn-eqee/datafusion-extra-functions-ffi),
  a prebuilt wheel of the `datafusion-extra-functions` crate. Without it the
  three raise `NotImplementedError`.
- Not in the table: `Expr.filter`, `Expr.drop_nulls`, `Expr.unique`,
  `Expr.map_batches`, `Expr.ewm_mean`, `LazyFrame.tail`,
  `LazyFrame.gather_every`. Narwhals does not support them on any lazy backend.

## Known limitations

- Row order is guaranteed only after `sort`; `concat` may interleave inputs.
- `nw.scan_csv`/`nw.scan_parquet` cannot dispatch to a plugin yet (narwhals
  gap); read with a `SessionContext` and pass the frame to `nw.from_native`.
