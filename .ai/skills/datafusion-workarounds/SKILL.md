---
name: datafusion-workarounds
description: "TRIGGER — read before adding or changing any expression, aggregate, window, join, cast, or dtype code in src/narwhals_datafusion/. DataFusion diverges from the SQL semantics narwhals' _sql layer assumes in a fixed set of ways; each has a settled workaround here, proven by a test. Reuse it, and never let an engine quirk produce a silently wrong result."
argument-hint: "[area] (e.g., \"windows\", \"joins\", \"column names\", \"aggregates\", \"dtypes\", or omit for the whole list)"
---

# DataFusion workarounds

Verified on datafusion 55, the major `pyproject.toml` caps at. One row per
engine quirk: what the engine does and what the plugin does about it, the test
that proves both, the code.

## Rule: no test, no workaround

Every row is proven by a test in [`tests/workarounds/`][tests], one file per
section. The test does two things:

1. Pins the quirk on raw DataFusion, asserting with `ENGINE_CHANGED`.
2. Shows the plugin's result through narwhals, or the error it declares.

- New workaround: row, code and test in the same commit.
- A quirk that no longer reproduces has no row: remove the row and its code.
- An `ENGINE_CHANGED` failure after a datafusion bump means the row is
  obsolete: drop the workaround once the floor passes that release.

Every workaround also carries a one-line comment naming the engine behaviour:

```bash
grep -rn "^\s*# " src/narwhals_datafusion/*.py
```

## Column names

| Name | Description | Test | Link |
|---|---|---|---|
| Identifier parsing | `datafusion.col()` parses SQL: unquoted names are lower-cased (with `a` and `A`, `col("A")` silently gives `a`) and a dot splits off a qualifier. Use `utils.col()`, which quotes. | [`test_col_matches_names_exactly`][t-names] | [`utils.col`][utils] |
| Name-string APIs | `unnest_columns` and `join(on=...)` parse names the same way. Pass `utils.quote(name)`; joins use `join_on` with `col()` keys. | [`test_name_string_apis_get_quoted_names`][t-names] | [`dataframe.explode`][dataframe], [`dataframe.join`][dataframe] |
| Missing-column errors | Engine errors quote the name only when it needs quoting. `catch_datafusion_exception` strips it for `ColumnNotFoundError`; wrap native calls that can fail on a missing column in `with translate_errors(self):` (`select`, `with_columns`, `filter`, `sort`, `aggregate`, `group_by().agg()`). `join` checks its keys with `_check_columns_exist`. | [`test_missing_column_error_names_column_as_given`][t-names] | [`utils.catch_datafusion_exception`][utils] |

## Literals and operators

| Name | Description | Test | Link |
|---|---|---|---|
| Missing operators | `Expr` has no unary minus, `**` or `//`, and integer `/` truncates. `__neg__` is `0 - expr` with a `0` of the input type (`-1 * x` widens ints and rejects durations; unsigned raises, `0 - x` wraps), `__pow__` is `F.pow`, `__truediv__` casts to `float64` first. `_floordiv` takes `(left - remainder) / right` and steps it down by `right / right` where the remainder's sign differs: no overflow, the operands' type (a literal `1` widens). | [`test_arithmetic_follows_python`][t-lit], [`test_neg_keeps_the_input_type`][t-lit] | [`expr.py`][expr], [`utils._floordiv`][utils] |
| Float `//` | Float `%` follows Python, so stepping by the remainder gives `1.0 // 0.1` 9.0 (polars 10.0) and NaN for `x // inf`. `__floordiv__` floors the true quotient when it is a float (checked while planning) and uses `_floordiv` otherwise. | [`test_float_floordiv_floors_the_quotient`][t-lit] | [`expr._floordiv`][expr] |
| `%` sign and zero | `%` takes the dividend's sign, narwhals wants the divisor's; `%` by zero raises. `floor_mod` adds the divisor only when signs differ, so narrow ints never overflow (`((a % b) + b) % b` does for int8 `99 % 100`); `__mod__` returns null for a zero divisor. | [`test_mod_takes_divisor_sign_and_nulls_zero`][t-lit] | [`utils.floor_mod`][utils] |
| `list.contains` | `array_has(expr, NULL)` is null for every row and rejects a narrower Decimal literal. `list.contains` uses `array_has_any(expr, make_array(item))`, which coerces both, so it also coerces a mismatched item (`"2"` finds 2) where polars raises. `utils.check_list_item` compares the item with a null of the element type in pyarrow first and raises `InvalidOperationError`; `string_view` elements compare as `string` (no pyarrow kernel). | [`test_list_contains_matches_null_and_coerces_decimal`][t-lit], [`test_list_contains_rejects_a_mismatched_item`][t-lit] | [`expr_list.contains`][expr_list] |
| Scalar arguments | Some functions reject raw Python values (`F.coalesce`, `F.when`, `F.nullif`); `F.lag` takes only a raw int. Wrap scalars in `lit()`, except `lag`'s offset; `FUNCTION_REMAP` lambdas get raw values, use `utils._ensure_expr`. | [`test_scalar_arguments_need_lit`][t-lit] | [`utils.py`][utils] |

## Aggregates

| Name | Description | Test | Link |
|---|---|---|---|
| `n_unique` nulls | `count_distinct` skips nulls, narwhals counts one; `max` and `sum` over no rows are null. `n_unique` adds `max(isnull)`, coalesced to 0; `null_count` is narwhals' `sum`, which coalesces. | [`test_n_unique_counts_null`][t-agg] | [`expr.n_unique`][expr] |
| Literal-only `aggregate` | `simplify_expressions` drops an aggregate of literals only (`nw.len()` too) and collect fails. `aggregate` adds a `count_star` and projects it away. | [`test_literal_only_aggregate_keeps_its_node`][t-agg] | [`dataframe.aggregate`][dataframe] |
| Extra missing | `mode`, `skew`, `kurtosis` come from [datafusion-extra-functions][crate]; without it they raise `NotImplementedError` naming the extra. | [`test_without_extra_points_at_it`][t-extra] | [`extra_udaf`][extra_functions] |
| `skew` bias | The crate's `skewness` is bias-adjusted. `_correct` undoes it and pins n=0/1/2. | [`test_skew_undoes_bias_adjustment`][t-agg] | [`expr.skew`][expr] |
| `mode` input types | The crate has accumulators for int, float, date, time, timestamp, `Utf8`, `Utf8View`; anything else (`LargeUtf8` from polars, `Boolean`, `Decimal`) fails at execution. `_mode_supports` raises `NotImplementedError` while planning. | [`test_mode_rejects_unsupported_types_while_planning`][t-agg] | [`expr.mode`][expr] |
| `mode` nulls | The crate skips nulls and rejects `List`, so narwhals' one-element-list trick fails. Coverage caveat and skip list until the crate counts nulls. | [`test_mode_does_not_count_nulls`][t-agg] | [coverage][coverage] |
| `median` input | [`median`][df-median] has numeric signatures only. `median` raises narwhals' `InvalidOperationError` for the rest. | [`test_median_rejects_non_numeric`][t-agg] | [`expr.median`][expr] |
| `quantile` | [`percentile_cont`][df-percentile] is exact, linear, NaN last. `quantile` uses it and raises `NotImplementedError` for other interpolations, as duckdb does. | [`test_quantile_is_linear_percentile_cont`][t-agg] | [`expr.quantile`][expr] |
| `product` | No aggregate: `cum_prod` is `not_implemented()`. | [`test_no_product_aggregate`][t-agg] | [`expr.cum_prod`][expr] |
| `list.sum` | [`array_sum`][df-array-sum] returns Float64 (inexact past 2**53) and null for an empty list. `list.sum` takes float elements only and gives 0 for a non-null empty list. | [`test_list_sum_is_float_only`][t-agg] | [`expr_list.sum`][expr_list] |
| `list.mean`, `list.median` | [`array_avg`][df-array-avg] matches `list.mean`. No `array_median`: `list.median` is `not_implemented()`. | [`test_list_mean_and_median`][t-agg] | [`expr_list.py`][expr_list] |
| Input dtype | `Expr.types()` takes literals only. `DataFusionLazyFrame._native_dtype` plans `select(expr)` for the type; `DataFusionExpr._with_native_dtype` hands it to `mode`, `median`, `list.sum`. | [`test_mode_rejects_unsupported_types_while_planning`][t-agg] | [`expr.py`][expr], [`dataframe.py`][dataframe] |

## Windows

| Name | Description | Test | Link |
|---|---|---|---|
| Order inside a windowed aggregate | An `ORDER BY` declared inside an aggregate raises when it runs as a window (54 dropped it silently). `first`/`last`/`any_value` put order and `IGNORE NULLS` on the `Window`. | [`test_order_inside_windowed_aggregate_moves_to_window`][t-win] | [`expr.first`][expr], [`utils.window_expression`][utils] |
| `n_unique` over | `DISTINCT` inside a windowed `count` is honoured (54 dropped it). `n_unique` windows `count_distinct` and `max(isnull)` separately and adds them. | [`test_n_unique_over_relies_on_windowed_distinct`][t-win] | [`expr.n_unique`][expr] |
| Cumulative default frame | With `order_by` the frame is cumulative. A whole-partition count must not pass the order; `last` over a window spans the partition (`frame_full=True`). | [`test_ordered_window_frame_is_cumulative`][t-win] | [`dataframe.unique`][dataframe], [`expr.last`][expr] |
| Bounded frames | `first_value`/`last_value` over a bounded frame need `retract_batch`, not implemented: `fill_null(limit=n)` raises. Unlimited fills are `last_value` over a cumulative frame with `ignore_nulls=True`. | [`test_bounded_first_last_value_frames_raise`][t-win] | [`expr.fill_null`][expr] |
| Only aggregates in `over` | `over` takes aggregate and window functions only. Window each aggregate and combine outside (`cov`, `skew`); horizontal functions broadcast through their operands' windows via `_from_elementwise_horizontal_op`. | [`test_only_aggregates_go_inside_over`][t-win] | [`namespace.cov`][namespace], [`expr.skew`][expr] |
| Sort null placement | datafusion-python 55 changed `Expr.sort()` to `nulls_first=False`. `sort_expr` always passes both flags; never call `.sort()` without them. | [`test_sort_null_placement_is_explicit`][t-win] | [`utils.sort_expr`][utils] |

## Joins

| Name | Description | Test | Link |
|---|---|---|---|
| Shared column names | A column on both sides is ambiguous for `join_on`; raw `join(on=...)` silently keeps one copy. `join` renames every right column to a temporary name, joins with `join_on`, then re-selects with narwhals' suffix rules. Do not alias a single key. | [`test_join_suffixes_names_on_both_sides`][t-joins] | [`dataframe.join`][dataframe] |
| Cross join | No `how="cross"`: cross join is `join_on(rhs)` without a predicate. | [`test_cross_join_has_no_native_how`][t-joins] | [`dataframe.join`][dataframe] |
| `join_asof` | No engine support: `not_implemented()`. | [`test_join_asof_is_declared_unsupported`][t-joins] | [`dataframe.py`][dataframe] |

## Frames

| Name | Description | Test | Link |
|---|---|---|---|
| `explode` empty lists | `unnest_columns` drops empty lists. `explode` turns them into nulls first (`nullif`) and keeps null lists (`preserve_nulls`). | [`test_explode_keeps_empty_and_null_lists`][t-frames] | [`dataframe.explode`][dataframe] |
| Multi-column `explode` | Unnesting several lists pads the shorter ones with null, where polars raises. `explode` on several columns raises `NotImplementedError`. | [`test_explode_multiple_columns_raises`][t-frames] | [`dataframe.explode`][dataframe] |
| `unpivot` | No native unpivot, and a union of one projection per value column scans the source once per column. `unpivot` unnests one `make_array` of `named_struct(variable, value)`; the field names are fixed, since `named_struct` rejects an empty one, and the output names are aliases. | [`test_unpivot_scans_the_source_once`][t-frames] | [`dataframe.unpivot`][dataframe] |
| `unpivot` types | `make_array` casts int and string to int (`'x'` fails) and rejects int with bool, so value columns are cast to polars' supertype first. `_value_supertype` uses pyarrow's permissive promotion, folds bool in, maps signed + uint64 to Float64, else String. | [`test_unpivot_casts_values_to_supertype`][t-frames] | [`dataframe.unpivot`][dataframe] |
| `concat` by name | `union` is positional. `concat` uses `union_by_name`, which also fills missing columns with null, so both modes share one path. | [`test_concat_diagonal_matches_columns_by_name`][t-frames] | [`namespace.concat`][namespace] |
| `union` order | Output order is nondeterministic (one partition per input); not pinnable, the test checks rows only. Tests asserting order after `concat` without a sort go in `ALWAYS_DESELECTED`. | [`test_concat_returns_every_row_in_some_order`][t-frames] | [`run_tests.py`][run_tests] |
| `rename` duplicates | A taken name is a planning error (`would be ambiguous`, `Projections require unique expression names`). `rename` checks the new names first and raises `DuplicateError`, as duckdb does. | [`test_rename_into_a_taken_name_raises_duplicate`][t-frames] | [`dataframe.rename`][dataframe] |
| `top_k` | A `row_number() <= k` filter sorts every row; the optimizer does not rewrite it. `top_k` is `sort` + `limit`, planned as `SortExec: TopK(fetch=k)`. | [`test_top_k_plans_a_top_k_sort`][t-frames] | [`dataframe.top_k`][dataframe] |
| `unique` on every column | A `row_number` window over every column sorts by all of them. `unique(keep="any")` over every column is `distinct`, a hash aggregate. | [`test_unique_on_every_column_is_distinct`][t-frames] | [`dataframe.unique`][dataframe] |
| `sink_parquet` target | `write_parquet` takes a path and `str()`s anything else into a file name. `sink_parquet` raises for non-path inputs. | [`test_sink_parquet_rejects_file_objects`][t-frames] | [`dataframe.sink_parquet`][dataframe] |
| `with_row_index` | `row_number` is UInt64; minus an Int64 literal coerces to Decimal. Cast to Int64 first. | [`test_with_row_index_is_int64`][t-frames] | [`dataframe.with_row_index`][dataframe] |

## Dtypes and time

| Name | Description | Test | Link |
|---|---|---|---|
| Date parts | `date_part('millisecond')` and friends include whole seconds; `dow` is 0=Sunday, narwhals wants 1=Monday. `dt` subtracts `second * 1000` etc. and shifts `dow`. | [`test_date_parts_follow_polars`][t-dtypes] | [`expr_dt.py`][expr_dt] |
| `truncate` multiples | `date_trunc` takes single units only (`'15 minute'` fails). Multiples use [`date_bin`][df-date-bin] anchored at the epoch, polars semantics. | [`test_truncate_by_multiple_anchors_at_epoch`][t-dtypes] | [`expr_dt.truncate`][expr_dt] |
| `truncate` type | `date_bin` returns Timestamp(ns) for a non-ns input; it and `date_trunc` do for a `Date`. `_floor` casts back to the input type. | [`test_truncate_keeps_input_type`][t-dtypes] | [`expr_dt._floor`][expr_dt] |
| `date_bin` origin | With an ns origin `date_bin` keeps the input unit but returns null past 2262. The origin is `us`, so the input coerces to ns and raises there instead. | [`test_truncate_origin_is_not_ns`][t-dtypes] | [`expr_dt.truncate`][expr_dt] |
| `truncate` aware multiples | `date_bin` bins an aware timestamp in UTC. Multiples bin the wall time ([`to_local_time`][df-to-local-time]) and cast back to the zone; a bin start in a DST gap or overlap raises. | [`test_truncate_by_multiple_bins_wall_time`][t-dtypes] | [`expr_dt._floor`][expr_dt] |
| Time zones | A cast from naive reads the value as wall time in the target zone; narwhals reads naive as UTC. `convert_time_zone` casts to UTC first; `replace_time_zone` is `to_local_time`, then that cast. Both keep the input unit. | [`test_convert_time_zone_reads_naive_as_utc`][t-dtypes] | [`expr_dt.convert_time_zone`][expr_dt] |
| Datetime to string | A timestamp cast to `Utf8` is ISO (`T`, `Z`, the shortest fraction). Polars writes a space, the unit's digits and `+00:00`; `cast(String)` uses `to_char` with that format for timestamps. | [`test_datetime_to_string_follows_polars`][t-dtypes] | [`expr.cast`][expr] |
| `Enum` casts | narwhals' Arrow dtype mapping raises for `Enum` naming PyArrow. `narwhals_to_native_dtype` raises first, naming DataFusion (narwhals' own message under `stable.v1`). | [`test_enum_cast_names_the_backend`][t-dtypes] | [`utils.narwhals_to_native_dtype`][utils] |
| `is_nan` input | `isnan` rejects non-numeric input while planning. `catch_datafusion_exception` turns that error into `InvalidOperationError`. | [`test_is_nan_on_strings_raises_invalid_operation`][t-dtypes] | [`utils.catch_datafusion_exception`][utils] |
| `to_datetime` format | The engine cannot infer a format. `to_datetime` requires one. | [`test_to_datetime_without_format_raises`][t-dtypes] | [`expr_str.to_datetime`][expr_str] |
| `to_datetime` unit | `to_timestamp` always returns ns. Polars takes the unit from the format: `%.3f` ms, `%.9f` ns, else us; `to_datetime` casts to it. | [`test_to_datetime_unit_follows_the_format`][t-dtypes] | [`expr_str.to_datetime`][expr_str] |
| `to_datetime` offsets | A `%z`/`%Z` format parses to UTC but returns a naive timestamp. The zone is re-attached. | [`test_to_datetime_offset_format_is_utc_aware`][t-dtypes] | [`expr_str.to_datetime`][expr_str] |

## Where the truth is

- [`docs/api-coverage.md`][coverage]: same commit as any change here.
- [`tests/workarounds/`][tests]: the proof for every row.
- [`narwhals/src/narwhals/_duckdb/`][duckdb]: the reference backend for what
  narwhals expects.
- Schemas are Arrow schemas; dtype mapping reuses `narwhals._arrow.utils` both
  ways.

[tests]: ../../../tests/workarounds
[t-names]: ../../../tests/workarounds/test_column_names.py
[t-lit]: ../../../tests/workarounds/test_literals.py
[t-agg]: ../../../tests/workarounds/test_aggregates.py
[t-win]: ../../../tests/workarounds/test_windows.py
[t-joins]: ../../../tests/workarounds/test_joins.py
[t-frames]: ../../../tests/workarounds/test_frames.py
[t-dtypes]: ../../../tests/workarounds/test_dtypes.py
[t-extra]: ../../../tests/test_extra_functions.py
[expr]: ../../../src/narwhals_datafusion/expr.py
[expr_list]: ../../../src/narwhals_datafusion/expr_list.py
[expr_str]: ../../../src/narwhals_datafusion/expr_str.py
[expr_dt]: ../../../src/narwhals_datafusion/expr_dt.py
[dataframe]: ../../../src/narwhals_datafusion/dataframe.py
[namespace]: ../../../src/narwhals_datafusion/namespace.py
[utils]: ../../../src/narwhals_datafusion/utils.py
[extra_functions]: ../../../src/narwhals_datafusion/extra_functions.py
[run_tests]: ../../../run_tests.py
[coverage]: ../../../docs/api-coverage.md
[duckdb]: ../../../narwhals/src/narwhals/_duckdb
[crate]: https://github.com/datafusion-contrib/datafusion-extra-functions
[df-median]: https://datafusion.apache.org/user-guide/sql/aggregate_functions.html#median
[df-percentile]: https://datafusion.apache.org/user-guide/sql/aggregate_functions.html#percentile-cont
[df-array-sum]: https://datafusion.apache.org/user-guide/sql/scalar_functions.html#array-sum
[df-array-avg]: https://datafusion.apache.org/user-guide/sql/scalar_functions.html#array-avg
[df-date-bin]: https://datafusion.apache.org/user-guide/sql/scalar_functions.html#date-bin
[df-to-local-time]: https://datafusion.apache.org/user-guide/sql/scalar_functions.html#to-local-time
