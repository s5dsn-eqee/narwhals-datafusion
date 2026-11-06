# API coverage

narwhals 2.28 on datafusion 55. ⚠️ entries work with the caveat in parentheses.

| Namespace | ✅ Supported | ⚠️ Partial | ❌ Not supported |
|---|---|---|---|
| `Expr` | `abs` `alias` `all` `any` `any_value` `ceil` `clip` `cos` `count` `cum_count` `cum_max` `cum_min` `cum_sum` `diff` `exp` `fill_nan` `first` `floor` `is_between` `is_close` `is_duplicated` `is_finite` `is_first_distinct` `is_in` `is_last_distinct` `is_nan` `is_null` `is_unique` `last` `len` `log` `max` `mean` `median` `min` `n_unique` `null_count` `over` `pipe` `rank` `rolling_mean` `rolling_std` `rolling_sum` `rolling_var` `round` `shift` `sin` `sqrt` `std` `sum` `var` | `cast` (no `Enum`) · `fill_null` (no `strategy` + `limit`) · `kurtosis` (`[extra-functions]` extra) · `mode` (`[extra-functions]` extra, `keep="any"` only, nulls not counted, numeric, temporal, `Utf8`/`Utf8View` input only) · `quantile` (`interpolation="linear"` only) · `replace_strict` (explicit `default` required) · `skew` (`[extra-functions]` extra) | `cum_prod` |
| `Expr.str` | `contains` `ends_with` `head` `len_chars` `pad_end` `pad_start` `replace_all` `slice` `split` `starts_with` `strip_chars` `strip_chars_end` `strip_chars_start` `tail` `to_date` `to_lowercase` `to_time` `to_uppercase` `zfill` | `to_datetime` (explicit `format` required) · `to_titlecase` (no word breaks on digits) | `replace` (use `replace_all`) |
| `Expr.dt` | `convert_time_zone` `date` `day` `hour` `microsecond` `millisecond` `minute` `month` `nanosecond` `ordinal_day` `replace_time_zone` `second` `to_string` `weekday` `year` | `truncate` (a multiple raises where an aware bin starts in a DST gap or overlap, and past 2262) | `offset_by` `timestamp` `total_microseconds` `total_milliseconds` `total_minutes` `total_nanoseconds` `total_seconds` |
| `Expr.list` | `contains` `get` `len` `max` `mean` `min` `sort` | `sum` (float elements only) · `unique` (`maintain_order=False` only) | `median` |
| `Expr.struct` | `field` | | |
| `LazyFrame` | `collect` `collect_schema` `drop` `drop_nulls` `filter` `group_by` `head` `join` `rename` `select` `sort` `top_k` `unique` `unpivot` `with_columns` `with_row_index` | `explode` (single column) · `sink_parquet` (file path only) | `join_asof` |

Engine-wide limits: [known limitations](known-limitations.md).
