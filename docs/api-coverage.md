# API coverage

narwhals 2.28 on datafusion 54. ⚠️ entries work with the caveat in parentheses.

| Namespace | ✅ Supported | ⚠️ Partial | ❌ Not supported |
|---|---|---|---|
| `Expr` | `abs` `alias` `all` `any` `any_value` `ceil` `clip` `cos` `count` `cum_count` `cum_max` `cum_min` `cum_sum` `diff` `exp` `fill_nan` `first` `floor` `is_between` `is_close` `is_duplicated` `is_finite` `is_first_distinct` `is_in` `is_last_distinct` `is_nan` `is_null` `is_unique` `last` `len` `log` `max` `mean` `median` `min` `null_count` `over` `pipe` `rank` `rolling_mean` `rolling_std` `rolling_sum` `rolling_var` `round` `shift` `sin` `sqrt` `std` `sum` `var` | `cast` (no `Enum`) · `fill_null` (no `strategy` + `limit`) · `kurtosis` (`[extra-functions]` extra) · `mode` (`[extra-functions]` extra, `keep="any"` only, nulls not counted) · `n_unique` (not over windows) · `replace_strict` (explicit `default` required) · `skew` (`[extra-functions]` extra) | `cum_prod` `quantile` |
| `Expr.str` | `contains` `ends_with` `head` `len_chars` `pad_end` `pad_start` `replace_all` `slice` `split` `starts_with` `strip_chars` `strip_chars_end` `strip_chars_start` `tail` `to_date` `to_lowercase` `to_time` `to_uppercase` `zfill` | `to_datetime` (explicit `format` required) · `to_titlecase` (no word breaks on digits) | `replace` (use `replace_all`) |
| `Expr.dt` | `convert_time_zone` `date` `day` `hour` `microsecond` `millisecond` `minute` `month` `nanosecond` `ordinal_day` `second` `to_string` `truncate` `weekday` `year` | `replace_time_zone` (`None`/`"UTC"` only) | `offset_by` `timestamp` `total_microseconds` `total_milliseconds` `total_minutes` `total_nanoseconds` `total_seconds` |
| `Expr.list` | `contains` `get` `len` `max` `min` `sort` | `unique` (`maintain_order=False` only) | `mean` `median` `sum` |
| `Expr.struct` | `field` | | |
| `LazyFrame` | `collect` `collect_schema` `drop` `drop_nulls` `filter` `group_by` `head` `join` `rename` `select` `sort` `top_k` `unique` `unpivot` `with_columns` `with_row_index` | `explode` (single column) · `sink_parquet` (file path only) | `join_asof` |

Engine-wide limits: [known limitations](known-limitations.md).
